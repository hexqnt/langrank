#![warn(clippy::pedantic)]

use crate::cli::Cli;
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Local};
use clap::Parser;
use colored::Colorize;
use csv::Writer;
use ndarray::Array2;
use reqwest::{Client, Response};
use rustc_hash::{FxHashMap, FxHashSet};
use scraper::{Html, Selector};
use serde::Serialize;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::io::Cursor;
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::fs;
use tokio::task;
use tokio::time::sleep;

mod cli;

const TIOBE_URL: &str = "https://www.tiobe.com/tiobe-index/";
const PYPL_URL: &str = "https://pypl.github.io/PYPL.html";
const BENCH_URL: &str = "https://salsa.debian.org/benchmarksgame-team/benchmarksgame/-/raw/master/public/data/alldata.csv";
const MAX_RETRIES: usize = 3;
const HTTP_TIMEOUT_SECONDS: u64 = 20;

#[derive(Debug, Serialize, Clone)]
struct RankingEntry {
    lang: String,
    rank: Option<u32>,
    share: f64,
    trend: Option<f64>,
}

#[derive(Default)]
struct AggregatedEntry {
    min_rank: Option<u32>,
    share_sum: f64,
    trend_sum: f64,
    trend_seen: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    colored::control::set_override(true);

    let mut cli = Cli::parse();

    if let Some(command) = cli.command.take() {
        crate::cli::handle_command(command)?;
        return Ok(());
    }

    let Cli {
        save_rankings,
        save_benchmarks,
        save_schulze,
        full_output,
        ..
    } = cli;

    let run_started_at = Local::now();

    let client = Client::builder()
        .user_agent("lang-rank-fetcher/0.1")
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECONDS))
        .build()
        .context("failed to build HTTP client")?;

    let (tiobe, mut pypl, bench_bytes) = tokio::try_join!(
        fetch_tiobe(&client),
        fetch_pypl(&client),
        download_benchmark_data(&client)
    )?;

    let pypl_original_len = pypl.len();
    adjust_pypl_entries(&tiobe, &mut pypl);

    if let Some(path) = save_rankings.as_ref() {
        save_rankings_csv(
            path.as_path(),
            &[(SourceKind::Tiobe, &tiobe), (SourceKind::Pypl, &pypl)],
        )
        .await?;
    }

    if let Some(path) = save_benchmarks.as_ref() {
        save_benchmarks_csv(&bench_bytes, path.as_path()).await?;
    }

    let benchmark_stats = load_benchmark_stats(&bench_bytes).await?;
    let benchmark_lang_count = benchmark_stats.len();
    let schulze_records = compute_schulze_records(&tiobe, &pypl, &benchmark_stats)?;
    if let Some(path) = save_schulze.as_ref() {
        save_schulze_csv(&schulze_records, path.as_path()).await?;
    }

    print_summary(&SummaryContext {
        tiobe_count: tiobe.len(),
        pypl_count: pypl_original_len,
        benchmark_lang_count,
        run_started_at: &run_started_at,
        paths: SummaryPaths {
            benchmarks: save_benchmarks.as_deref(),
            rankings: save_rankings.as_deref(),
            schulze: save_schulze.as_deref(),
        },
        schulze_records: &schulze_records,
        full_output,
    });

    Ok(())
}

struct SummaryPaths<'a> {
    benchmarks: Option<&'a Path>,
    rankings: Option<&'a Path>,
    schulze: Option<&'a Path>,
}

struct SummaryContext<'a> {
    tiobe_count: usize,
    pypl_count: usize,
    benchmark_lang_count: usize,
    run_started_at: &'a DateTime<Local>,
    paths: SummaryPaths<'a>,
    schulze_records: &'a [SchulzeRecord],
    full_output: bool,
}

fn print_summary(context: &SummaryContext<'_>) {
    println!();
    print_summary_header(context);
    print_summary_paths(&context.paths);
    println!();
    println!("{}", "Schulze Ranking".bold().bright_magenta());
    print_schulze_table(context.schulze_records, context.full_output);
    println!(
        "{}",
        "====================================================".bright_cyan()
    );
}

fn print_summary_header(context: &SummaryContext<'_>) {
    println!(
        "{}",
        "================= LangRank Update ================="
            .bold()
            .bright_cyan()
    );
    println!(
        "{} {}",
        "Run started".bright_yellow().bold(),
        context
            .run_started_at
            .format("%Y-%m-%d %H:%M:%S %Z")
            .to_string()
            .bright_white()
    );
    println!(
        "{} {} | {} | {}",
        "Sources".bright_yellow().bold(),
        format!("TIOBE: {}", context.tiobe_count).bright_white(),
        format!("PYPL: {}", context.pypl_count).bright_white(),
        format!("Benchmarks: {}", context.benchmark_lang_count).bright_white()
    );
}

fn print_summary_paths(paths: &SummaryPaths<'_>) {
    print_path_line(
        "Benchmarks CSV",
        paths.benchmarks,
        "not saved (use --save-benchmarks)",
    );
    print_path_line(
        "Combined CSV",
        paths.rankings,
        "not saved (use --save-rankings)",
    );
    print_path_line(
        "Schulze CSV",
        paths.schulze,
        "not saved (use --save-schulze)",
    );
}

fn print_path_line(label: &str, path: Option<&Path>, hint: &str) {
    let label_colored = label.bright_yellow().bold();
    match path {
        Some(path) => println!(
            "{} {}",
            label_colored,
            format!("{}", path.display()).bright_white()
        ),
        None => println!("{} {}", label_colored, hint.bright_black()),
    }
}

fn print_schulze_table(records: &[SchulzeRecord], full_output: bool) {
    if records.is_empty() {
        println!("{}", "No Schulze data available.".bright_black());
        return;
    }

    if full_output {
        print_full_schulze_table(records);
    } else {
        print_compact_schulze_table(records);
    }
}

fn print_full_schulze_table(records: &[SchulzeRecord]) {
    let header = format!(
        "{:>3} | {:<13} | {:>6} | {:>6} | {:>7} | {:>6} | {:>6} | {:>7} | {:>8} | {:>4}",
        "Pos", "Language", "T Rank", "T%", "T Trend", "P Rank", "P%", "P Trend", "Perf(s)", "Wins"
    );
    println!("{}", header.bold().bright_white());
    println!(
        "{}",
        "----+---------------+--------+--------+---------+--------+--------+---------+--------+------"
            .bright_black()
    );

    for record in records {
        let tiobe_rank = record
            .tiobe_rank
            .map_or_else(|| "-".to_string(), |value| value.to_string());
        let pypl_rank = record
            .pypl_rank
            .map_or_else(|| "-".to_string(), |value| value.to_string());
        let tiobe_share = format!("{:.2}", record.tiobe_share);
        let pypl_share = format!("{:.2}", record.pypl_share);
        let tiobe_trend = format_trend(record.tiobe_trend);
        let pypl_trend = format_trend(record.pypl_trend);
        let perf = format!("{:.2}", record.benchmark_elapsed);
        let line = format!(
            "{:>3} | {:<13} | {:>6} | {:>6} | {:>7} | {:>6} | {:>6} | {:>7} | {:>8} | {:>4}",
            record.position,
            record.lang,
            tiobe_rank,
            tiobe_share,
            tiobe_trend,
            pypl_rank,
            pypl_share,
            pypl_trend,
            perf,
            record.schulze_wins
        );
        println!("{}", line.bright_green());
    }
}

fn print_compact_schulze_table(records: &[SchulzeRecord]) {
    println!(
        "{}",
        "Pos | Language      | TIOBE% | PYPL% | Perf(s) | Wins"
            .to_string()
            .bold()
            .bright_white()
    );
    println!(
        "{}",
        "----+---------------+--------+-------+---------+------".bright_black()
    );
    for record in records.iter().take(10) {
        let line = format!(
            "{:>3} | {:<13} | {:>6.2} | {:>5.2} | {:>7.2} | {:>4}",
            record.position,
            record.lang,
            record.tiobe_share,
            record.pypl_share,
            record.benchmark_elapsed,
            record.schulze_wins
        );
        println!("{}", line.bright_green());
    }
    if records.len() > 10 {
        println!(
            "{}",
            format!(
                "... {} more entries (use --full-output to display all).",
                records.len() - 10
            )
            .bright_black()
        );
    }
}

async fn fetch_tiobe(client: &Client) -> Result<Vec<RankingEntry>> {
    let body = fetch_text_with_retry(client, TIOBE_URL)
        .await
        .context("failed to download TIOBE index")?;
    let document = Html::parse_document(&body);

    let table_selector =
        Selector::parse("table.table.table-striped.table-top20").expect("valid selector");
    let row_selector = Selector::parse("tr").expect("valid selector");
    let cell_selector = Selector::parse("td").expect("valid selector");

    let mut entries = Vec::new();

    if let Some(table) = document.select(&table_selector).next() {
        for row in table.select(&row_selector).skip(1) {
            let cells: Vec<String> = row.select(&cell_selector).map(extract_cell_text).collect();
            if cells.len() > 6 {
                let rank = parse_u32(&cells[0]);
                let lang = cells[4].clone();
                let share = parse_percent(&cells[5]).unwrap_or(0.0);
                let trend = parse_percent(&cells[6]);
                entries.push(RawEntry {
                    lang,
                    rank,
                    share,
                    trend,
                });
            }
        }
    }

    let other_table_selector = Selector::parse("table#otherPL").expect("valid selector");
    if let Some(table) = document.select(&other_table_selector).next() {
        for row in table.select(&row_selector).skip(1) {
            let cells: Vec<String> = row.select(&cell_selector).map(extract_cell_text).collect();
            if cells.len() > 2 {
                let rank = parse_u32(&cells[0]);
                let lang = cells[1].clone();
                let share = parse_percent(&cells[2]).unwrap_or(0.0);
                entries.push(RawEntry {
                    lang,
                    rank,
                    share,
                    trend: None,
                });
            }
        }
    }

    Ok(aggregate_entries(entries))
}

async fn fetch_pypl(client: &Client) -> Result<Vec<RankingEntry>> {
    let body = fetch_text_with_retry(client, PYPL_URL)
        .await
        .context("failed to download PYPL index")?;

    let start_marker = "<!-- begin section All-->";
    let end_marker = "<!-- end section All-->";
    let start_idx = body
        .find(start_marker)
        .map(|idx| idx + start_marker.len())
        .ok_or_else(|| anyhow!("PYPL start marker not found"))?;
    let end_idx = body
        .find(end_marker)
        .ok_or_else(|| anyhow!("PYPL end marker not found"))?;
    if start_idx >= end_idx {
        return Err(anyhow!("PYPL markers are in unexpected order"));
    }
    let raw_fragment = &body[start_idx..end_idx];
    let cell_selector = Selector::parse("td").expect("valid selector");

    let mut entries = Vec::new();

    for line in raw_fragment.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed == "\\" || trimmed.contains("\" + table + \"") {
            continue;
        }
        let mut cleaned = trimmed.trim_end_matches('\\').replace("\\\"", "\"");
        if !cleaned.starts_with("<tr") {
            cleaned = format!("<tr>{cleaned}");
        }
        if !cleaned.ends_with("</tr>") {
            cleaned.push_str("</tr>");
        }
        let row_html = format!("<table>{cleaned}</table>");
        let row = Html::parse_fragment(&row_html);
        let cells: Vec<String> = row.select(&cell_selector).map(extract_cell_text).collect();
        if cells.len() >= 5 {
            let rank = parse_u32(&cells[0]);
            let lang = cells[2].clone();
            let share = parse_percent(&cells[3]).unwrap_or(0.0);
            let trend = parse_percent(&cells[4]);
            entries.push(RawEntry {
                lang,
                rank,
                share,
                trend,
            });
        }
    }

    Ok(aggregate_entries(entries))
}

async fn download_benchmark_data(client: &Client) -> Result<Vec<u8>> {
    fetch_bytes_with_retry(client, BENCH_URL)
        .await
        .context("failed to download benchmark dataset")
}

async fn save_benchmarks_csv(bytes: &[u8], path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    fs::write(path, bytes)
        .await
        .with_context(|| format!("failed to write {}", path.display()))?;

    Ok(())
}

fn format_trend(trend: Option<f64>) -> String {
    match trend {
        Some(value) => {
            let normalized = if value.abs() < 0.005 { 0.0 } else { value };
            format!("{normalized:+.2}")
        }
        None => "-".to_string(),
    }
}

async fn save_rankings_csv(path: &Path, sources: &[(SourceKind, &[RankingEntry])]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let serialized = serialize_rankings(sources)?;
    fs::write(path, serialized)
        .await
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn serialize_rankings(sources: &[(SourceKind, &[RankingEntry])]) -> Result<Vec<u8>> {
    let mut writer = Writer::from_writer(Vec::new());
    for (source, entries) in sources {
        for entry in *entries {
            let record = CsvRecord {
                source: *source,
                lang: entry.lang.clone(),
                rank: entry.rank,
                share: entry.share,
                trend: entry.trend,
            };
            writer
                .serialize(record)
                .context("failed to serialize ranking record")?;
        }
    }
    writer
        .flush()
        .context("failed to flush ranking CSV writer")?;
    writer
        .into_inner()
        .context("failed to finalize ranking CSV output")
}

async fn fetch_text_with_retry(client: &Client, url: &str) -> Result<String> {
    send_with_retry(client, url)
        .await?
        .text()
        .await
        .with_context(|| format!("failed to read response body from {url}"))
}

async fn fetch_bytes_with_retry(client: &Client, url: &str) -> Result<Vec<u8>> {
    let bytes = send_with_retry(client, url)
        .await?
        .bytes()
        .await
        .with_context(|| format!("failed to read response body from {url}"))?;
    Ok(bytes.to_vec())
}

async fn send_with_retry(client: &Client, url: &str) -> Result<Response> {
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=MAX_RETRIES {
        match client.get(url).send().await {
            Ok(response) => match response.error_for_status() {
                Ok(success) => return Ok(success),
                Err(err) => last_err = Some(err.into()),
            },
            Err(err) => last_err = Some(err.into()),
        }

        if attempt < MAX_RETRIES {
            sleep(calculate_backoff(attempt)).await;
        }
    }

    let detail = last_err
        .as_ref()
        .map_or_else(|| "unknown error".to_string(), describe_error);
    Err(anyhow!(
        "failed to fetch {url} after {MAX_RETRIES} attempts: {detail}"
    ))
}

fn calculate_backoff(attempt: usize) -> Duration {
    const MAX_BACKOFF_EXPONENT: u32 = 10;
    let exponent = u32::try_from(attempt)
        .unwrap_or(MAX_BACKOFF_EXPONENT)
        .min(MAX_BACKOFF_EXPONENT);
    let seconds = 2_u64.saturating_pow(exponent);
    Duration::from_secs(seconds)
}

fn describe_error(error: &anyhow::Error) -> String {
    let mut pieces: Vec<String> = Vec::new();
    for (idx, cause) in error.chain().enumerate() {
        let text = cause.to_string();
        if text.is_empty() {
            continue;
        }
        if idx == 0 {
            pieces.push(text);
        } else {
            pieces.push(format!("caused by {text}"));
        }
    }

    if pieces.is_empty() {
        format!("{error:?}")
    } else {
        pieces.join(" | ")
    }
}

#[derive(Debug)]
struct RawEntry {
    lang: String,
    rank: Option<u32>,
    share: f64,
    trend: Option<f64>,
}

fn aggregate_entries(entries: Vec<RawEntry>) -> Vec<RankingEntry> {
    let alias_map = language_aliases();
    let mut aggregated: FxHashMap<String, AggregatedEntry> = FxHashMap::default();

    for entry in entries {
        let trimmed = entry.lang.trim();
        let normalized = alias_map.get(trimmed).copied().unwrap_or(trimmed);
        if normalized.is_empty() {
            continue;
        }
        let agg = aggregated.entry(normalized.to_owned()).or_default();
        agg.share_sum += entry.share;
        if let Some(rank) = entry.rank {
            agg.min_rank = Some(match agg.min_rank {
                None => rank,
                Some(existing) => existing.min(rank),
            });
        }
        if let Some(trend) = entry.trend {
            agg.trend_sum += trend;
            agg.trend_seen = true;
        }
    }

    let mut result: Vec<RankingEntry> = aggregated
        .into_iter()
        .map(|(lang, agg)| RankingEntry {
            lang,
            rank: agg.min_rank,
            share: agg.share_sum,
            trend: if agg.trend_seen {
                Some(agg.trend_sum)
            } else {
                None
            },
        })
        .collect();

    result.sort_by(|a, b| a.lang.cmp(&b.lang));
    result
}

fn adjust_pypl_entries(tiobe: &[RankingEntry], pypl: &mut Vec<RankingEntry>) {
    let Some(c_data) = tiobe.iter().find(|entry| entry.lang == "C") else {
        return;
    };
    let Some(cpp_data) = tiobe.iter().find(|entry| entry.lang == "C++") else {
        return;
    };
    let Some(position) = pypl.iter().position(|entry| entry.lang == "C/C++") else {
        return;
    };

    let combined = pypl.remove(position);
    let share_sum = c_data.share + cpp_data.share;
    if share_sum <= f64::EPSILON {
        pypl.push(combined);
        pypl.sort_by(|a, b| a.lang.cmp(&b.lang));
        return;
    }

    let cpp_ratio = cpp_data.share / share_sum;
    let c_ratio = 1.0 - cpp_ratio;
    let entries = [("C++", cpp_ratio), ("C", c_ratio)];

    for (lang, ratio) in entries {
        let share = combined.share * ratio;
        let trend = combined.trend.map(|value| value * ratio);
        let splitted = RankingEntry {
            lang: lang.to_string(),
            rank: combined.rank,
            share,
            trend,
        };
        pypl.push(splitted);
    }

    pypl.sort_by(|a, b| a.lang.cmp(&b.lang));
}

#[derive(Debug, Serialize, Copy, Clone, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum SourceKind {
    Tiobe,
    Pypl,
}

#[derive(Debug, Serialize)]
struct CsvRecord {
    source: SourceKind,
    lang: String,
    rank: Option<u32>,
    share: f64,
    trend: Option<f64>,
}

fn language_aliases() -> &'static FxHashMap<&'static str, &'static str> {
    static LANGUAGE_ALIASES: OnceLock<FxHashMap<&'static str, &'static str>> = OnceLock::new();
    LANGUAGE_ALIASES.get_or_init(|| {
        [
            ("Delphi/Object Pascal", "Delphi/Pascal"),
            ("MATLAB", "Matlab"),
            ("Cobol", "COBOL"),
            ("Powershell", "PowerShell"),
            ("VBScript", "VBA/VBS"),
            ("VBA", "VBA/VBS"),
            ("ABAP", "Abap"),
            ("(Visual) FoxPro", "FoxPro"),
        ]
        .into_iter()
        .collect()
    })
}

#[derive(Debug, Serialize)]
struct SchulzeRecord {
    position: usize,
    lang: String,
    tiobe_rank: Option<u32>,
    tiobe_share: f64,
    tiobe_trend: Option<f64>,
    pypl_rank: Option<u32>,
    pypl_share: f64,
    pypl_trend: Option<f64>,
    benchmark_elapsed: f64,
    schulze_wins: usize,
}

async fn save_schulze_csv(records: &[SchulzeRecord], output_path: &Path) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let mut writer = Writer::from_writer(Vec::new());
    for record in records {
        writer
            .serialize(record)
            .context("failed to serialize Schulze ranking record")?;
    }
    writer
        .flush()
        .context("failed to flush Schulze ranking writer")?;
    let serialized = writer
        .into_inner()
        .context("failed to finalize Schulze ranking output")?;

    fs::write(output_path, serialized)
        .await
        .with_context(|| format!("failed to write {}", output_path.display()))?;

    Ok(())
}

fn compute_schulze_records(
    tiobe: &[RankingEntry],
    pypl: &[RankingEntry],
    benchmark: &FxHashMap<String, f64>,
) -> Result<Vec<SchulzeRecord>> {
    let tiobe_index: FxHashMap<&str, usize> = tiobe
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.lang.as_str(), idx))
        .collect();
    let pypl_index: FxHashMap<&str, usize> = pypl
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.lang.as_str(), idx))
        .collect();

    let tiobe_set: FxHashSet<String> = tiobe.iter().map(|entry| entry.lang.clone()).collect();
    let pypl_set: FxHashSet<String> = pypl.iter().map(|entry| entry.lang.clone()).collect();
    let bench_set: FxHashSet<String> = benchmark.keys().cloned().collect();

    let mut languages: Vec<String> = tiobe_set
        .intersection(&pypl_set)
        .filter(|lang| bench_set.contains(*lang))
        .cloned()
        .collect();

    if languages.len() < 2 {
        return Err(anyhow!(
            "Not enough overlapping languages ({}) to compute Schulze ranking",
            languages.len()
        ));
    }

    languages.sort();

    let mut tiobe_order = languages.clone();
    tiobe_order.sort_by(|a, b| compare_descending(tiobe, &tiobe_index, a.as_str(), b.as_str()));
    let mut pypl_order = languages.clone();
    pypl_order.sort_by(|a, b| compare_descending(pypl, &pypl_index, a.as_str(), b.as_str()));
    let mut performance_order = languages.clone();
    performance_order.sort_by(|a, b| compare_ascending(benchmark, a, b));

    let ballots = vec![tiobe_order, pypl_order, performance_order];
    let (_d, p) = build_preference_matrices(&languages, &ballots);

    let index_map: FxHashMap<&str, usize> = languages
        .iter()
        .enumerate()
        .map(|(idx, lang)| (lang.as_str(), idx))
        .collect();

    let mut ranked = languages.clone();
    ranked.sort_by(|a, b| {
        let i_a = index_map[a.as_str()];
        let i_b = index_map[b.as_str()];
        match p[[i_a, i_b]].cmp(&p[[i_b, i_a]]) {
            Ordering::Greater => Ordering::Less,
            Ordering::Less => Ordering::Greater,
            Ordering::Equal => {
                let score_a = combined_score(
                    a.as_str(),
                    tiobe,
                    &tiobe_index,
                    pypl,
                    &pypl_index,
                    benchmark,
                );
                let score_b = combined_score(
                    b.as_str(),
                    tiobe,
                    &tiobe_index,
                    pypl,
                    &pypl_index,
                    benchmark,
                );
                match score_b.partial_cmp(&score_a).unwrap_or(Ordering::Equal) {
                    Ordering::Equal => a.cmp(b),
                    other => other,
                }
            }
        }
    });

    let mut records = Vec::with_capacity(languages.len());
    for (position, lang) in ranked.iter().enumerate() {
        let idx = index_map[lang.as_str()];
        let wins = (0..languages.len())
            .filter(|&other| other != idx && p[[idx, other]] > p[[other, idx]])
            .count();

        let tiobe_entry = tiobe_index
            .get(lang.as_str())
            .and_then(|&entry_idx| tiobe.get(entry_idx))
            .ok_or_else(|| anyhow!("missing TIOBE data for {lang}"))?;
        let pypl_entry = pypl_index
            .get(lang.as_str())
            .and_then(|&entry_idx| pypl.get(entry_idx))
            .ok_or_else(|| anyhow!("missing PYPL data for {lang}"))?;
        let bench_value = benchmark
            .get(lang)
            .copied()
            .ok_or_else(|| anyhow!("missing benchmark data for {lang}"))?;

        records.push(SchulzeRecord {
            position: position + 1,
            lang: lang.clone(),
            tiobe_rank: tiobe_entry.rank,
            tiobe_share: tiobe_entry.share,
            tiobe_trend: tiobe_entry.trend,
            pypl_rank: pypl_entry.rank,
            pypl_share: pypl_entry.share,
            pypl_trend: pypl_entry.trend,
            benchmark_elapsed: bench_value,
            schulze_wins: wins,
        });
    }

    Ok(records)
}

fn compare_descending(
    entries: &[RankingEntry],
    index_map: &FxHashMap<&str, usize>,
    a: &str,
    b: &str,
) -> Ordering {
    let share_for = |lang: &str| {
        index_map
            .get(lang)
            .and_then(|&idx| entries.get(idx))
            .map_or(0.0, |entry| entry.share)
    };
    let a_share = share_for(a);
    let b_share = share_for(b);
    b_share
        .partial_cmp(&a_share)
        .unwrap_or(Ordering::Equal)
        .then_with(|| a.cmp(b))
}

fn compare_ascending(map: &FxHashMap<String, f64>, a: &str, b: &str) -> Ordering {
    let a_value = map.get(a).copied().unwrap_or(f64::INFINITY);
    let b_value = map.get(b).copied().unwrap_or(f64::INFINITY);
    a_value
        .partial_cmp(&b_value)
        .unwrap_or(Ordering::Equal)
        .then_with(|| a.cmp(b))
}

fn build_preference_matrices(
    languages: &[String],
    ballots: &[Vec<String>],
) -> (Array2<usize>, Array2<usize>) {
    let n = languages.len();
    let index_map: FxHashMap<&str, usize> = languages
        .iter()
        .enumerate()
        .map(|(idx, lang)| (lang.as_str(), idx))
        .collect();

    let mut d = Array2::<usize>::zeros((n, n));
    for ballot in ballots {
        let mut positions = vec![0usize; n];
        for (pos, lang) in ballot.iter().enumerate() {
            if let Some(&idx) = index_map.get(lang.as_str()) {
                positions[idx] = pos;
            }
        }
        for i in 0..n {
            for j in 0..n {
                if i != j && positions[i] < positions[j] {
                    d[[i, j]] += 1;
                }
            }
        }
    }

    let mut p = Array2::<usize>::zeros((n, n));
    for i in 0..n {
        for j in 0..n {
            if i == j {
                continue;
            }
            if d[[i, j]] > d[[j, i]] {
                p[[i, j]] = d[[i, j]];
            }
        }
    }

    for i in 0..n {
        for j in 0..n {
            if i == j {
                continue;
            }
            for k in 0..n {
                if i == k || j == k {
                    continue;
                }
                let candidate = p[[j, i]].min(p[[i, k]]);
                if candidate > p[[j, k]] {
                    p[[j, k]] = candidate;
                }
            }
        }
    }

    (d, p)
}

fn combined_score(
    lang: &str,
    tiobe: &[RankingEntry],
    tiobe_index: &FxHashMap<&str, usize>,
    pypl: &[RankingEntry],
    pypl_index: &FxHashMap<&str, usize>,
    benchmark: &FxHashMap<String, f64>,
) -> f64 {
    let share_from = |index: &FxHashMap<&str, usize>, entries: &[RankingEntry]| {
        index
            .get(lang)
            .and_then(|&idx| entries.get(idx))
            .map_or(0.0, |entry| entry.share)
    };
    let tiobe_share = share_from(tiobe_index, tiobe);
    let pypl_share = share_from(pypl_index, pypl);
    let perf = benchmark.get(lang).copied().unwrap_or(f64::INFINITY);
    let perf_component = if perf > 0.0 && perf.is_finite() {
        1.0 / perf
    } else {
        0.0
    };
    tiobe_share + pypl_share + perf_component
}

async fn load_benchmark_stats(bytes: &[u8]) -> Result<FxHashMap<String, f64>> {
    let data = bytes.to_vec();
    let stats = task::spawn_blocking(move || compute_benchmark_stats_sync(&data))
        .await
        .context("failed to read benchmark statistics")??;
    Ok(stats)
}

fn compute_benchmark_stats_sync(data: &[u8]) -> Result<FxHashMap<String, f64>> {
    let cursor = Cursor::new(data);
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(cursor);

    let headers = reader
        .headers()
        .context("missing CSV headers in benchmark data")?
        .clone();
    let idx_lang = headers
        .iter()
        .position(|h| h == "lang")
        .ok_or_else(|| anyhow!("missing 'lang' column in benchmark data"))?;
    let idx_name = headers
        .iter()
        .position(|h| h == "name")
        .ok_or_else(|| anyhow!("missing 'name' column in benchmark data"))?;
    let idx_status = headers
        .iter()
        .position(|h| h == "status")
        .ok_or_else(|| anyhow!("missing 'status' column in benchmark data"))?;
    let idx_elapsed = headers
        .iter()
        .position(|h| h == "elapsed-time(s)")
        .ok_or_else(|| anyhow!("missing 'elapsed-time(s)' column in benchmark data"))?;

    let alias_map = benchmark_aliases();
    let mut best_per_problem: FxHashMap<(String, String), f64> = FxHashMap::default();

    for record in reader.records() {
        let record = record.context("failed to read benchmark record")?;
        let status_str = record.get(idx_status).unwrap_or("").trim();
        let status: i64 = match status_str.parse::<i64>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        if status < 0 {
            continue;
        }
        let lang_raw = record.get(idx_lang).unwrap_or("").trim();
        let name = record.get(idx_name).unwrap_or("").trim();
        let elapsed_str = record.get(idx_elapsed).unwrap_or("").trim();
        if lang_raw.is_empty() || name.is_empty() || elapsed_str.is_empty() {
            continue;
        }
        let elapsed: f64 = match elapsed_str.parse::<f64>() {
            Ok(value) if value.is_finite() && value > 0.0 => value,
            _ => continue,
        };

        let key = (lang_raw.to_lowercase(), name.to_string());
        let entry = best_per_problem.entry(key).or_insert(f64::INFINITY);
        if elapsed < *entry {
            *entry = elapsed;
        }
    }

    let mut per_lang: FxHashMap<String, Vec<f64>> = FxHashMap::default();
    for ((lang, _name), elapsed) in best_per_problem {
        if !elapsed.is_finite() || elapsed <= 0.0 {
            continue;
        }
        if let Some(canonical) = canonical_benchmark_lang(&lang, alias_map) {
            if canonical.is_empty() {
                continue;
            }
            per_lang.entry(canonical).or_default().push(elapsed);
        }
    }

    let mut medians: FxHashMap<String, f64> = FxHashMap::default();
    for (lang, mut values) in per_lang {
        let len = values.len();
        if len == 0 {
            continue;
        }
        let median = if len % 2 == 1 {
            let mid = len / 2;
            let (_, median, _) = values.select_nth_unstable_by(mid, f64::total_cmp);
            *median
        } else {
            let mid = len / 2;
            let (lower_part, upper, _) = values.select_nth_unstable_by(mid, f64::total_cmp);
            let lower = lower_part
                .iter()
                .copied()
                .max_by(f64::total_cmp)
                .unwrap_or(*upper);
            f64::midpoint(lower, *upper)
        };
        if median.is_finite() {
            medians.insert(lang, median);
        }
    }

    if let Some(value) = medians.get("C/C++").copied() {
        medians.insert("C".to_string(), value);
        medians.insert("C++".to_string(), value);
    }

    Ok(medians)
}

fn canonical_benchmark_lang(
    lang: &str,
    alias_map: &FxHashMap<&'static str, &'static str>,
) -> Option<String> {
    let key = lang.to_lowercase();
    if let Some(&alias) = alias_map.get(key.as_str()) {
        if alias.is_empty() {
            return None;
        }
        return Some(alias.to_string());
    }
    Some(capitalize_word(&key))
}

fn benchmark_aliases() -> &'static FxHashMap<&'static str, &'static str> {
    static BENCHMARK_ALIASES: OnceLock<FxHashMap<&'static str, &'static str>> = OnceLock::new();
    BENCHMARK_ALIASES.get_or_init(|| {
        [
            ("chapel", "Chapel"),
            ("clang", "C/C++"),
            ("csharpaot", "C#"),
            ("csharpcore", "C#"),
            ("dartexe", "Dart"),
            ("dartjit", "Dart"),
            ("erlang", "Erlang"),
            ("fpascal", "Free Pascal"),
            ("fsharpcore", "F#"),
            ("gcc", "C/C++"),
            ("ghc", "Haskell"),
            ("gnat", "Ada"),
            ("go", "Go"),
            ("gpp", "C/C++"),
            ("graalvm", "Graal"),
            ("icx", "C/C++"),
            ("ifc", "Fortran"),
            ("ifx", "Fortran"),
            ("java", "Java"),
            ("javaxint", "Java"),
            ("julia", "Julia"),
            ("lua", "Lua"),
            ("micropython", "Python"),
            ("mri", "Ruby"),
            ("node", "JavaScript"),
            ("ocaml", "OCaml"),
            ("openj9", "Java"),
            ("perl", "Perl"),
            ("pharo", "Smalltalk"),
            ("php", "PHP"),
            ("python3", "Python"),
            ("racket", "Racket"),
            ("ruby", "Ruby"),
            ("rust", "Rust"),
            ("sbcl", "Lisp"),
            ("swift", "Swift"),
            ("toit", "Toit"),
            ("vw", ""),
        ]
        .into_iter()
        .collect()
    })
}

fn capitalize_word(input: &str) -> String {
    let mut chars = input.chars();
    if let Some(first) = chars.next() {
        let mut output = String::new();
        output.extend(first.to_uppercase());
        output.push_str(&chars.as_str().to_lowercase());
        output
    } else {
        String::new()
    }
}

fn extract_cell_text(cell: scraper::ElementRef<'_>) -> String {
    cell.text()
        .map(str::trim)
        .filter(|chunk| !chunk.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_u32(value: &str) -> Option<u32> {
    value
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>()
        .parse::<u32>()
        .ok()
}

fn parse_percent(value: &str) -> Option<f64> {
    let trimmed = value.trim().trim_end_matches('%').trim();
    if trimmed.is_empty() {
        return None;
    }
    let cleaned = trimmed
        .replace(',', ".")
        .replace(['+', '\u{00a0}'], "")
        .trim()
        .to_string();
    if cleaned.is_empty() {
        return None;
    }
    cleaned.parse::<f64>().ok()
}

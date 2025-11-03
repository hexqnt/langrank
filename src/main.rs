#![warn(clippy::pedantic)]

use crate::cli::Cli;
use crate::sources::{
    download_benchmark_data, fetch_languish, fetch_pypl, fetch_tiobe, load_benchmark_stats,
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Local};
use clap::Parser;
use colored::Colorize;
use csv::Writer;
use ndarray::{Array2, Zip};
use reqwest::Client;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use std::cmp::Ordering;
use std::path::Path;
use std::time::Duration;
use tokio::fs;

mod cli;
mod sources;

const HTTP_TIMEOUT_SECONDS: u64 = 20;

#[derive(Debug, Serialize, Clone)]
pub struct RankingEntry {
    pub lang: String,
    pub rank: Option<u32>,
    pub share: f64,
    pub trend: Option<f64>,
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

    let (tiobe, mut pypl, languish, bench_bytes) = tokio::try_join!(
        fetch_tiobe(&client),
        fetch_pypl(&client),
        fetch_languish(&client),
        download_benchmark_data(&client)
    )?;

    let pypl_original_len = pypl.len();
    adjust_pypl_entries(&tiobe, &mut pypl);

    if let Some(path) = save_rankings.as_ref() {
        save_rankings_csv(
            path.as_path(),
            &[
                (SourceKind::Tiobe, &tiobe),
                (SourceKind::Pypl, &pypl),
                (SourceKind::Languish, &languish),
            ],
        )
        .await?;
    }

    if let Some(path) = save_benchmarks.as_ref() {
        save_benchmarks_csv(&bench_bytes, path.as_path()).await?;
    }

    let benchmark_stats = load_benchmark_stats(&bench_bytes).await?;
    let benchmark_lang_count = benchmark_stats.len();
    let schulze_records = compute_schulze_records(&tiobe, &pypl, &languish, &benchmark_stats)?;
    if let Some(path) = save_schulze.as_ref() {
        save_schulze_csv(&schulze_records, path.as_path()).await?;
    }

    print_summary(&SummaryContext {
        tiobe_count: tiobe.len(),
        pypl_count: pypl_original_len,
        languish_count: languish.len(),
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
    languish_count: usize,
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
        "{} {} | {} | {} | {}",
        "Sources".bright_yellow().bold(),
        format!("TIOBE: {}", context.tiobe_count).bright_white(),
        format!("PYPL: {}", context.pypl_count).bright_white(),
        format!("Languish: {}", context.languish_count).bright_white(),
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
        "{:>3} | {:<13} | {:>6} | {:>6} | {:>7} | {:>6} | {:>6} | {:>7} | {:>6} | {:>7} | {:>8} | {:>4}",
        "Pos",
        "Language",
        "T Rank",
        "T%",
        "T Trend",
        "P Rank",
        "P%",
        "P Trend",
        "L%",
        "L Trend",
        "Perf(s)",
        "Wins"
    );
    println!("{}", header.bold().bright_white());
    println!(
        "{}",
        "----+---------------+--------+--------+---------+--------+--------+---------+--------+---------+--------+------"
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
        let languish_share = format!("{:.2}", record.languish_share);
        let languish_trend = format_trend(record.languish_trend);
        let perf = format!("{:.2}", record.benchmark_elapsed);
        let line = format!(
            "{:>3} | {:<13} | {:>6} | {:>6} | {:>7} | {:>6} | {:>6} | {:>7} | {:>6} | {:>7} | {:>8} | {:>4}",
            record.position,
            record.lang,
            tiobe_rank,
            tiobe_share,
            tiobe_trend,
            pypl_rank,
            pypl_share,
            pypl_trend,
            languish_share,
            languish_trend,
            perf,
            record.schulze_wins
        );
        println!("{}", line.bright_green());
    }
}

fn print_compact_schulze_table(records: &[SchulzeRecord]) {
    println!(
        "{}",
        "Pos | Language      | TIOBE% | PYPL% | LANG% | Perf(s) | Wins"
            .to_string()
            .bold()
            .bright_white()
    );
    println!(
        "{}",
        "----+---------------+--------+-------+------+---------+------".bright_black()
    );
    for record in records.iter().take(10) {
        let line = format!(
            "{:>3} | {:<13} | {:>6.2} | {:>5.2} | {:>5.2} | {:>7.2} | {:>4}",
            record.position,
            record.lang,
            record.tiobe_share,
            record.pypl_share,
            record.languish_share,
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

async fn save_benchmarks_csv(bytes: &[u8], path: &Path) -> Result<()> {
    write_output_file(path, bytes).await
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

async fn write_output_file(path: &Path, bytes: &[u8]) -> Result<()> {
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

fn finalize_writer(mut writer: Writer<Vec<u8>>, label: &str) -> Result<Vec<u8>> {
    writer
        .flush()
        .with_context(|| format!("failed to flush {label}"))?;
    writer
        .into_inner()
        .with_context(|| format!("failed to finalize {label}"))
}

async fn save_rankings_csv(path: &Path, sources: &[(SourceKind, &[RankingEntry])]) -> Result<()> {
    let serialized = serialize_rankings(sources)?;
    write_output_file(path, &serialized).await
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
    finalize_writer(writer, "ranking CSV writer")
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
    Languish,
}

#[derive(Debug, Serialize)]
struct CsvRecord {
    source: SourceKind,
    lang: String,
    rank: Option<u32>,
    share: f64,
    trend: Option<f64>,
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
    languish_rank: Option<u32>,
    languish_share: f64,
    languish_trend: Option<f64>,
    benchmark_elapsed: f64,
    schulze_wins: usize,
}

async fn save_schulze_csv(records: &[SchulzeRecord], output_path: &Path) -> Result<()> {
    let mut writer = Writer::from_writer(Vec::new());
    for record in records {
        writer
            .serialize(record)
            .context("failed to serialize Schulze ranking record")?;
    }
    let serialized = finalize_writer(writer, "Schulze ranking writer")?;
    write_output_file(output_path, &serialized).await
}

fn compute_schulze_records(
    tiobe: &[RankingEntry],
    pypl: &[RankingEntry],
    languish: &[RankingEntry],
    benchmark: &FxHashMap<String, f64>,
) -> Result<Vec<SchulzeRecord>> {
    let tiobe_index = build_ranking_index(tiobe);
    let pypl_index = build_ranking_index(pypl);
    let languish_index = build_ranking_index(languish);
    let sources = RankingSources {
        tiobe: RankingSource {
            entries: tiobe,
            index: &tiobe_index,
        },
        pypl: RankingSource {
            entries: pypl,
            index: &pypl_index,
        },
        languish: RankingSource {
            entries: languish,
            index: &languish_index,
        },
        benchmark,
    };
    let languages = collect_languages(tiobe, pypl, languish, benchmark)?;
    let ballots = build_ballots(&languages, &sources);
    let (_d, p) = build_preference_matrices(&languages, &ballots);
    let index_map = build_language_index(&languages);
    let ranked = rank_languages(&languages, &p, &index_map, &sources);

    let mut records = Vec::with_capacity(languages.len());
    for (position, lang) in ranked.iter().enumerate() {
        let idx = index_map[lang.as_str()];
        let wins = (0..languages.len())
            .filter(|&other| other != idx && p[[idx, other]] > p[[other, idx]])
            .count();

        let tiobe_entry = sources
            .tiobe
            .entry(lang)
            .ok_or_else(|| anyhow!("missing TIOBE data for {lang}"))?;
        let pypl_entry = sources
            .pypl
            .entry(lang)
            .ok_or_else(|| anyhow!("missing PYPL data for {lang}"))?;
        let languish_entry = sources
            .languish
            .entry(lang)
            .ok_or_else(|| anyhow!("missing Languish data for {lang}"))?;
        let bench_value = sources
            .benchmark_value(lang)
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
            languish_rank: languish_entry.rank,
            languish_share: languish_entry.share,
            languish_trend: languish_entry.trend,
            benchmark_elapsed: bench_value,
            schulze_wins: wins,
        });
    }

    Ok(records)
}

struct RankingSource<'a> {
    entries: &'a [RankingEntry],
    index: &'a FxHashMap<&'a str, usize>,
}

impl<'a> RankingSource<'a> {
    fn entry(&self, lang: &str) -> Option<&'a RankingEntry> {
        self.index.get(lang).and_then(|&idx| self.entries.get(idx))
    }

    fn share(&self, lang: &str) -> f64 {
        self.entry(lang).map_or(0.0, |entry| entry.share)
    }
}

struct RankingSources<'a> {
    tiobe: RankingSource<'a>,
    pypl: RankingSource<'a>,
    languish: RankingSource<'a>,
    benchmark: &'a FxHashMap<String, f64>,
}

impl RankingSources<'_> {
    fn benchmark_value(&self, lang: &str) -> Option<f64> {
        self.benchmark.get(lang).copied()
    }
}

fn build_ranking_index(entries: &[RankingEntry]) -> FxHashMap<&str, usize> {
    entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.lang.as_str(), idx))
        .collect()
}

fn collect_languages(
    tiobe: &[RankingEntry],
    pypl: &[RankingEntry],
    languish: &[RankingEntry],
    benchmark: &FxHashMap<String, f64>,
) -> Result<Vec<String>> {
    let tiobe_set: FxHashSet<String> = tiobe.iter().map(|entry| entry.lang.clone()).collect();
    let pypl_set: FxHashSet<String> = pypl.iter().map(|entry| entry.lang.clone()).collect();
    let languish_set: FxHashSet<String> = languish.iter().map(|entry| entry.lang.clone()).collect();
    let bench_set: FxHashSet<String> = benchmark.keys().cloned().collect();

    let mut languages: Vec<String> = tiobe_set
        .intersection(&pypl_set)
        .filter(|lang| languish_set.contains(*lang))
        .filter(|lang| bench_set.contains(*lang))
        .cloned()
        .collect();

    if languages.len() < 2 {
        return Err(anyhow!(
            "Not enough overlapping languages ({}) to compute Schulze ranking",
            languages.len()
        ));
    }

    languages.sort_unstable();
    Ok(languages)
}

fn build_ballots(languages: &[String], sources: &RankingSources<'_>) -> Vec<Vec<String>> {
    let mut tiobe_order = languages.to_vec();
    tiobe_order.sort_by(|a, b| compare_by_share(a.as_str(), b.as_str(), &sources.tiobe));

    let mut pypl_order = languages.to_vec();
    pypl_order.sort_by(|a, b| compare_by_share(a.as_str(), b.as_str(), &sources.pypl));

    let mut languish_order = languages.to_vec();
    languish_order.sort_by(|a, b| compare_by_share(a.as_str(), b.as_str(), &sources.languish));

    let mut performance_order = languages.to_vec();
    performance_order.sort_by(|a, b| compare_ascending(sources.benchmark, a, b));

    vec![tiobe_order, pypl_order, languish_order, performance_order]
}

fn build_language_index(languages: &[String]) -> FxHashMap<&str, usize> {
    languages
        .iter()
        .enumerate()
        .map(|(idx, lang)| (lang.as_str(), idx))
        .collect()
}

fn rank_languages(
    languages: &[String],
    preference_strengths: &Array2<usize>,
    index_map: &FxHashMap<&str, usize>,
    sources: &RankingSources<'_>,
) -> Vec<String> {
    let mut ranked = languages.to_vec();
    ranked.sort_by(|a, b| {
        let i_a = index_map[a.as_str()];
        let i_b = index_map[b.as_str()];
        match preference_strengths[[i_a, i_b]].cmp(&preference_strengths[[i_b, i_a]]) {
            Ordering::Greater => Ordering::Less,
            Ordering::Less => Ordering::Greater,
            Ordering::Equal => {
                let score_a = combined_score(a.as_str(), sources);
                let score_b = combined_score(b.as_str(), sources);
                match score_b.partial_cmp(&score_a).unwrap_or(Ordering::Equal) {
                    Ordering::Equal => a.cmp(b),
                    other => other,
                }
            }
        }
    });
    ranked
}

fn compare_by_share(a: &str, b: &str, source: &RankingSource<'_>) -> Ordering {
    let a_share = source.share(a);
    let b_share = source.share(b);
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
    Zip::from(&mut p)
        .and(&d)
        .and(&d.t())
        .for_each(|p_cell, &d_ij, &d_ji| {
            if d_ij > d_ji {
                *p_cell = d_ij;
            }
        });

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

fn combined_score(lang: &str, sources: &RankingSources<'_>) -> f64 {
    let tiobe_share = sources.tiobe.share(lang);
    let pypl_share = sources.pypl.share(lang);
    let languish_share = sources.languish.share(lang);
    let perf = sources.benchmark_value(lang).unwrap_or(f64::INFINITY);
    let perf_component = if perf > 0.0 && perf.is_finite() {
        1.0 / perf
    } else {
        0.0
    };
    tiobe_share + pypl_share + languish_share + perf_component
}

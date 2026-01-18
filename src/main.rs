use crate::cli::Cli;
use crate::report::{HtmlReportContext, HtmlReportPaths, save_html_report};
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
use rustc_hash::FxHashMap;
use serde::Serialize;
use std::cmp::Ordering;
use std::path::Path;
use std::time::Duration;
use tokio::fs;

mod cli;
mod report;
mod sources;

const HTTP_TIMEOUT_SECONDS: u64 = 20;
const MIN_SOURCE_OVERLAP: usize = 3;
const MAX_RANKED_LANGUAGES: usize = 30;

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
        save_html,
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

    let benchmark_stats = load_benchmark_stats(bench_bytes).await?;
    let benchmark_lang_count = benchmark_stats.len();
    let schulze_records = compute_schulze_records(&tiobe, &pypl, &languish, &benchmark_stats)?;
    if let Some(path) = save_schulze.as_ref() {
        save_schulze_csv(&schulze_records, path.as_path()).await?;
    }

    if let Some(path) = save_html.as_ref() {
        let html_context = HtmlReportContext {
            tiobe_count: tiobe.len(),
            pypl_count: pypl_original_len,
            languish_count: languish.len(),
            benchmark_lang_count,
            run_started_at: &run_started_at,
            schulze_records: &schulze_records,
            full_output,
            paths: HtmlReportPaths {
                benchmarks: save_benchmarks.as_deref(),
                rankings: save_rankings.as_deref(),
                schulze: save_schulze.as_deref(),
            },
            output_path: path.as_path(),
        };
        save_html_report(path.as_path(), &html_context).await?;
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
            html: save_html.as_deref(),
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
    html: Option<&'a Path>,
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
        "=============================================================".bright_cyan()
    );
}

fn print_summary_header(context: &SummaryContext<'_>) {
    println!(
        "{}",
        "====================== LangRank Update ======================"
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
    print_path_line("HTML Report", paths.html, "not saved (use --save-html)");
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
        let perf = format_perf(record.benchmark_elapsed);
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
        let perf = format_perf(record.benchmark_elapsed);
        let line = format!(
            "{:>3} | {:<13} | {:>6.2} | {:>5.2} | {:>5.2} | {:>7} | {:>4}",
            record.position,
            record.lang,
            record.tiobe_share,
            record.pypl_share,
            record.languish_share,
            perf,
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
    trend.map_or_else(
        || "-".to_string(),
        |value| {
            let normalized = if value.abs() < 0.005 { 0.0 } else { value };
            format!("{normalized:+.2}")
        },
    )
}

fn format_perf(value: Option<f64>) -> String {
    match value {
        Some(v) if v.is_finite() => format!("{v:.2}"),
        _ => "-".to_string(),
    }
}

pub(crate) async fn write_output_file(path: &Path, bytes: &[u8]) -> Result<()> {
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
                lang: entry.lang.as_str(),
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
struct CsvRecord<'a> {
    source: SourceKind,
    lang: &'a str,
    rank: Option<u32>,
    share: f64,
    trend: Option<f64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SchulzeRecord {
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
    benchmark_elapsed: Option<f64>,
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
    let languages = collect_languages(tiobe, pypl, languish, benchmark, MIN_SOURCE_OVERLAP);
    let languages = limit_languages(languages, &sources, MAX_RANKED_LANGUAGES);
    if languages.len() < 2 {
        return Err(anyhow!(
            "Not enough overlapping languages ({}) to compute Schulze ranking",
            languages.len()
        ));
    }
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

        let tiobe_entry = sources.tiobe.entry(lang);
        let pypl_entry = sources.pypl.entry(lang);
        let languish_entry = sources.languish.entry(lang);
        let bench_value = sources.benchmark_value(lang);

        records.push(SchulzeRecord {
            position: position + 1,
            lang: lang.clone(),
            tiobe_rank: tiobe_entry.and_then(|entry| entry.rank),
            tiobe_share: tiobe_entry.map_or(0.0, |entry| entry.share),
            tiobe_trend: tiobe_entry.and_then(|entry| entry.trend),
            pypl_rank: pypl_entry.and_then(|entry| entry.rank),
            pypl_share: pypl_entry.map_or(0.0, |entry| entry.share),
            pypl_trend: pypl_entry.and_then(|entry| entry.trend),
            languish_rank: languish_entry.and_then(|entry| entry.rank),
            languish_share: languish_entry.map_or(0.0, |entry| entry.share),
            languish_trend: languish_entry.and_then(|entry| entry.trend),
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
    min_sources: usize,
) -> Vec<std::string::String> {
    let mut counts: FxHashMap<String, usize> = FxHashMap::default();
    for entry in tiobe {
        *counts.entry(entry.lang.clone()).or_insert(0) += 1;
    }
    for entry in pypl {
        *counts.entry(entry.lang.clone()).or_insert(0) += 1;
    }
    for entry in languish {
        *counts.entry(entry.lang.clone()).or_insert(0) += 1;
    }
    for lang in benchmark.keys() {
        *counts.entry(lang.clone()).or_insert(0) += 1;
    }

    let mut languages: Vec<String> = counts
        .into_iter()
        .filter(|(_, count)| *count >= min_sources)
        .map(|(lang, _)| lang)
        .collect();
    languages.sort_unstable();
    languages
}

fn limit_languages(
    languages: Vec<String>,
    sources: &RankingSources<'_>,
    max_languages: usize,
) -> Vec<String> {
    if max_languages == 0 || languages.len() <= max_languages {
        return languages;
    }

    let mut scored: Vec<(usize, f64, f64, String)> = Vec::with_capacity(languages.len());
    for lang in languages {
        let lang_ref = lang.as_str();
        let source_count = count_sources(lang_ref, sources);
        let popularity_score = sources.tiobe.share(lang_ref)
            + sources.pypl.share(lang_ref)
            + sources.languish.share(lang_ref);
        let perf_component = sources
            .benchmark_value(lang_ref)
            .and_then(|value| {
                if value.is_finite() && value > 0.0 {
                    Some(1.0 / value)
                } else {
                    None
                }
            })
            .unwrap_or(0.0);
        scored.push((source_count, popularity_score, perf_component, lang));
    }

    let cmp_scores =
        |(count_a, pop_a, perf_a, lang_a): &(usize, f64, f64, String),
         (count_b, pop_b, perf_b, lang_b): &(usize, f64, f64, String)| {
            count_b
                .cmp(count_a)
                .then_with(|| pop_b.partial_cmp(pop_a).unwrap_or(Ordering::Equal))
                .then_with(|| perf_b.partial_cmp(perf_a).unwrap_or(Ordering::Equal))
                .then_with(|| lang_a.cmp(lang_b))
        };
    let nth = max_languages.saturating_sub(1);
    scored.select_nth_unstable_by(nth, cmp_scores);
    scored.truncate(max_languages);

    let mut limited: Vec<String> = scored.into_iter().map(|(_, _, _, lang)| lang).collect();
    limited.sort_unstable();
    limited
}

fn count_sources(lang: &str, sources: &RankingSources<'_>) -> usize {
    let mut count = 0;
    if sources.tiobe.entry(lang).is_some() {
        count += 1;
    }
    if sources.pypl.entry(lang).is_some() {
        count += 1;
    }
    if sources.languish.entry(lang).is_some() {
        count += 1;
    }
    if sources.benchmark.contains_key(lang) {
        count += 1;
    }
    count
}

fn build_ballots(languages: &[String], sources: &RankingSources<'_>) -> Vec<Vec<String>> {
    let tiobe_order = order_by_metric(languages, |lang| sources.tiobe.share(lang), false);
    let pypl_order = order_by_metric(languages, |lang| sources.pypl.share(lang), false);
    let languish_order = order_by_metric(languages, |lang| sources.languish.share(lang), false);
    let performance_order = order_by_metric(
        languages,
        |lang| {
            sources
                .benchmark
                .get(lang)
                .copied()
                .unwrap_or(f64::INFINITY)
        },
        true,
    );

    vec![tiobe_order, pypl_order, languish_order, performance_order]
}

fn order_by_metric<F>(languages: &[String], metric: F, ascending: bool) -> Vec<String>
where
    F: Fn(&str) -> f64,
{
    let mut scored = Vec::with_capacity(languages.len());
    for (idx, lang) in languages.iter().enumerate() {
        scored.push((idx, metric(lang.as_str())));
    }
    scored.sort_by(|(idx_a, score_a), (idx_b, score_b)| {
        let mut ord = score_a.partial_cmp(score_b).unwrap_or(Ordering::Equal);
        if !ascending {
            ord = ord.reverse();
        }
        ord.then_with(|| languages[*idx_a].cmp(&languages[*idx_b]))
    });
    scored
        .into_iter()
        .map(|(idx, _)| languages[idx].clone())
        .collect()
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
    let combined_scores: Vec<f64> = languages
        .iter()
        .map(|lang| combined_score(lang.as_str(), sources))
        .collect();
    let mut ranked = languages.to_vec();
    ranked.sort_by(|a, b| {
        let i_a = index_map[a.as_str()];
        let i_b = index_map[b.as_str()];
        match preference_strengths[[i_a, i_b]].cmp(&preference_strengths[[i_b, i_a]]) {
            Ordering::Greater => Ordering::Less,
            Ordering::Less => Ordering::Greater,
            Ordering::Equal => {
                let score_a = combined_scores[i_a];
                let score_b = combined_scores[i_b];
                match score_b.partial_cmp(&score_a).unwrap_or(Ordering::Equal) {
                    Ordering::Equal => a.cmp(b),
                    other => other,
                }
            }
        }
    });
    ranked
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

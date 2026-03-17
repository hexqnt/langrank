use crate::cli::Cli;
use crate::progress::{ProgressState, Stage, run_with_spinner};
use crate::report::{HtmlReportContext, HtmlReportPaths, save_html_report};
use crate::schulze::{SchulzeConfig, SchulzeRecord, compute_schulze_records};
use crate::sources::{
    TECHEMPOWER_MAX_SCORE, download_benchmark_data, fetch_languish, fetch_pypl, fetch_techempower,
    fetch_tiobe, load_benchmark_scores,
};
use crate::summary::{SummaryContext, SummaryPaths, print_summary};
use anyhow::{Context, Result, anyhow};
use chrono::Local;
use clap::Parser;
use csv::Writer;
use flate2::Compression;
use flate2::write::GzEncoder;
use reqwest::Client;
use serde::Serialize;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs;

mod cli;
mod formatting;
mod parsing;
mod progress;
mod report;
mod schulze;
mod sources;
mod summary;

const HTTP_TIMEOUT_SECONDS: u64 = 20;
const MIN_SOURCE_ENTRIES: usize = 10;
const MIN_BENCHMARK_LANGUAGES: usize = 10;
const MIN_TECHEMPOWER_LANGUAGES: usize = 10;
const MIN_SOURCE_OVERLAP: usize = 3;
const MAX_RANKED_LANGUAGES: usize = 0;
#[derive(Debug, Serialize, Clone)]
pub struct RankingEntry {
    pub lang: String,
    pub rank: Option<u32>,
    pub share: f64,
    pub trend: Option<f64>,
}

#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> Result<()> {
    let use_color = should_use_color();
    colored::control::set_override(use_color);

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
        no_minify_html,
        full_output,
        no_progress,
        archive_csv,
        ..
    } = cli;

    if no_minify_html && save_html.is_none() {
        eprintln!("Warning: --no-minify-html has no effect without --save-html.");
    }
    let minify_html = !no_minify_html;

    let run_started_at = Local::now();

    let client = Client::builder()
        .user_agent("lang-rank-fetcher/0.1")
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECONDS))
        .build()
        .context("failed to build HTTP client")?;

    let progress_enabled = !no_progress && std::io::stderr().is_terminal();
    let progress = if progress_enabled {
        Some(ProgressState::new(use_color))
    } else {
        None
    };

    let (tiobe, mut pypl, languish, bench_bytes, techempower_scores) =
        if let Some(progress) = progress.as_ref() {
            tokio::try_join!(
                run_with_spinner(progress, Stage::Fetch, "TIOBE", fetch_tiobe(&client)),
                run_with_spinner(progress, Stage::Fetch, "PYPL", fetch_pypl(&client)),
                run_with_spinner(progress, Stage::Fetch, "Languish", fetch_languish(&client)),
                run_with_spinner(
                    progress,
                    Stage::Fetch,
                    "Benchmarks",
                    download_benchmark_data(&client)
                ),
                run_with_spinner(
                    progress,
                    Stage::Fetch,
                    "TechEmpower",
                    fetch_techempower(&client)
                )
            )?
        } else {
            tokio::try_join!(
                fetch_tiobe(&client),
                fetch_pypl(&client),
                fetch_languish(&client),
                download_benchmark_data(&client),
                fetch_techempower(&client)
            )?
        };

    let pypl_original_len = pypl.len();
    adjust_pypl_entries(&tiobe, &mut pypl);

    ensure_min_entries("TIOBE", tiobe.len(), MIN_SOURCE_ENTRIES)?;
    ensure_min_entries("PYPL", pypl_original_len, MIN_SOURCE_ENTRIES)?;
    ensure_min_entries("Languish", languish.len(), MIN_SOURCE_ENTRIES)?;

    let rankings_output = if let Some(path) = save_rankings.as_ref() {
        Some(
            save_rankings_csv(
                path.as_path(),
                &[
                    (SourceKind::Tiobe, &tiobe),
                    (SourceKind::Pypl, &pypl),
                    (SourceKind::Languish, &languish),
                ],
                archive_csv,
            )
            .await?,
        )
    } else {
        None
    };

    let benchmarks_output = if let Some(path) = save_benchmarks.as_ref() {
        Some(save_benchmarks_csv(&bench_bytes, path.as_path(), archive_csv).await?)
    } else {
        None
    };

    let benchmark_scores = if let Some(progress) = progress.as_ref() {
        run_with_spinner(
            progress,
            Stage::Compute,
            "Compute benchmarks",
            load_benchmark_scores(bench_bytes),
        )
        .await?
    } else {
        load_benchmark_scores(bench_bytes).await?
    };
    ensure_min_entries(
        "Benchmarks Game",
        benchmark_scores.len(),
        MIN_BENCHMARK_LANGUAGES,
    )?;
    ensure_min_entries(
        "TechEmpower",
        techempower_scores.len(),
        MIN_TECHEMPOWER_LANGUAGES,
    )?;
    let benchmark_lang_count = benchmark_scores.len();
    let techempower_lang_count = techempower_scores.len();
    let schulze_records = compute_schulze_records(
        &tiobe,
        &pypl,
        &languish,
        &benchmark_scores,
        &techempower_scores,
        SchulzeConfig {
            min_source_overlap: MIN_SOURCE_OVERLAP,
            max_ranked_languages: MAX_RANKED_LANGUAGES,
            techempower_max_score: TECHEMPOWER_MAX_SCORE,
        },
    )?;
    let schulze_output = if let Some(path) = save_schulze.as_ref() {
        Some(save_schulze_csv(&schulze_records, path.as_path(), archive_csv).await?)
    } else {
        None
    };

    if let Some(path) = save_html.as_ref() {
        let html_context = HtmlReportContext {
            tiobe_count: tiobe.len(),
            pypl_count: pypl_original_len,
            languish_count: languish.len(),
            benchmark_lang_count,
            techempower_lang_count,
            run_started_at: &run_started_at,
            schulze_records: &schulze_records,
            full_output,
            archive_csv,
            paths: HtmlReportPaths {
                benchmarks: benchmarks_output.as_deref(),
                rankings: rankings_output.as_deref(),
                schulze: schulze_output.as_deref(),
            },
            output_path: path.as_path(),
        };
        save_html_report(path.as_path(), &html_context, minify_html).await?;
    }

    if let Some(progress) = progress.as_ref() {
        progress.clear();
    }

    print_summary(&SummaryContext {
        tiobe_count: tiobe.len(),
        pypl_count: pypl_original_len,
        languish_count: languish.len(),
        benchmark_lang_count,
        techempower_lang_count,
        run_started_at: &run_started_at,
        paths: SummaryPaths {
            benchmarks: benchmarks_output.as_deref(),
            rankings: rankings_output.as_deref(),
            schulze: schulze_output.as_deref(),
            html: save_html.as_deref(),
        },
        schulze_records: &schulze_records,
        full_output,
    });

    Ok(())
}

async fn save_benchmarks_csv(bytes: &[u8], path: &Path, archive: bool) -> Result<PathBuf> {
    write_csv_output(path, bytes, archive).await
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

async fn write_csv_output(path: &Path, bytes: &[u8], archive: bool) -> Result<PathBuf> {
    if archive {
        let output_path = archive_output_path(path);
        let gzipped = gzip_bytes(bytes)?;
        write_output_file(&output_path, &gzipped).await?;
        Ok(output_path)
    } else {
        write_output_file(path, bytes).await?;
        Ok(path.to_path_buf())
    }
}

fn archive_output_path(path: &Path) -> PathBuf {
    if path
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz"))
    {
        return path.to_path_buf();
    }
    let mut name = path.as_os_str().to_os_string();
    name.push(".gz");
    PathBuf::from(name)
}

fn gzip_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(bytes)
        .context("failed to write gzip data")?;
    encoder.finish().context("failed to finalize gzip data")
}

fn finalize_writer(mut writer: Writer<Vec<u8>>, label: &str) -> Result<Vec<u8>> {
    writer
        .flush()
        .with_context(|| format!("failed to flush {label}"))?;
    writer
        .into_inner()
        .with_context(|| format!("failed to finalize {label}"))
}

fn should_use_color() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    std::io::stdout().is_terminal()
}

fn ensure_min_entries(label: &str, count: usize, min: usize) -> Result<()> {
    if count < min {
        return Err(anyhow!(
            "{label} returned {count} entries (expected at least {min}); the source format may have changed."
        ));
    }
    Ok(())
}

async fn save_rankings_csv(
    path: &Path,
    sources: &[(SourceKind, &[RankingEntry])],
    archive: bool,
) -> Result<PathBuf> {
    let serialized = serialize_rankings(sources)?;
    write_csv_output(path, &serialized, archive).await
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

async fn save_schulze_csv(
    records: &[SchulzeRecord],
    output_path: &Path,
    archive: bool,
) -> Result<PathBuf> {
    let mut writer = Writer::from_writer(Vec::new());
    for record in records {
        writer
            .serialize(record)
            .context("failed to serialize Schulze ranking record")?;
    }
    let serialized = finalize_writer(writer, "Schulze ranking writer")?;
    write_csv_output(output_path, &serialized, archive).await
}

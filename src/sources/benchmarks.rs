use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use rustc_hash::FxHashMap;
use std::io::Cursor;
use tokio::task;

use super::{canonicalize_language, fetch_bytes_with_retry};

const BENCH_URL: &str = "https://salsa.debian.org/benchmarksgame-team/benchmarksgame/-/raw/master/public/data/alldata.csv";

pub async fn download_benchmark_data(client: &Client) -> Result<Vec<u8>> {
    fetch_bytes_with_retry(client, BENCH_URL)
        .await
        .context("failed to download benchmark dataset")
}

pub async fn load_benchmark_scores(bytes: Vec<u8>) -> Result<FxHashMap<String, f64>> {
    let scores = task::spawn_blocking(move || compute_benchmark_scores_sync(&bytes))
        .await
        .context("failed to read benchmark statistics")??;
    Ok(scores)
}

fn compute_benchmark_scores_sync(data: &[u8]) -> Result<FxHashMap<String, f64>> {
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

    let mut best_per_lang_task: FxHashMap<(String, String), f64> = FxHashMap::default();
    let mut best_per_task: FxHashMap<String, f64> = FxHashMap::default();

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

        let Some(canonical) = canonicalize_language(lang_raw) else {
            continue;
        };
        let task = name.to_string();
        let key = (canonical.clone(), task.clone());
        let entry = best_per_lang_task.entry(key).or_insert(f64::INFINITY);
        if elapsed < *entry {
            *entry = elapsed;
        }

        let best_entry = best_per_task.entry(task).or_insert(f64::INFINITY);
        if elapsed < *best_entry {
            *best_entry = elapsed;
        }
    }

    let mut ratios_by_lang: FxHashMap<String, Vec<f64>> = FxHashMap::default();
    for ((lang, task), elapsed) in best_per_lang_task {
        let Some(best) = best_per_task.get(task.as_str()) else {
            continue;
        };
        if !best.is_finite() || *best <= 0.0 || !elapsed.is_finite() || elapsed <= 0.0 {
            continue;
        }
        let ratio = *best / elapsed;
        if ratio.is_finite() && ratio > 0.0 {
            ratios_by_lang.entry(lang).or_default().push(ratio);
        }
    }

    let mut scores: FxHashMap<String, f64> = FxHashMap::default();
    for (lang, ratios) in ratios_by_lang {
        let count = ratios.len();
        if count == 0 {
            continue;
        }
        let sum_ln: f64 = ratios.iter().map(|ratio| ratio.ln()).sum();
        let score = (sum_ln / (count as f64)).exp();
        if score.is_finite() {
            scores.insert(lang, score);
        }
    }

    if let Some(value) = scores.get("C/C++").copied() {
        scores.insert("C".to_string(), value);
        scores.insert("C++".to_string(), value);
    }

    Ok(scores)
}

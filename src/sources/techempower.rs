use anyhow::{Context, Result, anyhow};
use reqwest::{Client, Url};
use rustc_hash::FxHashMap;
use scraper::{Html, Selector};
use serde::Deserialize;
use serde_json::Value;

use super::{canonicalize_language, fetch_bytes_with_retry, fetch_text_with_retry};

const TFB_STATUS_URL: &str = "https://tfb-status.techempower.com";
const TEST_WEIGHTS: [f64; 6] = [1.0, 0.75, 0.75, 0.75, 1.5, 1.25];
const TEST_NAMES: [&str; 6] = ["json", "plaintext", "db", "query", "fortune", "update"];
pub const TECHEMPOWER_MAX_SCORE: f64 = 6.0;

#[derive(Debug, Clone, Copy, Default)]
struct FrameworkScores {
    rps: [f64; 6],
    present: [bool; 6],
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TechempowerResults {
    raw_data: FxHashMap<String, Value>,
    test_metadata: Vec<TfbTestMetadata>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct TfbTestMetadata {
    framework: String,
    language: String,
}

pub async fn fetch_techempower(client: &Client) -> Result<FxHashMap<String, f64>> {
    let run_id = latest_completed_run_id(client).await?;
    let results_url = results_url_for_run(client, &run_id).await?;
    let bytes = fetch_bytes_with_retry(client, &results_url)
        .await
        .with_context(|| format!("failed to download TechEmpower results from {results_url}"))?;
    let results: TechempowerResults =
        serde_json::from_slice(&bytes).context("failed to parse TechEmpower results JSON")?;
    let scores = compute_language_scores(&results)?;
    Ok(scores)
}

async fn latest_completed_run_id(client: &Client) -> Result<String> {
    let status_html = fetch_text_with_retry(client, TFB_STATUS_URL)
        .await
        .context("failed to fetch TechEmpower status page")?;
    let document = Html::parse_document(&status_html);
    let row_selector = Selector::parse("tr[data-uuid]")
        .map_err(|_| anyhow!("invalid selector for TechEmpower status rows"))?;

    for row in document.select(&row_selector) {
        let run_id = row.value().attr("data-uuid").unwrap_or("").trim();
        if run_id.is_empty() {
            continue;
        }
        let row_text = row.text().collect::<Vec<_>>().join(" ").to_lowercase();
        if is_completed_status(&row_text) {
            return Ok(run_id.to_string());
        }
    }

    Err(anyhow!("no completed TechEmpower runs found"))
}

async fn results_url_for_run(client: &Client, run_id: &str) -> Result<String> {
    let details_url = format!("{TFB_STATUS_URL}/results/{run_id}");
    let details_html = fetch_text_with_retry(client, &details_url)
        .await
        .with_context(|| format!("failed to fetch TechEmpower run page {details_url}"))?;
    let document = Html::parse_document(&details_html);
    let link_selector =
        Selector::parse("a").map_err(|_| anyhow!("invalid selector for TechEmpower run links"))?;
    for link in document.select(&link_selector) {
        let text = link.text().collect::<Vec<_>>().join(" ");
        if text.trim() == "results.json" {
            let href = link
                .value()
                .attr("href")
                .ok_or_else(|| anyhow!("missing href for results.json link"))?;
            return Ok(resolve_techempower_url(href));
        }
    }

    Err(anyhow!("results.json link not found for run {run_id}"))
}

fn resolve_techempower_url(href: &str) -> String {
    if href.starts_with("http://") || href.starts_with("https://") {
        return href.to_string();
    }
    if let Ok(base) = Url::parse(TFB_STATUS_URL)
        && let Ok(joined) = base.join(href)
    {
        return joined.to_string();
    }
    if href.starts_with('/') {
        format!("{TFB_STATUS_URL}{href}")
    } else {
        format!("{TFB_STATUS_URL}/{href}")
    }
}

fn is_completed_status(row_text: &str) -> bool {
    let tokens: Vec<&str> = row_text
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect();
    for (idx, token) in tokens.iter().enumerate() {
        if *token == "completed" {
            if idx > 0 && tokens[idx - 1] == "not" {
                continue;
            }
            return true;
        }
    }
    false
}

fn compute_language_scores(results: &TechempowerResults) -> Result<FxHashMap<String, f64>> {
    let framework_languages = map_framework_languages(&results.test_metadata);

    let mut per_framework: FxHashMap<String, FrameworkScores> = FxHashMap::default();
    let mut max_rps_by_test = [0.0_f64; 6];

    for (test_name, frameworks) in &results.raw_data {
        let Some(test_idx) = test_index(test_name.as_str()) else {
            continue;
        };
        let Some(frameworks_obj) = frameworks.as_object() else {
            continue;
        };
        for (framework, runs_value) in frameworks_obj {
            let Some(runs) = runs_value.as_array() else {
                continue;
            };
            let max_rps = runs
                .iter()
                .filter_map(calculate_rps_value)
                .fold(0.0_f64, f64::max);
            if max_rps <= 0.0 {
                continue;
            }
            let entry = per_framework.entry(framework.clone()).or_default();
            entry.rps[test_idx] = max_rps;
            entry.present[test_idx] = true;
            if max_rps > max_rps_by_test[test_idx] {
                max_rps_by_test[test_idx] = max_rps;
            }
        }
    }

    for (idx, max_rps) in max_rps_by_test.iter().enumerate() {
        if *max_rps <= 0.0 {
            return Err(anyhow!(
                "missing TechEmpower data for test '{}'",
                TEST_NAMES[idx]
            ));
        }
    }

    let mut best_by_lang: FxHashMap<String, f64> = FxHashMap::default();
    for (framework, scores) in per_framework {
        if !scores.present.iter().all(|present| *present) {
            continue;
        }
        let mut composite = 0.0_f64;
        for idx in 0..TEST_WEIGHTS.len() {
            let max_rps = max_rps_by_test[idx];
            if max_rps <= 0.0 {
                continue;
            }
            composite += (scores.rps[idx] / max_rps) * TEST_WEIGHTS[idx];
        }
        if composite <= 0.0 {
            continue;
        }
        let Some(lang) = framework_languages.get(&framework) else {
            continue;
        };
        let entry = best_by_lang.entry(lang.clone()).or_insert(0.0);
        if composite > *entry {
            *entry = composite;
        }
    }

    if best_by_lang.is_empty() {
        return Err(anyhow!("no TechEmpower language scores computed"));
    }

    Ok(best_by_lang)
}

fn map_framework_languages(metadata: &[TfbTestMetadata]) -> FxHashMap<String, String> {
    let mut map: FxHashMap<String, String> = FxHashMap::default();
    for entry in metadata {
        let Some(lang) = canonicalize_language(entry.language.as_str()) else {
            continue;
        };
        map.entry(entry.framework.clone()).or_insert(lang);
    }
    map
}

fn test_index(name: &str) -> Option<usize> {
    match name {
        "json" => Some(0),
        "plaintext" => Some(1),
        "db" => Some(2),
        "query" => Some(3),
        "fortune" => Some(4),
        "update" => Some(5),
        _ => None,
    }
}

fn calculate_rps_value(run: &Value) -> Option<f64> {
    let total = run.get("totalRequests")?.as_f64()?;
    let start = run.get("startTime")?.as_f64()?;
    let end = run.get("endTime")?.as_f64()?;
    let duration_ms = end - start;
    if total <= 0.0 || duration_ms <= 0.0 {
        return None;
    }
    Some(total / (duration_ms / 1000.0))
}

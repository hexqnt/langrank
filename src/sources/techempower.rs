use anyhow::{Context, Result, anyhow};
use reqwest::{Client, Url};
use rustc_hash::{FxHashMap, FxHashSet};
use scraper::{Html, Selector};
use serde::Deserialize;
use serde_json::Value;
use std::path::Path;

use super::{CanonicalLanguage, fetch_bytes_with_retry, fetch_text_with_retry};

const TFB_BENCHMARKS_URL: &str = "https://www.techempower.com/benchmarks/";
const TEST_WEIGHTS: [f64; 6] = [1.0, 0.75, 0.75, 0.75, 1.5, 1.25];
const TEST_NAMES: [&str; 6] = ["json", "plaintext", "db", "query", "fortune", "update"];
pub const TECHEMPOWER_MAX_SCORE: f64 = 6.0;
const MAX_FALLBACK_RESULTS_URLS: usize = 8;
const MIN_SUPPORTED_ROUND: u16 = 21;
const STATIC_FALLBACK_RESULTS_URLS: [&str; 3] = [
    "https://www.techempower.com/benchmarks/results/round23/ph.json",
    "https://www.techempower.com/benchmarks/results/round22/ph.json",
    "https://www.techempower.com/benchmarks/results/round21/ph.json",
];

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
    let fallback_urls = fallback_results_urls(client).await;
    let mut errors: Vec<String> = Vec::new();

    for results_url in fallback_urls {
        match fetch_techempower_for_results_url(client, &results_url).await {
            Ok(scores) => return Ok(scores),
            Err(err) => errors.push(format!("{results_url}: {err:#}")),
        }
    }

    let summary = if errors.is_empty() {
        "no fallback URLs were available".to_string()
    } else {
        errors.join(" | ")
    };
    Err(anyhow!(
        "failed to fetch TechEmpower data from benchmarks results sources; errors: {summary}"
    ))
}

async fn fetch_techempower_for_results_url(
    client: &Client,
    results_url: &str,
) -> Result<FxHashMap<String, f64>> {
    let bytes = fetch_bytes_with_retry(client, results_url)
        .await
        .with_context(|| format!("failed to download TechEmpower results from {results_url}"))?;
    let results: TechempowerResults =
        serde_json::from_slice(&bytes).context("failed to parse TechEmpower results JSON")?;
    let scores = compute_language_scores(&results)?;
    Ok(scores)
}

fn resolve_url(base_url: &str, href: &str) -> String {
    if href.starts_with("http://") || href.starts_with("https://") {
        return href.to_string();
    }
    if let Ok(base) = Url::parse(base_url)
        && let Ok(joined) = base.join(href)
    {
        return joined.to_string();
    }
    if href.starts_with('/') {
        format!("{base_url}{href}")
    } else {
        format!("{base_url}/{href}")
    }
}

async fn fallback_results_urls(client: &Client) -> Vec<String> {
    let mut urls = discover_fallback_results_urls(client)
        .await
        .unwrap_or_default();

    for url in STATIC_FALLBACK_RESULTS_URLS {
        urls.push(url.to_string());
    }

    urls = dedup_urls_preserve_order(urls);
    urls.truncate(MAX_FALLBACK_RESULTS_URLS);
    urls
}

async fn discover_fallback_results_urls(client: &Client) -> Result<Vec<String>> {
    let html = fetch_text_with_retry(client, TFB_BENCHMARKS_URL)
        .await
        .context("failed to fetch TechEmpower benchmarks page for fallback discovery")?;
    let bundle_url = benchmarks_bundle_url(&html)
        .ok_or_else(|| anyhow!("unable to locate benchmarks JS bundle for fallback discovery"))?;
    let bundle = fetch_text_with_retry(client, &bundle_url)
        .await
        .with_context(|| format!("failed to fetch TechEmpower benchmarks bundle {bundle_url}"))?;
    let urls = extract_round_results_urls(&bundle);
    if urls.is_empty() {
        return Err(anyhow!(
            "no fallback URLs found in TechEmpower benchmarks bundle"
        ));
    }
    Ok(urls)
}

fn benchmarks_bundle_url(html: &str) -> Option<String> {
    let document = Html::parse_document(html);
    let script_selector = Selector::parse("script[src]").ok()?;
    for script in document.select(&script_selector) {
        let src = script.value().attr("src")?;
        if src.contains("assets/index-")
            && Path::new(src)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("js"))
        {
            return Some(resolve_url(TFB_BENCHMARKS_URL, src));
        }
    }
    None
}

fn extract_round_results_urls(bundle: &str) -> Vec<String> {
    const ROUND_MARKER: &str = "data-r";
    let mut rounds: Vec<u16> = Vec::new();
    let mut rest = bundle;

    while let Some(start) = rest.find(ROUND_MARKER) {
        rest = &rest[start + ROUND_MARKER.len()..];
        let end = rest
            .find(|ch: char| !ch.is_ascii_digit())
            .unwrap_or(rest.len());
        if end == 0 {
            continue;
        }
        let digits = &rest[..end];
        if let Ok(round) = digits.parse::<u16>() {
            rounds.push(round);
        }
        rest = &rest[end..];
    }

    rounds.sort_unstable();
    rounds.dedup();
    rounds.reverse();
    rounds
        .into_iter()
        .filter(|round| *round >= MIN_SUPPORTED_ROUND)
        .map(|round| format!("{TFB_BENCHMARKS_URL}results/round{round}/ph.json"))
        .collect()
}

fn dedup_urls_preserve_order(urls: Vec<String>) -> Vec<String> {
    let mut seen: FxHashSet<String> = FxHashSet::default();
    let mut unique: Vec<String> = Vec::new();
    for url in urls {
        if seen.insert(url.clone()) {
            unique.push(url);
        }
    }
    unique
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
            composite = (scores.rps[idx] / max_rps).mul_add(TEST_WEIGHTS[idx], composite);
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
        let Some(lang) = CanonicalLanguage::parse(entry.language.as_str()) else {
            continue;
        };
        map.entry(entry.framework.clone())
            .or_insert_with(|| lang.into_string());
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

#[cfg(test)]
mod tests {
    use super::{dedup_urls_preserve_order, extract_round_results_urls};

    #[test]
    fn extracts_round_ph_urls() {
        let bundle = r#"
            const tabs = [
                { tab: "data-r18" },
                { tab: "data-r23" },
                { tab: "data-r22" },
                { tab: "data-r21" },
                { tab: "data-r23" },
            ];
        "#;
        let urls = extract_round_results_urls(bundle);
        assert_eq!(
            urls,
            vec![
                "https://www.techempower.com/benchmarks/results/round23/ph.json",
                "https://www.techempower.com/benchmarks/results/round22/ph.json",
                "https://www.techempower.com/benchmarks/results/round21/ph.json",
            ]
        );
    }

    #[test]
    fn dedups_urls_without_reordering() {
        let urls = vec![
            "https://www.techempower.com/benchmarks/results/round23/ph.json".to_string(),
            "https://www.techempower.com/benchmarks/results/round22/ph.json".to_string(),
            "https://www.techempower.com/benchmarks/results/round23/ph.json".to_string(),
        ];
        let unique = dedup_urls_preserve_order(urls);
        assert_eq!(
            unique,
            vec![
                "https://www.techempower.com/benchmarks/results/round23/ph.json",
                "https://www.techempower.com/benchmarks/results/round22/ph.json",
            ]
        );
    }
}

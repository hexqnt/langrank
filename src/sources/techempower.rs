use anyhow::{Context, Result, anyhow};
use reqwest::{Client, Url};
use rustc_hash::{FxHashMap, FxHashSet};
use scraper::{Html, Selector};
use serde::Deserialize;
use std::path::Path;

use super::{CanonicalLanguage, fetch_bytes_with_retry, fetch_text_with_retry};

const TFB_BENCHMARKS_URL: &str = "https://www.techempower.com/benchmarks/";
const MAX_FALLBACK_RESULTS_URLS: usize = 8;
const MIN_SUPPORTED_ROUND: u16 = 21;
const STATIC_FALLBACK_RESULTS_URLS: [&str; 3] = [
    "https://www.techempower.com/benchmarks/results/round23/ph.json",
    "https://www.techempower.com/benchmarks/results/round22/ph.json",
    "https://www.techempower.com/benchmarks/results/round21/ph.json",
];

#[derive(Clone, Copy)]
struct TestConfig {
    name: &'static str,
    weight: f64,
}

impl TestConfig {
    const fn new(name: &'static str, weight: f64) -> Self {
        Self { name, weight }
    }
}

const TESTS: [TestConfig; 6] = [
    TestConfig::new("json", 1.0),
    TestConfig::new("plaintext", 0.75),
    TestConfig::new("db", 0.75),
    TestConfig::new("query", 0.75),
    TestConfig::new("fortune", 1.5),
    TestConfig::new("update", 1.25),
];
const TEST_COUNT: usize = TESTS.len();

const fn total_test_weight() -> f64 {
    let mut total = 0.0;
    let mut index = 0;
    while index < TEST_COUNT {
        total += TESTS[index].weight;
        index += 1;
    }
    total
}

pub const TECHEMPOWER_MAX_SCORE: f64 = total_test_weight();

#[derive(Debug, Clone, Copy, Default)]
struct FrameworkThroughput {
    rps: [f64; TEST_COUNT],
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TechEmpowerResults<'a> {
    #[serde(borrow)]
    raw_data: BenchmarkData<'a>,
    #[serde(borrow)]
    test_metadata: Vec<FrameworkMetadata<'a>>,
}

type RunsByFramework<'a> = FxHashMap<&'a str, Vec<BenchmarkRun>>;

#[derive(Debug, Deserialize, Default)]
struct BenchmarkData<'a> {
    #[serde(borrow, default)]
    json: RunsByFramework<'a>,
    #[serde(borrow, default)]
    plaintext: RunsByFramework<'a>,
    #[serde(borrow, default)]
    db: RunsByFramework<'a>,
    #[serde(borrow, default)]
    query: RunsByFramework<'a>,
    #[serde(borrow, default)]
    fortune: RunsByFramework<'a>,
    #[serde(borrow, default)]
    update: RunsByFramework<'a>,
}

impl<'a> BenchmarkData<'a> {
    const fn tests(&self) -> [&RunsByFramework<'a>; TEST_COUNT] {
        [
            &self.json,
            &self.plaintext,
            &self.db,
            &self.query,
            &self.fortune,
            &self.update,
        ]
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkRun {
    #[serde(default)]
    total_requests: f64,
    #[serde(default)]
    start_time: f64,
    #[serde(default)]
    end_time: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct FrameworkMetadata<'a> {
    framework: &'a str,
    language: &'a str,
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
    parse_techempower_results(&bytes)
}

fn parse_techempower_results(bytes: &[u8]) -> Result<FxHashMap<String, f64>> {
    let results: TechEmpowerResults<'_> =
        serde_json::from_slice(bytes).context("failed to parse TechEmpower results JSON")?;
    compute_language_scores(&results)
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

fn compute_language_scores(results: &TechEmpowerResults<'_>) -> Result<FxHashMap<String, f64>> {
    let framework_languages = map_framework_languages(&results.test_metadata);

    let mut throughput_by_framework: FxHashMap<&str, FrameworkThroughput> = FxHashMap::default();
    let mut max_rps_by_test = [0.0_f64; TEST_COUNT];

    for (test_idx, frameworks) in results.raw_data.tests().into_iter().enumerate() {
        for (framework, runs) in frameworks {
            let max_rps = runs
                .iter()
                .filter_map(BenchmarkRun::requests_per_second)
                .fold(0.0_f64, f64::max);
            if max_rps <= 0.0 {
                continue;
            }
            let entry = throughput_by_framework.entry(framework).or_default();
            entry.rps[test_idx] = max_rps;
            if max_rps > max_rps_by_test[test_idx] {
                max_rps_by_test[test_idx] = max_rps;
            }
        }
    }

    for (test, max_rps) in TESTS.iter().zip(max_rps_by_test) {
        if max_rps <= 0.0 {
            return Err(anyhow!("missing TechEmpower data for test '{}'", test.name));
        }
    }

    let mut best_by_language: FxHashMap<String, f64> = FxHashMap::default();
    for (framework, throughput) in throughput_by_framework {
        if throughput.rps.iter().any(|rps| *rps <= 0.0) {
            continue;
        }
        let mut composite = 0.0_f64;
        for ((rps, max_rps), test) in throughput.rps.iter().zip(max_rps_by_test).zip(TESTS) {
            composite = (*rps / max_rps).mul_add(test.weight, composite);
        }
        if composite <= 0.0 {
            continue;
        }
        let Some(language) = framework_languages.get(&framework) else {
            continue;
        };
        let entry = best_by_language.entry(language.clone()).or_insert(0.0);
        if composite > *entry {
            *entry = composite;
        }
    }

    if best_by_language.is_empty() {
        return Err(anyhow!("no TechEmpower language scores computed"));
    }

    Ok(best_by_language)
}

fn map_framework_languages<'a>(metadata: &[FrameworkMetadata<'a>]) -> FxHashMap<&'a str, String> {
    let mut map = FxHashMap::default();
    for entry in metadata {
        let Some(language) = CanonicalLanguage::parse(entry.language) else {
            continue;
        };
        map.entry(entry.framework)
            .or_insert_with(|| language.into_string());
    }
    map
}

impl BenchmarkRun {
    fn requests_per_second(&self) -> Option<f64> {
        let duration = self.end_time - self.start_time;
        if self.total_requests <= 0.0 || duration <= 0.0 {
            return None;
        }
        Some(self.total_requests / (duration / 1000.0))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TECHEMPOWER_MAX_SCORE, TechEmpowerResults, compute_language_scores,
        dedup_urls_preserve_order, extract_round_results_urls,
    };

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

    #[test]
    fn parses_only_supported_tests_and_skips_incomplete_runs() {
        let run =
            |requests| format!(r#"{{"totalRequests":{requests},"startTime":1000,"endTime":2000}}"#);
        let tests = ["json", "plaintext", "db", "query", "fortune", "update"]
            .map(|name| {
                format!(
                    r#""{name}":{{"fast":[{{}},{fast}],"slow":[{slow}]}}"#,
                    fast = run(2000),
                    slow = run(1000),
                )
            })
            .join(",");
        let json = format!(
            r#"{{
                "rawData":{{{tests},"cached-query":"ignored"}},
                "testMetadata":[
                    {{"framework":"fast","language":"rust"}},
                    {{"framework":"slow","language":"java"}}
                ]
            }}"#
        );

        let results: TechEmpowerResults<'_> =
            serde_json::from_str(&json).expect("fixture should parse");
        let scores = compute_language_scores(&results).expect("scores should compute");

        assert_eq!(scores.get("Rust"), Some(&TECHEMPOWER_MAX_SCORE));
        assert_eq!(scores.get("Java"), Some(&(TECHEMPOWER_MAX_SCORE / 2.0)));
    }
}

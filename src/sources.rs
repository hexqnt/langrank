pub mod benchmarks;
pub mod languish;
pub mod pypl;
pub mod tiobe;

pub use benchmarks::{download_benchmark_data, load_benchmark_stats};
pub use languish::fetch_languish;
pub use pypl::fetch_pypl;
pub use tiobe::fetch_tiobe;

use crate::RankingEntry;
use anyhow::{Context, Result, anyhow};
use reqwest::{Client, Response};
use rustc_hash::FxHashMap;
use scraper::ElementRef;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::time::sleep;

const MAX_RETRIES: usize = 3;

#[derive(Debug)]
pub struct RawEntry {
    pub lang: String,
    pub rank: Option<u32>,
    pub share: f64,
    pub trend: Option<f64>,
}

#[derive(Default)]
struct AggregatedEntry {
    min_rank: Option<u32>,
    share_sum: f64,
    trend_sum: f64,
    trend_seen: bool,
}

pub async fn fetch_text_with_retry(client: &Client, url: &str) -> Result<String> {
    send_with_retry(client, url)
        .await?
        .text()
        .await
        .with_context(|| format!("failed to read response body from {url}"))
}

pub async fn fetch_bytes_with_retry(client: &Client, url: &str) -> Result<Vec<u8>> {
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

pub fn aggregate_entries(entries: Vec<RawEntry>) -> Vec<RankingEntry> {
    let alias_map = language_aliases();
    let mut aggregated: FxHashMap<String, AggregatedEntry> = FxHashMap::default();

    for entry in entries {
        let trimmed = entry.lang.trim();
        if trimmed.is_empty() {
            continue;
        }
        let lookup_key = normalize_alias_key(trimmed);
        let normalized = alias_map
            .get(lookup_key.as_str())
            .copied()
            .unwrap_or(trimmed);
        if normalized.is_empty() {
            continue;
        }
        let agg = aggregated.entry(normalized.to_owned()).or_default();
        agg.share_sum += entry.share;
        if let Some(rank) = entry.rank {
            agg.min_rank = Some(agg.min_rank.map_or(rank, |existing| existing.min(rank)));
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

pub fn extract_cell_text(cell: ElementRef<'_>) -> String {
    cell.text()
        .map(str::trim)
        .filter(|chunk| !chunk.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn parse_u32(value: &str) -> Option<u32> {
    value
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>()
        .parse::<u32>()
        .ok()
}

pub fn parse_percent(value: &str) -> Option<f64> {
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

fn normalize_alias_key(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_whitespace() {
            continue;
        }
        out.extend(ch.to_lowercase());
    }
    out
}

fn language_aliases() -> &'static FxHashMap<&'static str, &'static str> {
    static LANGUAGE_ALIASES: OnceLock<FxHashMap<&'static str, &'static str>> = OnceLock::new();
    LANGUAGE_ALIASES.get_or_init(|| {
        [
            ("delphi/objectpascal", "Delphi/Pascal"),
            ("matlab", "Matlab"),
            ("cobol", "COBOL"),
            ("powershell", "PowerShell"),
            ("vbscript", "VBA/VBS"),
            ("vba", "VBA/VBS"),
            ("abap", "Abap"),
            ("(visual)foxpro", "FoxPro"),
            ("c#", "C#"),
            ("csharp", "C#"),
            ("c-sharp", "C#"),
            ("f#", "F#"),
            ("fsharp", "F#"),
            ("f-sharp", "F#"),
            ("javascript", "JavaScript"),
            ("js", "JavaScript"),
            ("node", "JavaScript"),
            ("node.js", "JavaScript"),
            ("nodejs", "JavaScript"),
            ("typescript", "TypeScript"),
            ("ts", "TypeScript"),
            ("objective-c", "Objective-C"),
            ("objectivec", "Objective-C"),
            ("obj-c", "Objective-C"),
            ("objc", "Objective-C"),
            ("golang", "Go"),
            ("go", "Go"),
            ("vb", "Visual Basic"),
            ("vb.net", "Visual Basic"),
            ("vbnet", "Visual Basic"),
            ("visualbasic", "Visual Basic"),
            ("visualbasic.net", "Visual Basic"),
        ]
        .into_iter()
        .collect()
    })
}

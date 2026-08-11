use crate::RankingEntry;
use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use rustc_hash::FxHashMap;
use scraper::{Html, Selector};
use serde::Deserialize;
use serde_json::Value;
use std::simd::{Simd, cmp::SimdPartialEq};
use std::sync::OnceLock;

use super::{RawEntry, aggregate_entries, fetch_text_with_retry};

const LANGUISH_INDEX_URL: &str = "https://tjpalmer.github.io/languish/";
const BACKSLASH_SCAN_LANES: usize = 32;

#[derive(Debug, Deserialize)]
struct Table {
    keys: Vec<String>,
    rows: Vec<Vec<Value>>,
}

#[derive(Debug, Deserialize)]
struct LanguishData {
    items: Table,
    sums: Table,
}

#[derive(Clone, Copy)]
struct CoreWeights {
    issues: f64,
    pulls: f64,
    so_questions: f64,
    stars: f64,
}

// В отличие от Languish учитываем все четыре сигнала с одинаковым весом.
const CORE_WEIGHTS: CoreWeights = CoreWeights {
    issues: 1.0,
    pulls: 1.0,
    so_questions: 1.0,
    stars: 1.0,
};

impl CoreWeights {
    const fn total(self) -> f64 {
        self.issues + self.pulls + self.so_questions + self.stars
    }
}

pub async fn fetch_languish(client: &Client) -> Result<Vec<RankingEntry>> {
    let index_html = fetch_text_with_retry(client, LANGUISH_INDEX_URL)
        .await
        .context("failed to download Languish index page")?;
    let main_js_url = extract_main_js_url(&index_html)
        .ok_or_else(|| anyhow!("failed to locate Languish main chunk script"))?;

    let js_body = fetch_text_with_retry(client, &main_js_url)
        .await
        .with_context(|| format!("failed to download Languish JS bundle: {main_js_url}"))?;
    parse_languish_bundle(&js_body)
}

fn parse_languish_bundle(js_body: &str) -> Result<Vec<RankingEntry>> {
    let encoded = extract_json_parse_payload(js_body)
        .ok_or_else(|| anyhow!("failed to extract Languish embedded JSON payload"))?;

    let json_text = decode_js_string_literal(encoded);
    let tables = parse_languish_tables(&json_text)?;
    let quarters = recent_quarters(&tables.sums)?;
    let metrics_by_language = build_recent_metrics(
        &tables.items,
        quarters.latest.date,
        quarters.previous.map(|quarter| quarter.date),
    )?;
    let weights = CORE_WEIGHTS;
    let weight_total = weights.total();

    let mut ranked_languages = Vec::with_capacity(metrics_by_language.len());
    for (name, metrics) in metrics_by_language {
        let latest_mean = metrics.latest.as_ref().map_or(0.0, |metrics| {
            mean_percent(metrics, &quarters.latest.metrics, weights, weight_total)
        });
        let previous_mean = quarters.previous.map(|quarter| {
            metrics.previous.as_ref().map_or(0.0, |metrics| {
                mean_percent(metrics, &quarter.metrics, weights, weight_total)
            })
        });
        let trend = previous_mean.map(|previous| latest_mean - previous);
        ranked_languages.push((name.to_owned(), latest_mean, trend));
    }

    ranked_languages.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let mut entries: Vec<RawEntry> = Vec::with_capacity(ranked_languages.len());
    for (index, (name, mean, trend)) in ranked_languages.into_iter().enumerate() {
        let rank = u32::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1));
        if rank.is_none() {
            eprintln!("Warning: Languish rank overflow at index {index}; omitting rank for {name}");
        }
        if let Some(entry) = RawEntry::parse(name.as_str(), rank, mean, trend) {
            entries.push(entry);
        }
    }

    Ok(aggregate_entries(entries))
}

fn extract_main_js_url(index_html: &str) -> Option<String> {
    let doc = Html::parse_document(index_html);
    for node in doc.select(script_selector()) {
        if let Some(src) = node.value().attr("src")
            && src.contains("/static/js/main")
            && src.ends_with(".chunk.js")
        {
            // Относительный путь в HTML задан от корня GitHub Pages.
            let url = if src.starts_with("http://") || src.starts_with("https://") {
                src.to_string()
            } else {
                format!("https://tjpalmer.github.io{src}")
            };
            return Some(url);
        }
    }
    None
}

fn script_selector() -> &'static Selector {
    static SELECTOR: OnceLock<Selector> = OnceLock::new();
    SELECTOR
        .get_or_init(|| Selector::parse("script[src]").expect("Languish script selector is valid"))
}

fn extract_json_parse_payload(js: &str) -> Option<&str> {
    // Апостроф завершает строку, только если он не экранирован и за ним идёт `)`.
    let needle = "JSON.parse('";
    let start = js.find(needle)? + needle.len();
    let bytes = js.as_bytes();
    let mut i = start;
    let mut escaped = false;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '\'' && js[i + 1..].starts_with(')') {
            return Some(&js[start..i]);
        }
        i += 1;
    }
    None
}

fn decode_js_string_literal(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut decoded = String::with_capacity(input.len());
    let mut copied_until = 0;
    let mut i = 0;

    while let Some(offset) = find_backslash(&bytes[i..]) {
        i += offset;
        let run_start = i;
        while bytes.get(i) == Some(&b'\\') {
            i += 1;
        }
        let slash_count = i - run_start;
        if slash_count.is_multiple_of(2) {
            continue;
        }

        let escape_start = i - 1;
        if bytes.get(i) == Some(&b'\'') {
            decoded.push_str(&input[copied_until..escape_start]);
            decoded.push('\'');
            i += 1;
            copied_until = i;
            continue;
        }

        if i + 2 < bytes.len()
            && bytes[i] == b'x'
            && bytes[i + 1].is_ascii_hexdigit()
            && bytes[i + 2].is_ascii_hexdigit()
        {
            decoded.push_str(&input[copied_until..escape_start]);
            let value = (hex_value(bytes[i + 1]) << 4) | hex_value(bytes[i + 2]);
            decoded.push(char::from(value));
            i += 3;
            copied_until = i;
        }
    }

    decoded.push_str(&input[copied_until..]);
    decoded
}

fn find_backslash(bytes: &[u8]) -> Option<usize> {
    let (chunks, remainder) = bytes.as_chunks::<BACKSLASH_SCAN_LANES>();
    let backslashes = Simd::<u8, BACKSLASH_SCAN_LANES>::splat(b'\\');

    for (chunk_index, &chunk) in chunks.iter().enumerate() {
        let matches = Simd::from_array(chunk).simd_eq(backslashes).to_bitmask();
        if matches != 0 {
            let lane = usize::try_from(matches.trailing_zeros())
                .expect("SIMD backslash lane index fits usize");
            return Some(chunk_index * BACKSLASH_SCAN_LANES + lane);
        }
    }

    let remainder_start = bytes.len() - remainder.len();
    remainder
        .iter()
        .position(|&byte| byte == b'\\')
        .map(|offset| remainder_start + offset)
}

const fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => 10 + byte - b'a',
        b'A'..=b'F' => 10 + byte - b'A',
        _ => 0,
    }
}

#[derive(Default, Clone, Copy)]
struct Metrics {
    issues: f64,
    pulls: f64,
    so_questions: f64,
    stars: f64,
}

#[derive(Clone, Copy)]
struct MetricColumns {
    date: usize,
    issues: usize,
    pulls: usize,
    so_questions: usize,
    stars: usize,
}

#[derive(Clone, Copy)]
struct ItemColumns {
    name: usize,
    metrics: MetricColumns,
}

#[derive(Clone, Copy)]
struct MetricsRow<'a> {
    date: &'a str,
    metrics: Metrics,
}

#[derive(Clone, Copy)]
struct ItemRow<'a> {
    name: &'a str,
    date: &'a str,
    metrics: Metrics,
}

#[derive(Clone, Copy)]
struct QuarterSnapshot<'a> {
    date: &'a str,
    metrics: Metrics,
}

struct RecentQuarters<'a> {
    latest: QuarterSnapshot<'a>,
    previous: Option<QuarterSnapshot<'a>>,
}

fn parse_languish_tables(js: &str) -> Result<LanguishData> {
    serde_json::from_str(js).context("failed to parse decoded Languish JSON object")
}

fn recent_quarters(sums: &Table) -> Result<RecentQuarters<'_>> {
    let columns = metric_columns(&sums.keys)?;
    let mut dates: Vec<&str> = sums
        .rows
        .iter()
        .filter_map(|row| parse_metrics_row(row, columns))
        .map(|row| row.date)
        .collect();
    dates.sort_unstable();
    dates.dedup();
    let latest_date = dates
        .last()
        .copied()
        .ok_or_else(|| anyhow!("Languish: no dates available in dataset"))?;
    let previous_date = dates
        .len()
        .checked_sub(2)
        .and_then(|index| dates.get(index))
        .copied();

    let metrics_for_date = |date| {
        sums.rows.iter().find_map(|row| {
            let parsed = parse_metrics_row(row, columns)?;
            (parsed.date == date).then_some(parsed.metrics)
        })
    };
    let snapshot = |date| {
        metrics_for_date(date)
            .map(|metrics| QuarterSnapshot { date, metrics })
            .ok_or_else(|| anyhow!("Languish: missing sums for date {date}"))
    };

    Ok(RecentQuarters {
        latest: snapshot(latest_date)?,
        previous: previous_date.map(snapshot).transpose()?,
    })
}

fn index_of(keys: &[String], name: &str) -> Result<usize> {
    keys.iter()
        .position(|k| k == name)
        .ok_or_else(|| anyhow!("missing column '{name}'"))
}

fn metric_columns(keys: &[String]) -> Result<MetricColumns> {
    Ok(MetricColumns {
        date: index_of(keys, "date")?,
        issues: index_of(keys, "issues")?,
        pulls: index_of(keys, "pulls")?,
        so_questions: index_of(keys, "soQuestions")?,
        stars: index_of(keys, "stars")?,
    })
}

fn item_columns(keys: &[String]) -> Result<ItemColumns> {
    Ok(ItemColumns {
        name: index_of(keys, "name")?,
        metrics: metric_columns(keys)?,
    })
}

fn parse_metrics_row(row: &[Value], columns: MetricColumns) -> Option<MetricsRow<'_>> {
    let date = row.get(columns.date).and_then(Value::as_str)?;
    let metrics = Metrics {
        issues: row.get(columns.issues).map_or(0.0, as_f64),
        pulls: row.get(columns.pulls).map_or(0.0, as_f64),
        so_questions: row.get(columns.so_questions).map_or(0.0, as_f64),
        stars: row.get(columns.stars).map_or(0.0, as_f64),
    };
    Some(MetricsRow { date, metrics })
}

fn parse_item_row(row: &[Value], columns: ItemColumns) -> Option<ItemRow<'_>> {
    let name = row.get(columns.name).and_then(Value::as_str)?;
    let parsed = parse_metrics_row(row, columns.metrics)?;
    Some(ItemRow {
        name,
        date: parsed.date,
        metrics: parsed.metrics,
    })
}

#[derive(Default)]
struct RecentMetrics {
    latest: Option<Metrics>,
    previous: Option<Metrics>,
}

fn build_recent_metrics<'a>(
    items: &'a Table,
    latest: &str,
    previous: Option<&str>,
) -> Result<FxHashMap<&'a str, RecentMetrics>> {
    let mut metrics_by_language: FxHashMap<&str, RecentMetrics> = FxHashMap::default();
    let columns = item_columns(&items.keys)?;
    for row in &items.rows {
        let Some(parsed) = parse_item_row(row, columns) else {
            continue;
        };
        if parsed.date < "2012Q1" {
            continue;
        }
        // Языки без данных в текущих кварталах остаются в таблице с нулевым рейтингом.
        let current = metrics_by_language.entry(parsed.name).or_default();
        if parsed.date == latest {
            current.latest = Some(parsed.metrics);
        } else if previous == Some(parsed.date) {
            current.previous = Some(parsed.metrics);
        }
    }
    Ok(metrics_by_language)
}

fn as_f64(value: &Value) -> f64 {
    value.as_f64().unwrap_or(0.0)
}

fn mean_percent(
    metrics: &Metrics,
    totals: &Metrics,
    weights: CoreWeights,
    total_weight: f64,
) -> f64 {
    if total_weight <= f64::EPSILON {
        return 0.0;
    }

    let mut weighted_sum = 0.0;

    if weights.issues > 0.0 && totals.issues > 0.0 && metrics.issues > 0.0 {
        weighted_sum = (metrics.issues / totals.issues).mul_add(weights.issues, weighted_sum);
    }
    if weights.pulls > 0.0 && totals.pulls > 0.0 && metrics.pulls > 0.0 {
        weighted_sum = (metrics.pulls / totals.pulls).mul_add(weights.pulls, weighted_sum);
    }
    if weights.so_questions > 0.0 && totals.so_questions > 0.0 && metrics.so_questions > 0.0 {
        weighted_sum = (metrics.so_questions / totals.so_questions)
            .mul_add(weights.so_questions, weighted_sum);
    }
    if weights.stars > 0.0 && totals.stars > 0.0 && metrics.stars > 0.0 {
        weighted_sum = (metrics.stars / totals.stars).mul_add(weights.stars, weighted_sum);
    }

    weighted_sum * (100.0 / total_weight)
}

#[cfg(test)]
mod tests {
    use super::{
        as_f64, build_recent_metrics, decode_js_string_literal, extract_json_parse_payload,
        parse_languish_tables, recent_quarters,
    };
    use serde_json::Value;

    #[test]
    fn as_f64_supports_large_unsigned_values() {
        let raw = Value::from(u64::MAX);
        let converted = as_f64(&raw);
        assert!(converted.is_finite());
        assert!(converted > 1.0e18);
    }

    #[test]
    fn extracts_and_decodes_embedded_json_payload() {
        let js = r#"const payload = JSON.parse('{\\"items\\":{\\"keys\\":[] ,\\"rows\\":[]},\\"sums\\":{\\"keys\\":[],\\"rows\\":[]}}');"#;
        let encoded = extract_json_parse_payload(js).expect("payload should be extracted");
        let decoded = decode_js_string_literal(encoded);
        assert!(decoded.contains("items"));
        assert!(decoded.contains("sums"));
    }

    #[test]
    fn decodes_js_only_escapes_without_touching_json_escapes_or_utf8() {
        assert_eq!(decode_js_string_literal(r"é\'\xe9\u2013"), r"é'é\u2013");
        assert_eq!(decode_js_string_literal(r"\\'\\xE9"), r"\\'\\xE9");
        assert_eq!(decode_js_string_literal(r"\\\'\\\xE9"), "\\\\'\\\\é");

        let prefix = "a".repeat(super::BACKSLASH_SCAN_LANES * 2 + 1);
        assert_eq!(
            decode_js_string_literal(&format!(r"{prefix}\''")),
            format!("{prefix}''")
        );
    }

    #[test]
    fn retains_historical_languages_without_retaining_historical_metrics() {
        let json = r#"{
            "items": {
                "keys": ["name", "date", "issues", "pulls", "soQuestions", "stars"],
                "rows": [
                    ["Legacy", "2012Q1", 1, 1, 1, 1],
                    ["Current", "2024Q2", 2, 2, 2, 2],
                    ["Both", "2024Q1", 3, 3, 3, 3],
                    ["Both", "2024Q2", 4, 4, 4, 4]
                ]
            },
            "sums": {
                "keys": ["date", "issues", "pulls", "soQuestions", "stars"],
                "rows": [
                    ["2024Q1", 10, 10, 10, 10],
                    ["2024Q2", 20, 20, 20, 20]
                ]
            }
        }"#;
        let tables = parse_languish_tables(json).expect("fixture should parse");
        let quarters = recent_quarters(&tables.sums).expect("quarters should parse");
        let items = build_recent_metrics(
            &tables.items,
            quarters.latest.date,
            quarters.previous.map(|quarter| quarter.date),
        )
        .expect("items should parse");

        assert_eq!(quarters.latest.date, "2024Q2");
        assert_eq!(
            quarters.previous.map(|quarter| quarter.date),
            Some("2024Q1")
        );
        assert!(items["Legacy"].latest.is_none());
        assert!(items["Legacy"].previous.is_none());
        assert!(items["Current"].latest.is_some());
        assert!(items["Both"].latest.is_some());
        assert!(items["Both"].previous.is_some());
    }
}

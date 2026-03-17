use crate::RankingEntry;
use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use rustc_hash::FxHashMap;
use scraper::{Html, Selector};
use serde_json::Value;
use std::sync::OnceLock;

use super::{RawEntry, aggregate_entries, fetch_text_with_retry};

const LANGUISH_INDEX_URL: &str = "https://tjpalmer.github.io/languish/";

#[derive(Debug)]
struct Table {
    keys: Vec<String>,
    rows: Vec<Vec<Value>>,
}

#[derive(Debug)]
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

impl Default for CoreWeights {
    fn default() -> Self {
        // Languish default: issues=0, pulls=0, stars=1, soQuestions=1
        // but I think that issues=1, pulls=1, stars=1, soQuestions=1 is better =)
        Self {
            issues: 1.0,
            pulls: 1.0,
            so_questions: 1.0,
            stars: 1.0,
        }
    }
}

impl CoreWeights {
    fn total(&self) -> f64 {
        self.issues + self.pulls + self.so_questions + self.stars
    }
}

pub async fn fetch_languish(client: &Client) -> Result<Vec<RankingEntry>> {
    // 1) Load index and discover the main chunk containing embedded data
    let index_html = fetch_text_with_retry(client, LANGUISH_INDEX_URL)
        .await
        .context("failed to download Languish index page")?;
    let main_js_url = extract_main_js_url(&index_html)
        .ok_or_else(|| anyhow!("failed to locate Languish main chunk script"))?;

    // 2) Download the main JS bundle and extract JSON.parse('...') payload
    let js_body = fetch_text_with_retry(client, &main_js_url)
        .await
        .with_context(|| format!("failed to download Languish JS bundle: {main_js_url}"))?;
    let encoded = extract_json_parse_payload(&js_body)
        .ok_or_else(|| anyhow!("failed to extract Languish embedded JSON payload"))?;

    // 3) Decode JS string literal to real JSON text
    let json_text = decode_js_string_literal(&encoded)?;

    // 4) Parse the object with tables we need
    let tables = parse_languish_tables(&json_text)?;

    // 5) Compute latest quarter and (optionally) previous
    let dates = collect_sorted_dates(&tables.sums)?;
    let latest = dates
        .last()
        .ok_or_else(|| anyhow!("Languish: no dates available in dataset"))?;
    let prev = if dates.len() >= 2 {
        Some(dates[dates.len() - 2].clone())
    } else {
        None
    };

    // 6) Build sums by date (for normalization to percentages)
    let sums_by_date = build_sums_by_date(&tables.sums)?;
    let latest_sum = sums_by_date
        .get(latest.as_str())
        .ok_or_else(|| anyhow!("Languish: missing sums for latest date {latest}"))?;
    let prev_sum = prev.as_ref().and_then(|d| sums_by_date.get(d.as_str()));

    // 7) Build items by (name,date)
    let items_by_name_date = build_items_by_name_date(&tables.items)?;
    let weights = CoreWeights::default();
    let weight_total = weights.total();

    // 8) Compute mean percentage for latest (and previous for trend)
    let mut per_lang: Vec<(String, f64, Option<f64>)> =
        Vec::with_capacity(items_by_name_date.len());
    for (name, by_date) in &items_by_name_date {
        let latest_point = by_date.get(latest.as_str());
        let latest_mean =
            latest_point.map_or(0.0, |m| mean_percent(m, latest_sum, weights, weight_total));
        let prev_mean = match (prev.as_ref(), prev_sum) {
            (Some(prev_date), Some(sum)) => {
                let prev_point = by_date.get(prev_date.as_str());
                Some(prev_point.map_or(0.0, |m| mean_percent(m, sum, weights, weight_total)))
            }
            _ => None,
        };
        let trend = prev_mean.map(|p| latest_mean - p);
        per_lang.push((name.clone(), latest_mean, trend));
    }

    // 9) Compute ranks by descending mean
    per_lang.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let mut entries: Vec<RawEntry> = Vec::with_capacity(per_lang.len());
    for (idx, (name, mean, trend)) in per_lang.into_iter().enumerate() {
        let rank = u32::try_from(idx)
            .ok()
            .and_then(|value| value.checked_add(1));
        if rank.is_none() {
            eprintln!("Warning: Languish rank overflow at index {idx}; omitting rank for {name}");
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
            // Ensure absolute URL
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

fn extract_json_parse_payload(js: &str) -> Option<String> {
    // Find JSON.parse(' ... ') and extract the (possibly escaped) payload.
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
        } else if ch == '\'' {
            // End of string if next non-empty char is ')'
            if js[i + 1..].starts_with(')') {
                let payload = &js[start..i];
                return Some(payload.to_string());
            }
        }
        i += 1;
    }
    None
}

fn decode_js_string_literal(encoded: &str) -> Result<String> {
    // Trick: wrap as a JSON string to decode common escapes
    // Need to escape existing backslashes and quotes for JSON parser.
    let mut wrapped = String::with_capacity(encoded.len() + 2);
    wrapped.push('"');
    for ch in encoded.chars() {
        match ch {
            '"' => wrapped.push_str("\\\""),
            '\\' => wrapped.push_str("\\\\"),
            _ => wrapped.push(ch),
        }
    }
    wrapped.push('"');

    let mut decoded: String = serde_json::from_str(&wrapped)
        .context("failed to decode JS string literal via JSON layer")?;

    // Handle JS-specific escapes that JSON doesn't decode
    decoded = decoded.replace("\\'", "'");
    decoded = replace_js_hex_escapes(&decoded);
    Ok(decoded)
}

fn replace_js_hex_escapes(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 3 < bytes.len() && bytes[i + 1] == b'x' {
            let h1 = bytes[i + 2] as char;
            let h2 = bytes[i + 3] as char;
            if h1.is_ascii_hexdigit() && h2.is_ascii_hexdigit() {
                let val = u32::from((hex_val(h1) << 4) | hex_val(h2));
                if let Some(ch) = char::from_u32(val) {
                    out.push(ch);
                    i += 4;
                    continue;
                }
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

const fn hex_val(c: char) -> u8 {
    match c {
        '0'..='9' => (c as u8) - b'0',
        'a'..='f' => 10 + (c as u8) - b'a',
        'A'..='F' => 10 + (c as u8) - b'A',
        _ => 0,
    }
}

#[derive(Default, Clone, Copy)]
struct MetricsRaw {
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
    metrics: MetricsRaw,
}

#[derive(Clone, Copy)]
struct ItemRow<'a> {
    name: &'a str,
    date: &'a str,
    metrics: MetricsRaw,
}

fn parse_languish_tables(js: &str) -> Result<LanguishData> {
    let v: Value =
        serde_json::from_str(js).context("failed to parse decoded Languish JSON object")?;
    let items_v = v
        .get("items")
        .ok_or_else(|| anyhow!("Languish: missing 'items' table"))?;
    let sums_v = v
        .get("sums")
        .ok_or_else(|| anyhow!("Languish: missing 'sums' table"))?;
    let items = Table {
        keys: table_keys(
            items_v
                .get("keys")
                .ok_or_else(|| anyhow!("Languish: items.keys missing"))?,
        )?,
        rows: table_rows(
            "items",
            items_v
                .get("rows")
                .ok_or_else(|| anyhow!("Languish: items.rows missing"))?,
        )?,
    };
    let sums = Table {
        keys: table_keys(
            sums_v
                .get("keys")
                .ok_or_else(|| anyhow!("Languish: sums.keys missing"))?,
        )?,
        rows: table_rows(
            "sums",
            sums_v
                .get("rows")
                .ok_or_else(|| anyhow!("Languish: sums.rows missing"))?,
        )?,
    };
    Ok(LanguishData { items, sums })
}

fn table_keys(v: &Value) -> Result<Vec<String>> {
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow!("expected array for keys"))?;
    Ok(arr
        .iter()
        .map(|k| k.as_str().unwrap_or("").to_string())
        .collect())
}

fn table_rows(table_name: &str, v: &Value) -> Result<Vec<Vec<Value>>> {
    let rows = v
        .as_array()
        .ok_or_else(|| anyhow!("expected array for {table_name} rows"))?;
    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(values) = row.as_array() {
            parsed.push(values.clone());
        }
    }
    Ok(parsed)
}

fn collect_sorted_dates(sums: &Table) -> Result<Vec<String>> {
    let columns = metric_columns(&sums.keys)?;
    let mut dates: Vec<String> = sums
        .rows
        .iter()
        .filter_map(|row| parse_metrics_row(row, columns))
        .map(|row| row.date.to_string())
        .collect();
    dates.sort_unstable();
    dates.dedup();
    Ok(dates)
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
    let metrics = MetricsRaw {
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

fn build_sums_by_date(sums: &Table) -> Result<FxHashMap<String, MetricsRaw>> {
    let mut map = FxHashMap::default();
    let columns = metric_columns(&sums.keys)?;
    for row in &sums.rows {
        let Some(parsed) = parse_metrics_row(row, columns) else {
            continue;
        };
        map.insert(parsed.date.to_string(), parsed.metrics);
    }
    Ok(map)
}

fn build_items_by_name_date(
    items: &Table,
) -> Result<FxHashMap<String, FxHashMap<String, MetricsRaw>>> {
    let mut map: FxHashMap<String, FxHashMap<String, MetricsRaw>> = FxHashMap::default();
    let columns = item_columns(&items.keys)?;
    for row in &items.rows {
        let Some(parsed) = parse_item_row(row, columns) else {
            continue;
        };
        if parsed.date < "2012Q1" {
            // Match Languish filtering baseline
            continue;
        }
        map.entry(parsed.name.to_string())
            .or_default()
            .insert(parsed.date.to_string(), parsed.metrics);
    }
    Ok(map)
}

fn as_f64(v: &Value) -> f64 {
    v.as_f64()
        .or_else(|| {
            v.as_i64()
                .and_then(|value| value.to_string().parse::<f64>().ok())
        })
        .or_else(|| {
            v.as_u64()
                .and_then(|value| value.to_string().parse::<f64>().ok())
        })
        .unwrap_or(0.0)
}

fn mean_percent(m: &MetricsRaw, sum: &MetricsRaw, w: CoreWeights, total_w: f64) -> f64 {
    if total_w <= f64::EPSILON {
        return 0.0;
    }

    let mut weighted_sum = 0.0;

    if w.issues > 0.0 && sum.issues > 0.0 && m.issues > 0.0 {
        weighted_sum = (m.issues / sum.issues).mul_add(w.issues, weighted_sum);
    }
    if w.pulls > 0.0 && sum.pulls > 0.0 && m.pulls > 0.0 {
        weighted_sum = (m.pulls / sum.pulls).mul_add(w.pulls, weighted_sum);
    }
    if w.so_questions > 0.0 && sum.so_questions > 0.0 && m.so_questions > 0.0 {
        weighted_sum = (m.so_questions / sum.so_questions).mul_add(w.so_questions, weighted_sum);
    }
    if w.stars > 0.0 && sum.stars > 0.0 && m.stars > 0.0 {
        weighted_sum = (m.stars / sum.stars).mul_add(w.stars, weighted_sum);
    }

    weighted_sum * (100.0 / total_w)
}

#[cfg(test)]
mod tests {
    use super::{as_f64, decode_js_string_literal, extract_json_parse_payload};
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
        let decoded = decode_js_string_literal(&encoded).expect("payload should decode");
        assert!(decoded.contains("items"));
        assert!(decoded.contains("sums"));
    }
}

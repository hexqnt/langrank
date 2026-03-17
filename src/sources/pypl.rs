use crate::RankingEntry;
use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use scraper::{Html, Selector};
use std::sync::OnceLock;

use super::{
    RawEntry, aggregate_entries, extract_cell_text, fetch_text_with_retry, parse_percent, parse_u32,
};

const PYPL_URL: &str = "https://pypl.github.io/PYPL.html";

struct PyplRow<'a> {
    rank: &'a str,
    lang: &'a str,
    share: &'a str,
    trend: &'a str,
}

impl<'a> PyplRow<'a> {
    fn parse(cells: &'a [String]) -> Option<Self> {
        match cells {
            [rank, _, lang, share, trend, ..] => Some(Self {
                rank,
                lang,
                share,
                trend,
            }),
            _ => None,
        }
    }

    fn into_entry(self) -> Option<RawEntry> {
        RawEntry::parse(
            self.lang,
            parse_u32(self.rank),
            parse_percent(self.share).unwrap_or(0.0),
            parse_percent(self.trend),
        )
    }
}

pub async fn fetch_pypl(client: &Client) -> Result<Vec<RankingEntry>> {
    let body = fetch_text_with_retry(client, PYPL_URL)
        .await
        .context("failed to download PYPL index")?;
    parse_pypl(body.as_str())
}

fn parse_pypl(body: &str) -> Result<Vec<RankingEntry>> {
    let raw_fragment = extract_all_section(body)?;
    let table_html = build_rows_table_html(raw_fragment);
    let document = Html::parse_fragment(table_html.as_str());
    let mut entries = Vec::new();

    for row in document.select(row_selector()) {
        let cells: Vec<String> = row.select(cell_selector()).map(extract_cell_text).collect();
        if let Some(entry) = PyplRow::parse(&cells).and_then(PyplRow::into_entry) {
            entries.push(entry);
        }
    }
    Ok(aggregate_entries(entries))
}

fn extract_all_section(body: &str) -> Result<&str> {
    let start_marker = "<!-- begin section All-->";
    let end_marker = "<!-- end section All-->";
    let start_idx = body
        .find(start_marker)
        .map(|idx| idx + start_marker.len())
        .ok_or_else(|| anyhow!("PYPL start marker not found"))?;
    let end_idx = body[start_idx..]
        .find(end_marker)
        .map(|idx| idx + start_idx)
        .ok_or_else(|| anyhow!("PYPL end marker not found"))?;
    if start_idx >= end_idx {
        return Err(anyhow!("PYPL markers are in unexpected order"));
    }
    Ok(&body[start_idx..end_idx])
}

fn build_rows_table_html(raw_fragment: &str) -> String {
    let mut table_html = String::with_capacity(raw_fragment.len() + 32);
    table_html.push_str("<table>");
    for line in raw_fragment.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed == "\\" || trimmed.contains("\" + table + \"") {
            continue;
        }
        let mut cleaned = trimmed.trim_end_matches('\\').replace("\\\"", "\"");
        if !cleaned.starts_with("<tr") {
            let mut wrapped = String::with_capacity(cleaned.len() + 9);
            wrapped.push_str("<tr>");
            wrapped.push_str(cleaned.as_str());
            cleaned = wrapped;
        }
        if !cleaned.ends_with("</tr>") {
            cleaned.push_str("</tr>");
        }
        table_html.push_str(cleaned.as_str());
    }
    table_html.push_str("</table>");
    table_html
}

fn cell_selector() -> &'static Selector {
    static SELECTOR: OnceLock<Selector> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("td").expect("PYPL cell selector is valid"))
}

fn row_selector() -> &'static Selector {
    static SELECTOR: OnceLock<Selector> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("tr").expect("PYPL row selector is valid"))
}

#[cfg(test)]
mod tests {
    use super::parse_pypl;

    #[test]
    fn parses_all_section_with_noise() {
        let body = r#"
            <html><body>
            <!-- begin section All-->
            <tr><td>1</td><td></td><td>Rust</td><td>13.2%</td><td>+0.4%</td></tr>\
            " + table + "
            <tr><td>2</td><td></td><td>Go</td><td>10.1%</td><td>-0.1%</td></tr>\
            <!-- end section All-->
            </body></html>
        "#;

        let entries = parse_pypl(body).expect("PYPL fixture should parse");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].lang, "Go");
        assert_eq!(entries[1].lang, "Rust");
    }
}

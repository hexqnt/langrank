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

    let start_marker = "<!-- begin section All-->";
    let end_marker = "<!-- end section All-->";
    let start_idx = body
        .find(start_marker)
        .map(|idx| idx + start_marker.len())
        .ok_or_else(|| anyhow!("PYPL start marker not found"))?;
    let end_idx = body
        .find(end_marker)
        .ok_or_else(|| anyhow!("PYPL end marker not found"))?;
    if start_idx >= end_idx {
        return Err(anyhow!("PYPL markers are in unexpected order"));
    }
    let raw_fragment = &body[start_idx..end_idx];
    let mut entries = Vec::new();

    for line in raw_fragment.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed == "\\" || trimmed.contains("\" + table + \"") {
            continue;
        }
        let mut cleaned = trimmed.trim_end_matches('\\').replace("\\\"", "\"");
        if !cleaned.starts_with("<tr") {
            cleaned = format!("<tr>{cleaned}");
        }
        if !cleaned.ends_with("</tr>") {
            cleaned.push_str("</tr>");
        }
        let row_html = format!("<table>{cleaned}</table>");
        let row = Html::parse_fragment(&row_html);
        let cells: Vec<String> = row.select(cell_selector()).map(extract_cell_text).collect();
        if let Some(entry) = PyplRow::parse(&cells).and_then(PyplRow::into_entry) {
            entries.push(entry);
        }
    }

    Ok(aggregate_entries(entries))
}

fn cell_selector() -> &'static Selector {
    static SELECTOR: OnceLock<Selector> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("td").expect("PYPL cell selector is valid"))
}

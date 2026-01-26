use crate::RankingEntry;
use anyhow::{Context, Result};
use reqwest::Client;
use scraper::{Html, Selector};

use super::{
    RawEntry, aggregate_entries, extract_cell_text, fetch_text_with_retry, parse_percent, parse_u32,
};

const TIOBE_URL: &str = "https://www.tiobe.com/tiobe-index/";

pub async fn fetch_tiobe(client: &Client) -> Result<Vec<RankingEntry>> {
    let body = fetch_text_with_retry(client, TIOBE_URL)
        .await
        .context("failed to download TIOBE index")?;
    let document = Html::parse_document(&body);

    let table_selector =
        Selector::parse("table.table.table-striped.table-top20").expect("valid selector");
    let row_selector = Selector::parse("tr").expect("valid selector");
    let cell_selector = Selector::parse("td").expect("valid selector");

    let mut entries = Vec::new();

    if let Some(table) = document.select(&table_selector).next() {
        for row in table.select(&row_selector).skip(1) {
            let cells: Vec<String> = row.select(&cell_selector).map(extract_cell_text).collect();
            if cells.len() >= 7 {
                let rank = parse_u32(&cells[0]);
                let lang = cells[4].clone();
                let share = parse_percent(&cells[5]).unwrap_or(0.0);
                let trend = parse_percent(&cells[6]);
                entries.push(RawEntry {
                    lang,
                    rank,
                    share,
                    trend,
                });
            } else if cells.len() >= 6 {
                let rank = parse_u32(&cells[0]);
                let lang = cells[3].clone();
                let share = parse_percent(&cells[4]).unwrap_or(0.0);
                let trend = parse_percent(&cells[5]);
                entries.push(RawEntry {
                    lang,
                    rank,
                    share,
                    trend,
                });
            }
        }
    }

    let other_table_selector = Selector::parse("table#otherPL").expect("valid selector");
    if let Some(table) = document.select(&other_table_selector).next() {
        for row in table.select(&row_selector).skip(1) {
            let cells: Vec<String> = row.select(&cell_selector).map(extract_cell_text).collect();
            if cells.len() > 2 {
                let rank = parse_u32(&cells[0]);
                let lang = cells[1].clone();
                let share = parse_percent(&cells[2]).unwrap_or(0.0);
                entries.push(RawEntry {
                    lang,
                    rank,
                    share,
                    trend: None,
                });
            }
        }
    }

    Ok(aggregate_entries(entries))
}

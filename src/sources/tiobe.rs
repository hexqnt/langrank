use crate::RankingEntry;
use anyhow::{Context, Result};
use reqwest::Client;
use scraper::{Html, Selector};
use std::sync::OnceLock;

use super::{
    RawEntry, aggregate_entries, extract_cell_text, fetch_text_with_retry, parse_percent, parse_u32,
};

const TIOBE_URL: &str = "https://www.tiobe.com/tiobe-index/";

struct MainRow<'a> {
    rank: &'a str,
    lang: &'a str,
    share: &'a str,
    trend: &'a str,
}

impl<'a> MainRow<'a> {
    fn parse(cells: &'a [String]) -> Option<Self> {
        match cells {
            [rank, _, _, _, lang, share, trend, ..] | [rank, _, _, lang, share, trend, ..] => {
                Some(Self {
                    rank,
                    lang,
                    share,
                    trend,
                })
            }
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

struct OtherRow<'a> {
    rank: &'a str,
    lang: &'a str,
    share: &'a str,
}

impl<'a> OtherRow<'a> {
    fn parse(cells: &'a [String]) -> Option<Self> {
        match cells {
            [rank, lang, share, ..] => Some(Self { rank, lang, share }),
            _ => None,
        }
    }

    fn into_entry(self) -> Option<RawEntry> {
        RawEntry::parse(
            self.lang,
            parse_u32(self.rank),
            parse_percent(self.share).unwrap_or(0.0),
            None,
        )
    }
}

pub async fn fetch_tiobe(client: &Client) -> Result<Vec<RankingEntry>> {
    let body = fetch_text_with_retry(client, TIOBE_URL)
        .await
        .context("failed to download TIOBE index")?;
    Ok(parse_tiobe_html(body.as_str()))
}

fn parse_tiobe_html(body: &str) -> Vec<RankingEntry> {
    let document = Html::parse_document(body);
    let mut entries = Vec::new();

    if let Some(table) = document.select(main_table_selector()).next() {
        for row in table.select(row_selector()).skip(1) {
            let cells: Vec<String> = row.select(cell_selector()).map(extract_cell_text).collect();
            if let Some(entry) = MainRow::parse(&cells).and_then(MainRow::into_entry) {
                entries.push(entry);
            }
        }
    }

    if let Some(table) = document.select(other_table_selector()).next() {
        for row in table.select(row_selector()).skip(1) {
            let cells: Vec<String> = row.select(cell_selector()).map(extract_cell_text).collect();
            if let Some(entry) = OtherRow::parse(&cells).and_then(OtherRow::into_entry) {
                entries.push(entry);
            }
        }
    }

    aggregate_entries(entries)
}

fn main_table_selector() -> &'static Selector {
    static SELECTOR: OnceLock<Selector> = OnceLock::new();
    SELECTOR.get_or_init(|| {
        Selector::parse("table.table.table-striped.table-top20")
            .expect("TIOBE main table selector is valid")
    })
}

fn other_table_selector() -> &'static Selector {
    static SELECTOR: OnceLock<Selector> = OnceLock::new();
    SELECTOR.get_or_init(|| {
        Selector::parse("table#otherPL").expect("TIOBE other table selector is valid")
    })
}

fn row_selector() -> &'static Selector {
    static SELECTOR: OnceLock<Selector> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("tr").expect("TIOBE row selector is valid"))
}

fn cell_selector() -> &'static Selector {
    static SELECTOR: OnceLock<Selector> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("td").expect("TIOBE cell selector is valid"))
}

#[cfg(test)]
mod tests {
    use super::parse_tiobe_html;

    #[test]
    fn parses_main_and_other_tables() {
        let html = r#"
            <html>
              <table class="table table-striped table-top20">
                <tr><th>header</th></tr>
                <tr>
                  <td>1</td><td>x</td><td>x</td><td>x</td>
                  <td>Rust</td><td>10.2%</td><td>+0.6%</td>
                </tr>
              </table>
              <table id="otherPL">
                <tr><th>header</th></tr>
                <tr>
                  <td>24</td><td>Go</td><td>3.1%</td>
                </tr>
              </table>
            </html>
        "#;

        let entries = parse_tiobe_html(html);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].lang, "Go");
        assert_eq!(entries[1].lang, "Rust");
    }
}

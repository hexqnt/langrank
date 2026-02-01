pub mod benchmarks;
pub mod languish;
pub mod pypl;
pub mod techempower;
pub mod tiobe;

pub use benchmarks::{download_benchmark_data, load_benchmark_scores};
pub use languish::fetch_languish;
pub use pypl::fetch_pypl;
pub use techempower::{TECHEMPOWER_MAX_SCORE, fetch_techempower};
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
    let mut aggregated: FxHashMap<String, AggregatedEntry> = FxHashMap::default();

    for entry in entries {
        let Some(normalized) = canonicalize_language(entry.lang.as_str()) else {
            continue;
        };
        let agg = aggregated.entry(normalized).or_default();
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
    let mut out = String::new();
    for chunk in cell.text() {
        let trimmed = chunk.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(trimmed);
    }
    out
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
    let mut buf = String::with_capacity(value.len());
    let mut saw_digit = false;
    let mut saw_decimal = false;

    for ch in value.chars() {
        match ch {
            '0'..='9' => {
                buf.push(ch);
                saw_digit = true;
            }
            '.' | ',' => {
                if !saw_decimal {
                    buf.push('.');
                    saw_decimal = true;
                }
            }
            '-' | '\u{2212}' | '\u{2013}' | '\u{2014}' => {
                if buf.is_empty() {
                    buf.push('-');
                }
            }
            '+' | '%' | ' ' | '\t' | '\n' | '\r' | '\u{00a0}' | '\u{202f}' => {}
            _ => {}
        }
    }

    if !saw_digit {
        return None;
    }

    buf.parse::<f64>().ok()
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

pub fn canonicalize_language(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lookup_key = normalize_alias_key(trimmed);
    let alias_map = canonical_aliases();
    if let Some(&alias) = alias_map.get(lookup_key.as_str()) {
        if alias.is_empty() {
            return None;
        }
        return Some(alias.to_string());
    }
    Some(trimmed.to_string())
}

fn canonical_aliases() -> &'static FxHashMap<&'static str, &'static str> {
    static CANONICAL_ALIASES: OnceLock<FxHashMap<&'static str, &'static str>> = OnceLock::new();
    CANONICAL_ALIASES.get_or_init(|| {
        [
            ("delphi/objectpascal", "Delphi/Pascal"),
            ("matlab", "Matlab"),
            ("cobol", "COBOL"),
            ("powershell", "PowerShell"),
            ("vbscript", "VBA/VBS"),
            ("vba", "VBA/VBS"),
            // ("classicvisualbasic", "VBA/VBS"),
            ("abap", "Abap"),
            ("(visual)foxpro", "FoxPro"),
            ("c", "C"),
            ("c#", "C#"),
            ("csharp", "C#"),
            ("c-sharp", "C#"),
            ("c++", "C++"),
            ("c/c++", "C/C++"),
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
            ("cpp", "C++"),
            ("vb", "Visual Basic"),
            ("vb.net", "Visual Basic"),
            ("vbnet", "Visual Basic"),
            ("visualbasic", "Visual Basic"),
            ("visualbasic.net", "Visual Basic"),
            ("cfml", "CFML"),
            ("clojure", "Clojure"),
            ("commonlisp", "Lisp"),
            ("crystal", "Crystal"),
            ("d", "D"),
            ("dart", "Dart"),
            ("elixir", "Elixir"),
            ("fortran", "Fortran"),
            ("haskell", "Haskell"),
            ("julia", "Julia"),
            ("kotlin", "Kotlin"),
            ("lua", "Lua"),
            ("luau", "Luau"),
            ("nim", "Nim"),
            ("pascal", "Delphi/Pascal"),
            ("prolog", "Prolog"),
            ("python", "Python"),
            ("r", "R"),
            ("ruby", "Ruby"),
            ("rust", "Rust"),
            ("scala", "Scala"),
            ("swift", "Swift"),
            ("ur", "Ur"),
            ("v", "V"),
            ("vala", "Vala"),
            ("zig", "Zig"),
            // Benchmarks Game aliases and runtimes.
            ("chapel", "Chapel"),
            ("clang", "C/C++"),
            ("csharpaot", "C#"),
            ("csharpcore", "C#"),
            ("dartexe", "Dart"),
            ("dartjit", "Dart"),
            ("erlang", "Erlang"),
            ("fpascal", "Delphi/Pascal"),
            ("fsharpcore", "F#"),
            ("gcc", "C/C++"),
            ("ghc", "Haskell"),
            ("gnat", "Ada"),
            ("gpp", "C/C++"),
            ("graalvm", "Graal"),
            ("icx", "C/C++"),
            ("ifc", "Fortran"),
            ("ifx", "Fortran"),
            ("java", "Java"),
            ("javaxint", "Java"),
            ("micropython", "Python"),
            ("mri", "Ruby"),
            ("ocaml", "OCaml"),
            ("openj9", "Java"),
            ("perl", "Perl"),
            ("pharo", "Smalltalk"),
            ("php", "PHP"),
            ("python3", "Python"),
            ("racket", "Racket"),
            ("sbcl", "Lisp"),
            ("toit", "Toit"),
            ("vw", ""),
        ]
        .into_iter()
        .collect()
    })
}

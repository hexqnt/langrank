use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use rustc_hash::FxHashMap;
use std::io::Cursor;
use std::sync::OnceLock;
use tokio::task;

use super::fetch_bytes_with_retry;

const BENCH_URL: &str = "https://salsa.debian.org/benchmarksgame-team/benchmarksgame/-/raw/master/public/data/alldata.csv";

pub async fn download_benchmark_data(client: &Client) -> Result<Vec<u8>> {
    fetch_bytes_with_retry(client, BENCH_URL)
        .await
        .context("failed to download benchmark dataset")
}

pub async fn load_benchmark_stats(bytes: Vec<u8>) -> Result<FxHashMap<String, f64>> {
    let stats = task::spawn_blocking(move || compute_benchmark_stats_sync(&bytes))
        .await
        .context("failed to read benchmark statistics")??;
    Ok(stats)
}

fn compute_benchmark_stats_sync(data: &[u8]) -> Result<FxHashMap<String, f64>> {
    let cursor = Cursor::new(data);
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(cursor);

    let headers = reader
        .headers()
        .context("missing CSV headers in benchmark data")?
        .clone();
    let idx_lang = headers
        .iter()
        .position(|h| h == "lang")
        .ok_or_else(|| anyhow!("missing 'lang' column in benchmark data"))?;
    let idx_name = headers
        .iter()
        .position(|h| h == "name")
        .ok_or_else(|| anyhow!("missing 'name' column in benchmark data"))?;
    let idx_status = headers
        .iter()
        .position(|h| h == "status")
        .ok_or_else(|| anyhow!("missing 'status' column in benchmark data"))?;
    let idx_elapsed = headers
        .iter()
        .position(|h| h == "elapsed-time(s)")
        .ok_or_else(|| anyhow!("missing 'elapsed-time(s)' column in benchmark data"))?;

    let alias_map = benchmark_aliases();
    let mut best_per_problem: FxHashMap<(String, String), f64> = FxHashMap::default();

    for record in reader.records() {
        let record = record.context("failed to read benchmark record")?;
        let status_str = record.get(idx_status).unwrap_or("").trim();
        let status: i64 = match status_str.parse::<i64>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        if status < 0 {
            continue;
        }
        let lang_raw = record.get(idx_lang).unwrap_or("").trim();
        let name = record.get(idx_name).unwrap_or("").trim();
        let elapsed_str = record.get(idx_elapsed).unwrap_or("").trim();
        if lang_raw.is_empty() || name.is_empty() || elapsed_str.is_empty() {
            continue;
        }
        let elapsed: f64 = match elapsed_str.parse::<f64>() {
            Ok(value) if value.is_finite() && value > 0.0 => value,
            _ => continue,
        };

        let lang_key = lang_raw.to_lowercase();
        let key = (lang_key, name.to_string());
        let entry = best_per_problem.entry(key).or_insert(f64::INFINITY);
        if elapsed < *entry {
            *entry = elapsed;
        }
    }

    let mut per_lang: FxHashMap<String, Vec<f64>> = FxHashMap::default();
    for ((lang, _name), elapsed) in best_per_problem {
        if !elapsed.is_finite() || elapsed <= 0.0 {
            continue;
        }
        if let Some(canonical) = canonical_benchmark_lang(lang.as_str(), alias_map) {
            if canonical.is_empty() {
                continue;
            }
            per_lang.entry(canonical).or_default().push(elapsed);
        }
    }

    let mut medians: FxHashMap<String, f64> = FxHashMap::default();
    for (lang, mut values) in per_lang {
        let len = values.len();
        if len == 0 {
            continue;
        }
        let median = if len % 2 == 1 {
            let mid = len / 2;
            let (_, median, _) = values.select_nth_unstable_by(mid, f64::total_cmp);
            *median
        } else {
            let mid = len / 2;
            let (lower_part, upper, _) = values.select_nth_unstable_by(mid, f64::total_cmp);
            let lower = lower_part
                .iter()
                .copied()
                .max_by(f64::total_cmp)
                .unwrap_or(*upper);
            f64::midpoint(lower, *upper)
        };
        if median.is_finite() {
            medians.insert(lang, median);
        }
    }

    if let Some(value) = medians.get("C/C++").copied() {
        medians.insert("C".to_string(), value);
        medians.insert("C++".to_string(), value);
    }

    Ok(medians)
}

fn canonical_benchmark_lang(
    lang_lower: &str,
    alias_map: &FxHashMap<&'static str, &'static str>,
) -> Option<String> {
    if let Some(&alias) = alias_map.get(lang_lower) {
        if alias.is_empty() {
            return None;
        }
        return Some(alias.to_string());
    }
    Some(capitalize_word(lang_lower))
}

fn benchmark_aliases() -> &'static FxHashMap<&'static str, &'static str> {
    static BENCHMARK_ALIASES: OnceLock<FxHashMap<&'static str, &'static str>> = OnceLock::new();
    BENCHMARK_ALIASES.get_or_init(|| {
        [
            ("chapel", "Chapel"),
            ("clang", "C/C++"),
            ("csharpaot", "C#"),
            ("csharpcore", "C#"),
            ("dartexe", "Dart"),
            ("dartjit", "Dart"),
            ("erlang", "Erlang"),
            ("fpascal", "Free Pascal"),
            ("fsharpcore", "F#"),
            ("gcc", "C/C++"),
            ("ghc", "Haskell"),
            ("gnat", "Ada"),
            ("go", "Go"),
            ("gpp", "C/C++"),
            ("graalvm", "Graal"),
            ("icx", "C/C++"),
            ("ifc", "Fortran"),
            ("ifx", "Fortran"),
            ("java", "Java"),
            ("javaxint", "Java"),
            ("julia", "Julia"),
            ("lua", "Lua"),
            ("micropython", "Python"),
            ("mri", "Ruby"),
            ("node", "JavaScript"),
            ("ocaml", "OCaml"),
            ("openj9", "Java"),
            ("perl", "Perl"),
            ("pharo", "Smalltalk"),
            ("php", "PHP"),
            ("python3", "Python"),
            ("racket", "Racket"),
            ("ruby", "Ruby"),
            ("rust", "Rust"),
            ("sbcl", "Lisp"),
            ("swift", "Swift"),
            ("toit", "Toit"),
            ("vw", ""),
        ]
        .into_iter()
        .collect()
    })
}

fn capitalize_word(input: &str) -> String {
    let mut chars = input.chars();
    chars.next().map_or_else(String::new, |first| {
        let mut output = String::new();
        output.extend(first.to_uppercase());
        output.push_str(&chars.as_str().to_lowercase());
        output
    })
}

use anyhow::{Context, Result, anyhow};
use csv::StringRecord;
use reqwest::Client;
use rustc_hash::FxHashMap;
use std::io::Cursor;
use tokio::task;

use super::{CanonicalLanguage, fetch_bytes_with_retry};

const BENCH_URL: &str = "https://salsa.debian.org/benchmarksgame-team/benchmarksgame/-/raw/master/public/data/alldata.csv";

#[derive(Default)]
struct StringInterner {
    ids: FxHashMap<String, usize>,
    values: Vec<String>,
}

impl StringInterner {
    fn intern(&mut self, value: &str) -> usize {
        if let Some(&id) = self.ids.get(value) {
            return id;
        }
        let id = self.values.len();
        let value = value.to_owned();
        self.ids.insert(value.clone(), id);
        self.values.push(value);
        id
    }

    const fn len(&self) -> usize {
        self.values.len()
    }

    fn into_strings(self) -> Vec<String> {
        self.values
    }
}

type LanguageId = usize;
type TaskId = usize;

#[derive(Clone, Copy, Default)]
struct GeometricMeanStats {
    log_sum: f64,
    sample_count: usize,
}

#[derive(Clone, Copy)]
struct BenchmarkColumns {
    language: usize,
    task: usize,
    status: usize,
    elapsed: usize,
}

impl BenchmarkColumns {
    fn parse(headers: &StringRecord) -> Result<Self> {
        let index = |name| {
            headers
                .iter()
                .position(|header| header == name)
                .ok_or_else(|| anyhow!("missing '{name}' column in benchmark data"))
        };
        Ok(Self {
            language: index("lang")?,
            task: index("name")?,
            status: index("status")?,
            elapsed: index("elapsed-time(s)")?,
        })
    }
}

struct BenchmarkRow<'a> {
    language: &'a str,
    task: &'a str,
    elapsed: f64,
}

impl<'a> BenchmarkRow<'a> {
    fn parse(record: &'a StringRecord, columns: BenchmarkColumns) -> Option<Self> {
        let status = record.get(columns.status)?.trim().parse::<i64>().ok()?;
        if status < 0 {
            return None;
        }

        let language = record.get(columns.language)?.trim();
        let task = record.get(columns.task)?.trim();
        let elapsed = record.get(columns.elapsed)?.trim().parse::<f64>().ok()?;
        if language.is_empty() || task.is_empty() || !elapsed.is_finite() || elapsed <= 0.0 {
            return None;
        }

        Some(Self {
            language,
            task,
            elapsed,
        })
    }
}

fn canonical_language_id(
    raw: &str,
    language_id_cache: &mut FxHashMap<String, Option<LanguageId>>,
    languages: &mut StringInterner,
) -> Option<LanguageId> {
    if let Some(&cached) = language_id_cache.get(raw) {
        return cached;
    }

    let id = CanonicalLanguage::parse(raw)
        .map(CanonicalLanguage::into_string)
        .map(|language| languages.intern(&language));
    language_id_cache.insert(raw.to_owned(), id);
    id
}

/// Загружает исходный CSV Benchmarks Game.
///
/// # Errors
///
/// Возвращает ошибку, если данные не удалось получить по HTTP.
pub async fn download_benchmark_data(client: &Client) -> Result<Vec<u8>> {
    fetch_bytes_with_retry(client, BENCH_URL)
        .await
        .context("failed to download benchmark dataset")
}

/// Вычисляет нормализованные показатели языков из CSV Benchmarks Game.
///
/// # Errors
///
/// Возвращает ошибку при некорректной структуре CSV или сбое фоновой задачи.
pub async fn load_benchmark_scores(bytes: Vec<u8>) -> Result<FxHashMap<String, f64>> {
    let scores = task::spawn_blocking(move || compute_benchmark_scores_sync(&bytes))
        .await
        .context("failed to read benchmark statistics")??;
    Ok(scores)
}

fn compute_benchmark_scores_sync(data: &[u8]) -> Result<FxHashMap<String, f64>> {
    let cursor = Cursor::new(data);
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(cursor);

    let headers = reader
        .headers()
        .context("missing CSV headers in benchmark data")?
        .clone();
    let columns = BenchmarkColumns::parse(&headers)?;

    let mut language_id_cache: FxHashMap<String, Option<LanguageId>> = FxHashMap::default();
    let mut languages = StringInterner::default();
    let mut tasks = StringInterner::default();
    let mut best_by_language_task: FxHashMap<(LanguageId, TaskId), f64> = FxHashMap::default();
    let mut best_by_task: Vec<f64> = Vec::new();

    for record in reader.records() {
        let record = record.context("failed to read benchmark record")?;
        let Some(row) = BenchmarkRow::parse(&record, columns) else {
            continue;
        };

        let Some(language_id) =
            canonical_language_id(row.language, &mut language_id_cache, &mut languages)
        else {
            continue;
        };
        let task_id = tasks.intern(row.task);
        best_by_task.resize(tasks.len(), f64::INFINITY);

        let entry = best_by_language_task
            .entry((language_id, task_id))
            .or_insert(f64::INFINITY);
        if row.elapsed < *entry {
            *entry = row.elapsed;
        }

        if row.elapsed < best_by_task[task_id] {
            best_by_task[task_id] = row.elapsed;
        }
    }

    let mut stats_by_language = vec![GeometricMeanStats::default(); languages.len()];
    for ((language_id, task_id), elapsed) in best_by_language_task {
        let best = best_by_task[task_id];
        let ratio = best / elapsed;
        if ratio.is_finite() && ratio > 0.0 {
            let stats = &mut stats_by_language[language_id];
            stats.log_sum += ratio.ln();
            stats.sample_count += 1;
        }
    }

    let mut scores: FxHashMap<String, f64> = FxHashMap::default();
    for (language, stats) in languages.into_strings().into_iter().zip(stats_by_language) {
        if stats.sample_count == 0 {
            continue;
        }
        let Ok(sample_count) = u32::try_from(stats.sample_count) else {
            eprintln!(
                "Warning: benchmark sample count for {language} too large ({}); skipping score",
                stats.sample_count
            );
            continue;
        };
        let score = (stats.log_sum / f64::from(sample_count)).exp();
        if score.is_finite() {
            scores.insert(language, score);
        }
    }

    if let Some(value) = scores.get("C/C++").copied() {
        scores.insert("C".to_string(), value);
        scores.insert("C++".to_string(), value);
    }

    Ok(scores)
}

#[cfg(test)]
mod tests {
    use super::compute_benchmark_scores_sync;

    #[test]
    fn computes_geometric_mean_from_best_runs() {
        let csv = b"lang,name,status,elapsed-time(s)\n\
            gpp,task-a,0,1.0\n\
            gpp,task-a,0,1.5\n\
            gpp,task-b,0,4.0\n\
            python,task-a,0,2.0\n\
            python,task-b,0,2.0\n";

        let scores = compute_benchmark_scores_sync(csv).expect("fixture should parse");
        let expected = 0.5_f64.sqrt();

        assert!((scores["C/C++"] - expected).abs() < f64::EPSILON);
        assert!((scores["Python"] - expected).abs() < f64::EPSILON);
        assert_eq!(scores["C"], scores["C/C++"]);
        assert_eq!(scores["C++"], scores["C/C++"]);
    }
}

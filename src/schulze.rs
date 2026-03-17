use crate::RankingEntry;
use anyhow::{Result, anyhow};
use ndarray::{Array2, Zip};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use std::cmp::Ordering;

#[derive(Debug, Serialize)]
pub struct SchulzeRecord {
    pub position: usize,
    pub lang: String,
    pub tiobe_rank: Option<u32>,
    pub tiobe_share: f64,
    pub tiobe_trend: Option<f64>,
    pub pypl_rank: Option<u32>,
    pub pypl_share: f64,
    pub pypl_trend: Option<f64>,
    pub languish_rank: Option<u32>,
    pub languish_share: f64,
    pub languish_trend: Option<f64>,
    pub benchmark_score: Option<f64>,
    pub techempower_score: Option<f64>,
    pub perf_score: f64,
    pub schulze_wins: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct SchulzeConfig {
    pub min_source_overlap: usize,
    pub max_ranked_languages: usize,
    pub techempower_max_score: f64,
}

pub fn compute_schulze_records(
    tiobe: &[RankingEntry],
    pypl: &[RankingEntry],
    languish: &[RankingEntry],
    benchmark: &FxHashMap<String, f64>,
    techempower: &FxHashMap<String, f64>,
    config: SchulzeConfig,
) -> Result<Vec<SchulzeRecord>> {
    let tiobe_index = build_ranking_index(tiobe);
    let pypl_index = build_ranking_index(pypl);
    let languish_index = build_ranking_index(languish);
    let sources = RankingSources {
        tiobe: RankingSource {
            entries: tiobe,
            index: &tiobe_index,
        },
        pypl: RankingSource {
            entries: pypl,
            index: &pypl_index,
        },
        languish: RankingSource {
            entries: languish,
            index: &languish_index,
        },
        benchmark,
        techempower,
        techempower_max_score: config.techempower_max_score,
    };
    let languages = collect_languages(
        tiobe,
        pypl,
        languish,
        benchmark,
        techempower,
        config.min_source_overlap,
    );
    let languages = limit_languages(languages, &sources, config.max_ranked_languages);
    if languages.len() < 2 {
        return Err(anyhow!(
            "Not enough overlapping languages ({}) to compute Schulze ranking",
            languages.len()
        ));
    }
    let ballots = build_ballots(&languages, &sources);
    let preference_strengths = build_preference_matrix(languages.len(), &ballots);
    let index_map = build_language_index(&languages);
    let ranked_indices = rank_languages(&languages, &preference_strengths, &index_map, &sources);

    let mut records = Vec::with_capacity(languages.len());
    for (position, &idx) in ranked_indices.iter().enumerate() {
        let lang = languages[idx].as_str();
        let wins = (0..languages.len())
            .filter(|&other| {
                other != idx
                    && preference_strengths[[idx, other]] > preference_strengths[[other, idx]]
            })
            .count();

        let tiobe_entry = sources.tiobe.entry(lang);
        let pypl_entry = sources.pypl.entry(lang);
        let languish_entry = sources.languish.entry(lang);
        let bench_value = sources.benchmark_value(lang);
        let techempower_score = sources.techempower_value(lang);
        let perf_score = sources.perf_score(lang);

        records.push(SchulzeRecord {
            position: position + 1,
            lang: lang.to_owned(),
            tiobe_rank: tiobe_entry.and_then(|entry| entry.rank),
            tiobe_share: tiobe_entry.map_or(0.0, |entry| entry.share),
            tiobe_trend: tiobe_entry.and_then(|entry| entry.trend),
            pypl_rank: pypl_entry.and_then(|entry| entry.rank),
            pypl_share: pypl_entry.map_or(0.0, |entry| entry.share),
            pypl_trend: pypl_entry.and_then(|entry| entry.trend),
            languish_rank: languish_entry.and_then(|entry| entry.rank),
            languish_share: languish_entry.map_or(0.0, |entry| entry.share),
            languish_trend: languish_entry.and_then(|entry| entry.trend),
            benchmark_score: bench_value,
            techempower_score,
            perf_score,
            schulze_wins: wins,
        });
    }

    Ok(records)
}

struct RankingSource<'a> {
    entries: &'a [RankingEntry],
    index: &'a FxHashMap<&'a str, usize>,
}

impl<'a> RankingSource<'a> {
    fn entry(&self, lang: &str) -> Option<&'a RankingEntry> {
        self.index.get(lang).and_then(|&idx| self.entries.get(idx))
    }

    fn share(&self, lang: &str) -> f64 {
        self.entry(lang).map_or(0.0, |entry| entry.share)
    }
}

struct RankingSources<'a> {
    tiobe: RankingSource<'a>,
    pypl: RankingSource<'a>,
    languish: RankingSource<'a>,
    benchmark: &'a FxHashMap<String, f64>,
    techempower: &'a FxHashMap<String, f64>,
    techempower_max_score: f64,
}

impl RankingSources<'_> {
    fn benchmark_value(&self, lang: &str) -> Option<f64> {
        self.benchmark.get(lang).copied()
    }

    fn techempower_value(&self, lang: &str) -> Option<f64> {
        self.techempower.get(lang).copied()
    }

    fn perf_score(&self, lang: &str) -> f64 {
        let bg = self.benchmark_value(lang).unwrap_or(0.0);
        let te_raw = self.techempower_value(lang).unwrap_or(0.0);
        let te_norm = if self.techempower_max_score > 0.0 {
            te_raw / self.techempower_max_score
        } else {
            0.0
        };
        f64::midpoint(bg, te_norm)
    }
}

fn build_ranking_index(entries: &[RankingEntry]) -> FxHashMap<&str, usize> {
    entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.lang.as_str(), idx))
        .collect()
}

fn collect_languages<'a>(
    tiobe: &'a [RankingEntry],
    pypl: &'a [RankingEntry],
    languish: &'a [RankingEntry],
    benchmark: &'a FxHashMap<String, f64>,
    techempower: &'a FxHashMap<String, f64>,
    min_sources: usize,
) -> Vec<String> {
    let mut counts: FxHashMap<&str, usize> = FxHashMap::default();
    for entry in tiobe {
        *counts.entry(entry.lang.as_str()).or_insert(0) += 1;
    }
    for entry in pypl {
        *counts.entry(entry.lang.as_str()).or_insert(0) += 1;
    }
    for entry in languish {
        *counts.entry(entry.lang.as_str()).or_insert(0) += 1;
    }

    let mut perf_langs: FxHashSet<&str> = FxHashSet::default();
    for lang in benchmark.keys() {
        perf_langs.insert(lang.as_str());
    }
    for lang in techempower.keys() {
        perf_langs.insert(lang.as_str());
    }
    for lang in perf_langs {
        *counts.entry(lang).or_insert(0) += 1;
    }

    let mut languages: Vec<String> = counts
        .into_iter()
        .filter(|(_, count)| *count >= min_sources)
        .map(|(lang, _)| lang.to_owned())
        .collect();
    languages.sort_unstable();
    languages
}

fn limit_languages(
    languages: Vec<String>,
    sources: &RankingSources<'_>,
    max_languages: usize,
) -> Vec<String> {
    if max_languages == 0 || languages.len() <= max_languages {
        return languages;
    }

    let mut scored: Vec<(usize, f64, f64, String)> = Vec::with_capacity(languages.len());
    for lang in languages {
        let lang_ref = lang.as_str();
        let source_count = count_sources(lang_ref, sources);
        let popularity_score = sources.tiobe.share(lang_ref)
            + sources.pypl.share(lang_ref)
            + sources.languish.share(lang_ref);
        let perf_component = sources.perf_score(lang_ref);
        scored.push((source_count, popularity_score, perf_component, lang));
    }

    let cmp_scores =
        |(count_a, pop_a, perf_a, lang_a): &(usize, f64, f64, String),
         (count_b, pop_b, perf_b, lang_b): &(usize, f64, f64, String)| {
            count_b
                .cmp(count_a)
                .then_with(|| pop_b.total_cmp(pop_a))
                .then_with(|| perf_b.total_cmp(perf_a))
                .then_with(|| lang_a.cmp(lang_b))
        };
    let nth = max_languages.saturating_sub(1);
    scored.select_nth_unstable_by(nth, cmp_scores);
    scored.truncate(max_languages);

    let mut limited: Vec<String> = scored.into_iter().map(|(_, _, _, lang)| lang).collect();
    limited.sort_unstable();
    limited
}

fn count_sources(lang: &str, sources: &RankingSources<'_>) -> usize {
    let mut count = 0;
    if sources.tiobe.entry(lang).is_some() {
        count += 1;
    }
    if sources.pypl.entry(lang).is_some() {
        count += 1;
    }
    if sources.languish.entry(lang).is_some() {
        count += 1;
    }
    if sources.benchmark.contains_key(lang) || sources.techempower.contains_key(lang) {
        count += 1;
    }
    count
}

fn build_ballots(languages: &[String], sources: &RankingSources<'_>) -> Vec<Vec<usize>> {
    let tiobe_order = order_by_metric(languages, |lang| sources.tiobe.share(lang), false);
    let pypl_order = order_by_metric(languages, |lang| sources.pypl.share(lang), false);
    let languish_order = order_by_metric(languages, |lang| sources.languish.share(lang), false);
    let performance_order = order_by_metric(languages, |lang| sources.perf_score(lang), false);

    vec![tiobe_order, pypl_order, languish_order, performance_order]
}

fn order_by_metric<F>(languages: &[String], metric: F, ascending: bool) -> Vec<usize>
where
    F: Fn(&str) -> f64,
{
    let mut scored: Vec<(usize, f64)> = Vec::with_capacity(languages.len());
    for (idx, lang) in languages.iter().enumerate() {
        scored.push((idx, metric(lang.as_str())));
    }
    scored.sort_by(|(idx_a, score_a), (idx_b, score_b)| {
        let ord = if ascending {
            score_a.total_cmp(score_b)
        } else {
            score_b.total_cmp(score_a)
        };
        ord.then_with(|| languages[*idx_a].cmp(&languages[*idx_b]))
    });
    scored.into_iter().map(|(idx, _)| idx).collect()
}

fn build_language_index(languages: &[String]) -> FxHashMap<&str, usize> {
    languages
        .iter()
        .enumerate()
        .map(|(idx, lang)| (lang.as_str(), idx))
        .collect()
}

fn rank_languages(
    languages: &[String],
    preference_strengths: &Array2<usize>,
    index_map: &FxHashMap<&str, usize>,
    sources: &RankingSources<'_>,
) -> Vec<usize> {
    let combined_scores: Vec<f64> = languages
        .iter()
        .map(|lang| combined_score(lang.as_str(), sources))
        .collect();
    let mut ranked: Vec<usize> = (0..languages.len()).collect();
    ranked.sort_by(|&i_a, &i_b| {
        match preference_strengths[[i_a, i_b]].cmp(&preference_strengths[[i_b, i_a]]) {
            Ordering::Greater => Ordering::Less,
            Ordering::Less => Ordering::Greater,
            Ordering::Equal => {
                let score_a = combined_scores[i_a];
                let score_b = combined_scores[i_b];
                match score_b.partial_cmp(&score_a).unwrap_or(Ordering::Equal) {
                    Ordering::Equal => {
                        let lang_a = languages[i_a].as_str();
                        let lang_b = languages[i_b].as_str();
                        lang_a
                            .cmp(lang_b)
                            .then_with(|| index_map[lang_a].cmp(&index_map[lang_b]))
                    }
                    other => other,
                }
            }
        }
    });
    ranked
}

fn build_preference_matrix(n: usize, ballots: &[Vec<usize>]) -> Array2<usize> {
    let mut d = Array2::<usize>::zeros((n, n));
    for ballot in ballots {
        let mut positions = vec![0usize; n];
        for (pos, &idx) in ballot.iter().enumerate() {
            positions[idx] = pos;
        }
        for i in 0..n {
            for j in 0..n {
                if i != j && positions[i] < positions[j] {
                    d[[i, j]] += 1;
                }
            }
        }
    }

    let mut p = Array2::<usize>::zeros((n, n));
    Zip::from(&mut p)
        .and(&d)
        .and(&d.t())
        .for_each(|p_cell, &d_ij, &d_ji| {
            if d_ij > d_ji {
                *p_cell = d_ij;
            }
        });

    for i in 0..n {
        for j in 0..n {
            if i == j {
                continue;
            }
            for k in 0..n {
                if i == k || j == k {
                    continue;
                }
                let candidate = p[[j, i]].min(p[[i, k]]);
                if candidate > p[[j, k]] {
                    p[[j, k]] = candidate;
                }
            }
        }
    }

    p
}

fn combined_score(lang: &str, sources: &RankingSources<'_>) -> f64 {
    let tiobe_share = sources.tiobe.share(lang);
    let pypl_share = sources.pypl.share(lang);
    let languish_share = sources.languish.share(lang);
    let perf_component = sources.perf_score(lang);
    tiobe_share + pypl_share + languish_share + perf_component
}

#[cfg(test)]
mod tests {
    use super::{SchulzeConfig, compute_schulze_records};
    use crate::RankingEntry;
    use rustc_hash::FxHashMap;

    #[test]
    fn stable_ranking_on_fixed_snapshot() {
        let tiobe = vec![
            RankingEntry {
                lang: "Rust".to_string(),
                rank: Some(1),
                share: 20.0,
                trend: Some(0.5),
            },
            RankingEntry {
                lang: "Go".to_string(),
                rank: Some(2),
                share: 15.0,
                trend: Some(0.1),
            },
            RankingEntry {
                lang: "Python".to_string(),
                rank: Some(3),
                share: 10.0,
                trend: Some(-0.1),
            },
        ];
        let pypl = vec![
            RankingEntry {
                lang: "Rust".to_string(),
                rank: Some(2),
                share: 12.0,
                trend: Some(0.2),
            },
            RankingEntry {
                lang: "Go".to_string(),
                rank: Some(1),
                share: 14.0,
                trend: Some(0.3),
            },
            RankingEntry {
                lang: "Python".to_string(),
                rank: Some(3),
                share: 9.0,
                trend: Some(-0.2),
            },
        ];
        let languish = vec![
            RankingEntry {
                lang: "Rust".to_string(),
                rank: Some(1),
                share: 18.0,
                trend: Some(0.4),
            },
            RankingEntry {
                lang: "Go".to_string(),
                rank: Some(3),
                share: 11.0,
                trend: Some(0.1),
            },
            RankingEntry {
                lang: "Python".to_string(),
                rank: Some(2),
                share: 13.0,
                trend: Some(0.0),
            },
        ];

        let mut benchmark = FxHashMap::default();
        benchmark.insert("Rust".to_string(), 0.9);
        benchmark.insert("Go".to_string(), 0.8);
        benchmark.insert("Python".to_string(), 0.5);

        let mut techempower = FxHashMap::default();
        techempower.insert("Rust".to_string(), 5.4);
        techempower.insert("Go".to_string(), 4.8);
        techempower.insert("Python".to_string(), 3.2);

        let records = compute_schulze_records(
            &tiobe,
            &pypl,
            &languish,
            &benchmark,
            &techempower,
            SchulzeConfig {
                min_source_overlap: 3,
                max_ranked_languages: 0,
                techempower_max_score: 6.0,
            },
        )
        .expect("snapshot ranking should be computed");

        let order: Vec<&str> = records.iter().map(|record| record.lang.as_str()).collect();
        assert_eq!(order, vec!["Rust", "Go", "Python"]);
    }
}

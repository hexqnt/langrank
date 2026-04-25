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
    let sources = RankingSources::new(tiobe, pypl, languish, benchmark, techempower, config);
    let languages = collect_language_names(&sources, config.min_source_overlap);
    let candidates = build_candidates(languages, &sources);
    let candidates = limit_candidates(candidates, config.max_ranked_languages);

    if candidates.len() < 2 {
        return Err(anyhow!(
            "Not enough overlapping languages ({}) to compute Schulze ranking",
            candidates.len()
        ));
    }

    let ballots = build_ballots(&candidates);
    let preference_strengths = build_preference_matrix(candidates.len(), &ballots);
    let ranked_indices = rank_languages(&candidates, &preference_strengths);

    Ok(build_records(
        &candidates,
        &ranked_indices,
        &preference_strengths,
    ))
}

struct RankingSource<'a> {
    entries: &'a [RankingEntry],
    index: FxHashMap<&'a str, usize>,
}

impl<'a> RankingSource<'a> {
    fn new(entries: &'a [RankingEntry]) -> Self {
        Self {
            entries,
            index: build_ranking_index(entries),
        }
    }

    fn entry(&self, lang: &str) -> Option<&'a RankingEntry> {
        let &idx = self.index.get(lang)?;
        self.entries.get(idx)
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

impl<'a> RankingSources<'a> {
    fn new(
        tiobe: &'a [RankingEntry],
        pypl: &'a [RankingEntry],
        languish: &'a [RankingEntry],
        benchmark: &'a FxHashMap<String, f64>,
        techempower: &'a FxHashMap<String, f64>,
        config: SchulzeConfig,
    ) -> Self {
        Self {
            tiobe: RankingSource::new(tiobe),
            pypl: RankingSource::new(pypl),
            languish: RankingSource::new(languish),
            benchmark,
            techempower,
            techempower_max_score: config.techempower_max_score,
        }
    }

    fn benchmark_value(&self, lang: &str) -> Option<f64> {
        self.benchmark.get(lang).copied()
    }

    fn techempower_value(&self, lang: &str) -> Option<f64> {
        self.techempower.get(lang).copied()
    }
}

struct LanguageCandidate<'a> {
    name: String,
    tiobe: Option<&'a RankingEntry>,
    pypl: Option<&'a RankingEntry>,
    languish: Option<&'a RankingEntry>,
    benchmark_score: Option<f64>,
    techempower_score: Option<f64>,
    source_count: usize,
    popularity_score: f64,
    perf_score: f64,
    combined_score: f64,
}

impl<'a> LanguageCandidate<'a> {
    fn new(name: String, sources: &RankingSources<'a>) -> Self {
        let lang = name.as_str();
        let tiobe = sources.tiobe.entry(lang);
        let pypl = sources.pypl.entry(lang);
        let languish = sources.languish.entry(lang);
        let benchmark_score = sources.benchmark_value(lang);
        let techempower_score = sources.techempower_value(lang);
        let perf_score = performance_score(
            benchmark_score,
            techempower_score,
            sources.techempower_max_score,
        );
        let popularity_score = source_share(tiobe) + source_share(pypl) + source_share(languish);
        let source_count = usize::from(tiobe.is_some())
            + usize::from(pypl.is_some())
            + usize::from(languish.is_some())
            + usize::from(benchmark_score.is_some() || techempower_score.is_some());

        Self {
            name,
            tiobe,
            pypl,
            languish,
            benchmark_score,
            techempower_score,
            source_count,
            popularity_score,
            perf_score,
            combined_score: popularity_score + perf_score,
        }
    }

    const fn lang(&self) -> &str {
        self.name.as_str()
    }

    fn record(&self, position: usize, schulze_wins: usize) -> SchulzeRecord {
        SchulzeRecord {
            position,
            lang: self.name.clone(),
            tiobe_rank: self.tiobe.and_then(|entry| entry.rank),
            tiobe_share: source_share(self.tiobe),
            tiobe_trend: self.tiobe.and_then(|entry| entry.trend),
            pypl_rank: self.pypl.and_then(|entry| entry.rank),
            pypl_share: source_share(self.pypl),
            pypl_trend: self.pypl.and_then(|entry| entry.trend),
            languish_rank: self.languish.and_then(|entry| entry.rank),
            languish_share: source_share(self.languish),
            languish_trend: self.languish.and_then(|entry| entry.trend),
            benchmark_score: self.benchmark_score,
            techempower_score: self.techempower_score,
            perf_score: self.perf_score,
            schulze_wins,
        }
    }
}

fn build_ranking_index(entries: &[RankingEntry]) -> FxHashMap<&str, usize> {
    entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.lang.as_str(), idx))
        .collect()
}

fn source_share(entry: Option<&RankingEntry>) -> f64 {
    entry.map_or(0.0, |entry| entry.share)
}

fn performance_score(
    benchmark_score: Option<f64>,
    techempower_score: Option<f64>,
    techempower_max_score: f64,
) -> f64 {
    let benchmark = benchmark_score.unwrap_or(0.0);
    let techempower = techempower_score.unwrap_or(0.0);
    let normalized_techempower = if techempower_max_score > 0.0 {
        techempower / techempower_max_score
    } else {
        0.0
    };
    f64::midpoint(benchmark, normalized_techempower)
}

fn collect_language_names(sources: &RankingSources<'_>, min_sources: usize) -> Vec<String> {
    let mut counts: FxHashMap<&str, usize> = FxHashMap::default();
    add_ranking_source(&mut counts, sources.tiobe.entries);
    add_ranking_source(&mut counts, sources.pypl.entries);
    add_ranking_source(&mut counts, sources.languish.entries);
    add_performance_source(&mut counts, sources.benchmark, sources.techempower);

    let mut languages: Vec<String> = counts
        .into_iter()
        .filter(|(_, count)| *count >= min_sources)
        .map(|(lang, _)| lang.to_owned())
        .collect();
    languages.sort_unstable();
    languages
}

fn add_ranking_source<'a>(counts: &mut FxHashMap<&'a str, usize>, entries: &'a [RankingEntry]) {
    let mut seen = FxHashSet::default();
    for entry in entries {
        let lang = entry.lang.as_str();
        if seen.insert(lang) {
            *counts.entry(lang).or_insert(0) += 1;
        }
    }
}

fn add_performance_source<'a>(
    counts: &mut FxHashMap<&'a str, usize>,
    benchmark: &'a FxHashMap<String, f64>,
    techempower: &'a FxHashMap<String, f64>,
) {
    let mut seen = FxHashSet::default();
    for lang in benchmark.keys().chain(techempower.keys()) {
        seen.insert(lang.as_str());
    }
    for lang in seen {
        *counts.entry(lang).or_insert(0) += 1;
    }
}

fn build_candidates<'a>(
    languages: Vec<String>,
    sources: &RankingSources<'a>,
) -> Vec<LanguageCandidate<'a>> {
    languages
        .into_iter()
        .map(|lang| LanguageCandidate::new(lang, sources))
        .collect()
}

fn limit_candidates(
    mut candidates: Vec<LanguageCandidate<'_>>,
    max_languages: usize,
) -> Vec<LanguageCandidate<'_>> {
    if max_languages == 0 || candidates.len() <= max_languages {
        return candidates;
    }

    candidates.select_nth_unstable_by(max_languages - 1, compare_candidate_scores);
    candidates.truncate(max_languages);
    candidates.sort_unstable_by(|left, right| left.lang().cmp(right.lang()));
    candidates
}

fn compare_candidate_scores(
    left: &LanguageCandidate<'_>,
    right: &LanguageCandidate<'_>,
) -> Ordering {
    right
        .source_count
        .cmp(&left.source_count)
        .then_with(|| right.popularity_score.total_cmp(&left.popularity_score))
        .then_with(|| right.perf_score.total_cmp(&left.perf_score))
        .then_with(|| left.lang().cmp(right.lang()))
}

const SOURCE_BALLOT_COUNT: usize = 4;

type Ballots = [Vec<usize>; SOURCE_BALLOT_COUNT];

fn build_ballots(candidates: &[LanguageCandidate<'_>]) -> Ballots {
    [
        order_by_metric(candidates, |candidate| source_share(candidate.tiobe)),
        order_by_metric(candidates, |candidate| source_share(candidate.pypl)),
        order_by_metric(candidates, |candidate| source_share(candidate.languish)),
        order_by_metric(candidates, |candidate| candidate.perf_score),
    ]
}

fn order_by_metric<F>(candidates: &[LanguageCandidate<'_>], metric: F) -> Vec<usize>
where
    F: Fn(&LanguageCandidate<'_>) -> f64,
{
    let mut scored: Vec<(usize, f64)> = candidates
        .iter()
        .enumerate()
        .map(|(idx, candidate)| (idx, metric(candidate)))
        .collect();

    scored.sort_by(|(idx_a, score_a), (idx_b, score_b)| {
        score_b
            .total_cmp(score_a)
            .then_with(|| candidates[*idx_a].lang().cmp(candidates[*idx_b].lang()))
    });
    scored.into_iter().map(|(idx, _)| idx).collect()
}

fn rank_languages(
    candidates: &[LanguageCandidate<'_>],
    preference_strengths: &Array2<usize>,
) -> Vec<usize> {
    let mut ranked: Vec<usize> = (0..candidates.len()).collect();
    ranked.sort_by(|&left, &right| {
        match preference_strengths[[left, right]].cmp(&preference_strengths[[right, left]]) {
            Ordering::Greater => Ordering::Less,
            Ordering::Less => Ordering::Greater,
            Ordering::Equal => candidates[right]
                .combined_score
                .total_cmp(&candidates[left].combined_score)
                .then_with(|| candidates[left].lang().cmp(candidates[right].lang())),
        }
    });
    ranked
}

fn build_records(
    candidates: &[LanguageCandidate<'_>],
    ranked_indices: &[usize],
    preference_strengths: &Array2<usize>,
) -> Vec<SchulzeRecord> {
    ranked_indices
        .iter()
        .enumerate()
        .map(|(position, &idx)| {
            let wins = schulze_wins(preference_strengths, idx);
            candidates[idx].record(position + 1, wins)
        })
        .collect()
}

fn schulze_wins(preference_strengths: &Array2<usize>, candidate_idx: usize) -> usize {
    (0..preference_strengths.nrows())
        .filter(|&other_idx| {
            other_idx != candidate_idx
                && preference_strengths[[candidate_idx, other_idx]]
                    > preference_strengths[[other_idx, candidate_idx]]
        })
        .count()
}

fn build_preference_matrix(candidate_count: usize, ballots: &Ballots) -> Array2<usize> {
    let direct_preferences = build_direct_preference_matrix(candidate_count, ballots);
    let strongest_paths = build_initial_strongest_paths(&direct_preferences);
    compute_strongest_paths(strongest_paths)
}

fn build_direct_preference_matrix(candidate_count: usize, ballots: &Ballots) -> Array2<usize> {
    let mut preferences = Array2::<usize>::zeros((candidate_count, candidate_count));
    for ballot in ballots {
        for (preferred_pos, &preferred_idx) in ballot.iter().enumerate() {
            for &weaker_idx in &ballot[preferred_pos + 1..] {
                preferences[[preferred_idx, weaker_idx]] += 1;
            }
        }
    }
    preferences
}

fn build_initial_strongest_paths(direct_preferences: &Array2<usize>) -> Array2<usize> {
    let mut paths = Array2::<usize>::zeros(direct_preferences.dim());
    Zip::from(&mut paths)
        .and(direct_preferences)
        .and(&direct_preferences.t())
        .for_each(|path, &left, &right| {
            if left > right {
                *path = left;
            }
        });
    paths
}

fn compute_strongest_paths(mut paths: Array2<usize>) -> Array2<usize> {
    let candidate_count = paths.nrows();
    for pivot in 0..candidate_count {
        for from in 0..candidate_count {
            if from == pivot {
                continue;
            }
            let path_to_pivot = paths[[from, pivot]];
            if path_to_pivot == 0 {
                continue;
            }
            for to in 0..candidate_count {
                if to == pivot || to == from {
                    continue;
                }
                let candidate = path_to_pivot.min(paths[[pivot, to]]);
                if candidate > paths[[from, to]] {
                    paths[[from, to]] = candidate;
                }
            }
        }
    }
    paths
}

#[cfg(test)]
mod tests {
    use super::{SchulzeConfig, compute_schulze_records};
    use crate::RankingEntry;
    use rustc_hash::FxHashMap;

    fn entry(lang: &str, rank: u32, share: f64, trend: f64) -> RankingEntry {
        RankingEntry {
            lang: lang.to_owned(),
            rank: Some(rank),
            share,
            trend: Some(trend),
        }
    }

    fn performance_scores(scores: &[(&str, f64)]) -> FxHashMap<String, f64> {
        scores
            .iter()
            .map(|&(lang, score)| (lang.to_owned(), score))
            .collect()
    }

    #[test]
    fn stable_ranking_on_fixed_snapshot() {
        let tiobe = vec![
            entry("Rust", 1, 20.0, 0.5),
            entry("Go", 2, 15.0, 0.1),
            entry("Python", 3, 10.0, -0.1),
        ];
        let pypl = vec![
            entry("Rust", 2, 12.0, 0.2),
            entry("Go", 1, 14.0, 0.3),
            entry("Python", 3, 9.0, -0.2),
        ];
        let languish = vec![
            entry("Rust", 1, 18.0, 0.4),
            entry("Go", 3, 11.0, 0.1),
            entry("Python", 2, 13.0, 0.0),
        ];

        let benchmark = performance_scores(&[("Rust", 0.9), ("Go", 0.8), ("Python", 0.5)]);
        let techempower = performance_scores(&[("Rust", 5.4), ("Go", 4.8), ("Python", 3.2)]);

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

    #[test]
    fn duplicate_entries_in_one_source_count_once() {
        let tiobe = vec![entry("Rust", 1, 20.0, 0.5), entry("Rust", 2, 10.0, 0.1)];
        let pypl = vec![entry("Go", 1, 14.0, 0.3), entry("Python", 2, 9.0, -0.2)];
        let languish = vec![entry("Ruby", 1, 13.0, 0.0)];
        let benchmark = FxHashMap::default();
        let techempower = FxHashMap::default();

        let error = compute_schulze_records(
            &tiobe,
            &pypl,
            &languish,
            &benchmark,
            &techempower,
            SchulzeConfig {
                min_source_overlap: 2,
                max_ranked_languages: 0,
                techempower_max_score: 1.0,
            },
        )
        .expect_err("duplicates inside one source must not satisfy overlap");

        assert_eq!(
            error.to_string(),
            "Not enough overlapping languages (0) to compute Schulze ranking"
        );
    }
}

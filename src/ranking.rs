use serde::Serialize;
use std::fmt;

/// Запись рейтинга одного языка.
#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct RankingEntry {
    /// Каноническое имя языка программирования.
    pub lang: String,
    /// Позиция в исходном рейтинге, если источник её предоставляет.
    pub rank: Option<u32>,
    /// Доля или нормализованный показатель источника.
    pub share: f64,
    /// Изменение показателя, если источник его предоставляет.
    pub trend: Option<f64>,
}

/// Источник рейтинга популярности.
#[derive(Debug, Serialize, Copy, Clone, Eq, PartialEq, Hash)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum RankingSource {
    Tiobe,
    Pypl,
    Languish,
}

impl RankingSource {
    /// Возвращает стабильное строковое имя источника.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tiobe => "tiobe",
            Self::Pypl => "pypl",
            Self::Languish => "languish",
        }
    }
}

impl fmt::Display for RankingSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Нормализованный набор записей из одного источника.
#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct RankingDataset {
    source: RankingSource,
    entries: Vec<RankingEntry>,
}

impl RankingDataset {
    pub(crate) const fn new(source: RankingSource, entries: Vec<RankingEntry>) -> Self {
        Self { source, entries }
    }

    /// Возвращает источник набора.
    #[must_use]
    pub const fn source(&self) -> RankingSource {
        self.source
    }

    /// Возвращает записи набора.
    #[must_use]
    pub fn entries(&self) -> &[RankingEntry] {
        &self.entries
    }

    /// Возвращает итератор по записям.
    pub fn iter(&self) -> std::slice::Iter<'_, RankingEntry> {
        self.entries.iter()
    }

    /// Возвращает количество записей.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Проверяет, что набор не содержит записей.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Разбирает набор на источник и записи без копирования записей.
    #[must_use]
    pub fn into_parts(self) -> (RankingSource, Vec<RankingEntry>) {
        (self.source, self.entries)
    }
}

impl IntoIterator for RankingDataset {
    type Item = RankingEntry;
    type IntoIter = std::vec::IntoIter<RankingEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<'a> IntoIterator for &'a RankingDataset {
    type Item = &'a RankingEntry;
    type IntoIter = std::slice::Iter<'a, RankingEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Разделяет совокупную запись PYPL `C/C++` пропорционально долям TIOBE.
///
/// Остальные записи не изменяются. Если для расчёта недостаточно данных,
/// исходная запись `C/C++` сохраняется.
pub fn reconcile_pypl_with_tiobe(tiobe: &[RankingEntry], pypl: &mut Vec<RankingEntry>) {
    let Some(c_data) = tiobe.iter().find(|entry| entry.lang == "C") else {
        return;
    };
    let Some(cpp_data) = tiobe.iter().find(|entry| entry.lang == "C++") else {
        return;
    };
    let Some(position) = pypl.iter().position(|entry| entry.lang == "C/C++") else {
        return;
    };

    let combined = pypl.remove(position);
    let share_sum = c_data.share + cpp_data.share;
    if !share_sum.is_finite() || share_sum <= f64::EPSILON {
        pypl.push(combined);
        pypl.sort_by(|left, right| left.lang.cmp(&right.lang));
        return;
    }

    let cpp_ratio = cpp_data.share / share_sum;
    let entries = [("C++", cpp_ratio), ("C", 1.0 - cpp_ratio)];

    for (lang, ratio) in entries {
        pypl.push(RankingEntry {
            lang: lang.to_owned(),
            rank: combined.rank,
            share: combined.share * ratio,
            trend: combined.trend.map(|value| value * ratio),
        });
    }

    pypl.sort_by(|left, right| left.lang.cmp(&right.lang));
}

#[cfg(test)]
mod tests {
    use super::{RankingDataset, RankingEntry, RankingSource, reconcile_pypl_with_tiobe};

    fn entry(lang: &str, share: f64, trend: Option<f64>) -> RankingEntry {
        RankingEntry {
            lang: lang.to_owned(),
            rank: Some(1),
            share,
            trend,
        }
    }

    #[test]
    fn splits_combined_pypl_entry_using_tiobe_shares() {
        let tiobe = [entry("C", 6.0, None), entry("C++", 4.0, None)];
        let mut pypl = vec![entry("Rust", 10.0, None), entry("C/C++", 5.0, Some(2.0))];

        reconcile_pypl_with_tiobe(&tiobe, &mut pypl);

        assert_eq!(pypl.len(), 3);
        assert_eq!(pypl[0], entry("C", 3.0, Some(1.2)));
        assert_eq!(pypl[1], entry("C++", 2.0, Some(0.8)));
        assert_eq!(pypl[2], entry("Rust", 10.0, None));
    }

    #[test]
    fn retains_combined_entry_when_tiobe_shares_are_zero() {
        let tiobe = [entry("C", 0.0, None), entry("C++", 0.0, None)];
        let combined = entry("C/C++", 5.0, Some(2.0));
        let mut pypl = vec![combined.clone()];

        reconcile_pypl_with_tiobe(&tiobe, &mut pypl);

        assert_eq!(pypl, [combined]);
    }

    #[test]
    fn dataset_supports_borrowed_and_owned_iteration() {
        let dataset = RankingDataset::new(
            RankingSource::Tiobe,
            vec![entry("Rust", 1.0, None), entry("Go", 0.5, None)],
        );

        assert_eq!(dataset.source().to_string(), "tiobe");
        assert_eq!(dataset.len(), 2);
        assert!(!dataset.is_empty());
        assert_eq!(
            dataset
                .iter()
                .map(|entry| entry.lang.as_str())
                .collect::<Vec<_>>(),
            ["Rust", "Go"]
        );
        assert_eq!((&dataset).into_iter().count(), 2);
        assert_eq!(dataset.into_iter().count(), 2);
    }
}

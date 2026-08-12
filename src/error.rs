use crate::RankingSource;
use thiserror::Error;

/// Ошибка высокоуровневой загрузки рейтингов.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FetchError {
    /// Не удалось создать HTTP-клиент с настройками по умолчанию.
    #[error("failed to build HTTP client")]
    ClientBuild(#[source] reqwest::Error),

    /// Не удалось загрузить или разобрать данные источника.
    #[error("failed to fetch {ranking_source}")]
    Source {
        /// Источник, при работе с которым произошла ошибка.
        ranking_source: RankingSource,
        /// Исходная ошибка с полной цепочкой причин.
        #[source]
        error: anyhow::Error,
    },

    /// Источник вернул слишком мало записей, что обычно означает изменение формата.
    #[error(
        "{ranking_source} returned {actual} entries (expected at least {minimum}); the source format may have changed"
    )]
    TooFewEntries {
        /// Источник с подозрительно коротким ответом.
        ranking_source: RankingSource,
        /// Фактическое количество записей.
        actual: usize,
        /// Минимальное ожидаемое количество записей.
        minimum: usize,
    },
}

impl FetchError {
    pub(crate) const fn source_failure(
        ranking_source: RankingSource,
        error: anyhow::Error,
    ) -> Self {
        Self::Source {
            ranking_source,
            error,
        }
    }
}

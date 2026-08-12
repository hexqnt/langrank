use crate::{
    FetchError, RankingDataset, RankingEntry, RankingSource, fetch_languish, fetch_pypl,
    fetch_tiobe, reconcile_pypl_with_tiobe,
};
use reqwest::Client;
use std::time::Duration;

const HTTP_TIMEOUT: Duration = Duration::from_secs(20);
const USER_AGENT: &str = "lang-rank-fetcher/0.1";

/// Минимальное правдоподобное количество записей рейтингового источника.
pub const MIN_RANKING_ENTRIES: usize = 10;

/// Клиент для загрузки рейтингов популярности.
#[derive(Debug, Clone)]
pub struct Fetcher {
    client: Client,
}

impl Fetcher {
    /// Создаёт загрузчик с рекомендуемыми HTTP-настройками.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку, если не удалось создать HTTP-клиент.
    pub fn new() -> Result<Self, FetchError> {
        let client = Client::builder()
            .user_agent(USER_AGENT)
            .timeout(HTTP_TIMEOUT)
            .build()
            .map_err(FetchError::ClientBuild)?;
        Ok(Self { client })
    }

    /// Создаёт загрузчик поверх пользовательского HTTP-клиента.
    #[must_use]
    pub const fn from_client(client: Client) -> Self {
        Self { client }
    }

    /// Возвращает HTTP-клиент для низкоуровневых функций загрузки.
    #[must_use]
    pub const fn client(&self) -> &Client {
        &self.client
    }

    /// Загружает один рейтинг без межисточниковых преобразований.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку при сбое HTTP-запроса, разбора ответа или если
    /// источник вернул подозрительно мало записей.
    pub async fn fetch(&self, source: RankingSource) -> Result<RankingDataset, FetchError> {
        let entries = fetch_source(&self.client, source).await?;
        ensure_min_entries(source, &entries)?;
        Ok(RankingDataset::new(source, entries))
    }

    /// Параллельно загружает TIOBE, PYPL и Languish.
    ///
    /// Наборы возвращаются в порядке TIOBE, PYPL, Languish. Фиксированный
    /// массив не требует отдельной heap-аллокации для контейнера результата.
    ///
    /// В итоговом наборе PYPL совокупная запись `C/C++` согласуется с
    /// отдельными долями C и C++ из TIOBE.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку при сбое загрузки, разбора ответа или если один из
    /// источников вернул подозрительно мало записей.
    pub async fn fetch_rankings(&self) -> Result<[RankingDataset; 3], FetchError> {
        let (tiobe, mut pypl, languish) = tokio::try_join!(
            fetch_source(&self.client, RankingSource::Tiobe),
            fetch_source(&self.client, RankingSource::Pypl),
            fetch_source(&self.client, RankingSource::Languish),
        )?;

        ensure_min_entries(RankingSource::Tiobe, &tiobe)?;
        ensure_min_entries(RankingSource::Pypl, &pypl)?;
        ensure_min_entries(RankingSource::Languish, &languish)?;
        reconcile_pypl_with_tiobe(&tiobe, &mut pypl);

        Ok([
            RankingDataset::new(RankingSource::Tiobe, tiobe),
            RankingDataset::new(RankingSource::Pypl, pypl),
            RankingDataset::new(RankingSource::Languish, languish),
        ])
    }
}

async fn fetch_source(
    client: &Client,
    source: RankingSource,
) -> Result<Vec<RankingEntry>, FetchError> {
    let result = match source {
        RankingSource::Tiobe => fetch_tiobe(client).await,
        RankingSource::Pypl => fetch_pypl(client).await,
        RankingSource::Languish => fetch_languish(client).await,
    };
    result.map_err(|error| FetchError::source_failure(source, error))
}

const fn ensure_min_entries(
    source: RankingSource,
    entries: &[RankingEntry],
) -> Result<(), FetchError> {
    if entries.len() < MIN_RANKING_ENTRIES {
        return Err(FetchError::TooFewEntries {
            ranking_source: source,
            actual: entries.len(),
            minimum: MIN_RANKING_ENTRIES,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{MIN_RANKING_ENTRIES, ensure_min_entries};
    use crate::{FetchError, RankingSource};

    #[test]
    fn reports_source_and_counts_for_short_dataset() {
        let error = ensure_min_entries(RankingSource::Pypl, &[])
            .expect_err("empty dataset should be rejected");

        assert!(matches!(
            error,
            FetchError::TooFewEntries {
                ranking_source: RankingSource::Pypl,
                actual: 0,
                minimum: MIN_RANKING_ENTRIES,
            }
        ));
    }
}

//! Загрузка и нормализация рейтингов языков программирования.
//!
//! Низкоуровневые функции принимают настроенный [`reqwest::Client`]. Для
//! большинства сценариев удобнее [`Fetcher`], который создаёт клиент с
//! подходящими значениями тайм-аута и User-Agent.
//!
//! ```no_run
//! use langrank::{FetchError, Fetcher};
//!
//! async fn load() -> Result<(), FetchError> {
//!     for dataset in Fetcher::new()?.fetch_rankings().await? {
//!         let source = dataset.source();
//!         for entry in dataset {
//!             println!("{source}: {}", entry.lang);
//!         }
//!     }
//!     Ok(())
//! }
//! ```

mod error;
mod fetcher;
mod parsing;
mod ranking;
mod sources;

pub use error::FetchError;
pub use fetcher::{Fetcher, MIN_RANKING_ENTRIES};
pub use ranking::{RankingDataset, RankingEntry, RankingSource, reconcile_pypl_with_tiobe};
pub use sources::{
    TECHEMPOWER_MAX_SCORE, download_benchmark_data, fetch_languish, fetch_pypl, fetch_techempower,
    fetch_tiobe, load_benchmark_scores,
};

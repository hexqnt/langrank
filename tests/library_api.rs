use langrank::{Fetcher, RankingEntry, RankingSource, reconcile_pypl_with_tiobe};

fn entry(lang: &str, share: f64) -> RankingEntry {
    RankingEntry {
        lang: lang.to_owned(),
        rank: Some(1),
        share,
        trend: None,
    }
}

#[test]
fn exposes_types_needed_by_a_data_consumer() {
    let fetcher = Fetcher::new().expect("HTTP client should be created");
    assert!(fetcher.client().get("https://example.com").build().is_ok());
    assert_eq!(RankingSource::Tiobe.as_str(), "tiobe");

    let tiobe = [entry("C", 3.0), entry("C++", 1.0)];
    let mut pypl = vec![entry("C/C++", 8.0)];
    reconcile_pypl_with_tiobe(&tiobe, &mut pypl);

    assert_eq!(pypl.len(), 2);
    assert_eq!(pypl[0].lang, "C");
    assert_eq!(pypl[1].lang, "C++");
}

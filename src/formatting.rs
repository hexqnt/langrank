pub fn format_trend(trend: Option<f64>) -> String {
    format_trend_with_class(trend).0
}

pub fn format_optional_rank(rank: Option<u32>) -> String {
    rank.map_or_else(|| "-".to_string(), |value| value.to_string())
}

pub fn format_optional_float(value: Option<f64>) -> String {
    match value {
        Some(v) if v.is_finite() => format!("{v:.2}"),
        _ => "-".to_string(),
    }
}

pub fn format_perf_score(
    perf_score: f64,
    benchmark: Option<f64>,
    techempower: Option<f64>,
) -> String {
    if benchmark.is_none() && techempower.is_none() {
        "-".to_string()
    } else {
        format!("{perf_score:.2}")
    }
}

pub fn format_trend_with_class(trend: Option<f64>) -> (String, &'static str) {
    trend.map_or_else(
        || ("-".to_string(), "neutral"),
        |value| {
            let normalized = normalize_trend_value(value);
            let label = format!("{normalized:+.2}");
            let class = if normalized > 0.0 {
                "up"
            } else if normalized < 0.0 {
                "down"
            } else {
                "neutral"
            };
            (label, class)
        },
    )
}

fn normalize_trend_value(value: f64) -> f64 {
    if value.abs() < 0.005 { 0.0 } else { value }
}

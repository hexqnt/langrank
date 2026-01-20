pub fn format_trend(trend: Option<f64>) -> String {
    trend.map_or_else(
        || "-".to_string(),
        |value| {
            let normalized = if value.abs() < 0.005 { 0.0 } else { value };
            format!("{normalized:+.2}")
        },
    )
}

pub fn format_perf(value: Option<f64>) -> String {
    match value {
        Some(v) if v.is_finite() => format!("{v:.2}"),
        _ => "-".to_string(),
    }
}

use crate::SchulzeRecord;
use crate::write_output_file;
use anyhow::Result;
use chrono::{DateTime, Local};
use maud::{DOCTYPE, Markup, PreEscaped, html};
use minify_html::{Cfg, minify};
use std::path::Path;

pub struct HtmlReportPaths<'a> {
    pub(crate) benchmarks: Option<&'a Path>,
    pub(crate) rankings: Option<&'a Path>,
    pub(crate) schulze: Option<&'a Path>,
}

pub struct HtmlReportContext<'a> {
    pub(crate) tiobe_count: usize,
    pub(crate) pypl_count: usize,
    pub(crate) languish_count: usize,
    pub(crate) benchmark_lang_count: usize,
    pub(crate) techempower_lang_count: usize,
    pub(crate) run_started_at: &'a DateTime<Local>,
    pub(crate) schulze_records: &'a [SchulzeRecord],
    pub(crate) full_output: bool,
    pub(crate) archive_csv: bool,
    pub(crate) paths: HtmlReportPaths<'a>,
    pub(crate) output_path: &'a Path,
}

pub async fn save_html_report(
    output_path: &Path,
    context: &HtmlReportContext<'_>,
    minify_html: bool,
) -> Result<()> {
    let html = render_html_report(context);
    if minify_html {
        let cfg = Cfg::new();
        let minified = minify(html.as_bytes(), &cfg);
        write_output_file(output_path, &minified).await
    } else {
        write_output_file(output_path, html.as_bytes()).await
    }
}

fn render_html_report(context: &HtmlReportContext<'_>) -> String {
    let generated_at = context
        .run_started_at
        .format("%Y-%m-%d %H:%M:%S %Z")
        .to_string();
    let total = context.schulze_records.len();
    let top_n = total.min(10);
    let showing = if context.full_output {
        format!("Showing all {total} languages")
    } else {
        format!("Showing top {top_n} of {total} languages")
    };
    let hint = if context.full_output {
        String::new()
    } else {
        "Run with --full-output to include the full table.".to_string()
    };
    let (table_class, table_header, table_rows) = if context.full_output {
        (
            "table-full",
            render_full_table_header(),
            render_full_table_rows(context.schulze_records),
        )
    } else {
        (
            "table-compact",
            render_compact_table_header(),
            render_compact_table_rows(context.schulze_records, top_n),
        )
    };
    let downloads = render_downloads(context);
    let table_wrap_class = if context.full_output {
        format!("table-wrap {table_class} show-shares show-trends")
    } else {
        format!("table-wrap {table_class} show-shares")
    };
    let title = format!(
        "LangRank Report - {}",
        context.run_started_at.format("%Y-%m-%d")
    );

    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) }
                meta name="description" content=(REPORT_DESCRIPTION);
                meta name="keywords" content=(REPORT_KEYWORDS);
                link rel="canonical" href=(REPORT_URL);
                meta property="og:title" content=(title);
                meta property="og:description" content=(REPORT_DESCRIPTION);
                meta property="og:type" content="website";
                meta property="og:url" content=(REPORT_URL);
                meta name="twitter:card" content="summary";
                meta name="twitter:title" content=(title);
                meta name="twitter:description" content=(REPORT_DESCRIPTION);
                link rel="icon" type="image/svg+xml" href=(REPORT_FAVICON);
                meta name="color-scheme" content="light";
                link rel="preconnect" href=(CDN_FONTS_GOOGLEAPIS);
                link rel="preconnect" href=(CDN_FONTS_GSTATIC) crossorigin;
                link rel="stylesheet" href=(CDN_FONTS_STYLESHEET);
                style { (PreEscaped(REPORT_STYLE)) }
            }
            body {
                div class="page" {
                    header class="hero" {
                        div class="hero-top" {
                            div class="pill" { "LangRank v" (env!("CARGO_PKG_VERSION")) }
                            a class="github-link"
                                href=(GITHUB_REPO_URL)
                                target="_blank"
                                rel="noopener"
                                aria-label="Open GitHub repository" {
                                    (PreEscaped(GITHUB_SVG))
                                    span { "GitHub" }
                                }
                        }
                        h1 { "LangRank Report" }
                        p class="subtitle" {
                            "Aggregated language popularity and performance ranking using the "
                            a href=(SCHULZE_METHOD_URL) target="_blank" rel="noopener noreferrer" {
                                "Schulze method"
                            }
                            "."
                        }
                        div class="meta" {
                            div {
                                span class="label" { "Generated" }
                                span class="value mono" { (generated_at) }
                            }
                            div {
                                span class="label" { "Coverage" }
                                span class="value mono" { (showing) }
                            }
                        }
                    }

                    section class="cards" {
                        div class="card" {
                            div class="card-label" { "Ranked languages" }
                            div class="card-value" { (total) }
                        }
                        div class="card" {
                            div class="card-label" { "TIOBE entries" }
                            div class="card-value" { (context.tiobe_count) }
                        }
                        div class="card" {
                            div class="card-label" { "PYPL entries" }
                            div class="card-value" { (context.pypl_count) }
                        }
                        div class="card" {
                            div class="card-label" { "Languish entries" }
                            div class="card-value" { (context.languish_count) }
                        }
                        div class="card" {
                            div class="card-label" { "Benchmarks langs" }
                            div class="card-value" { (context.benchmark_lang_count) }
                        }
                        div class="card" {
                            div class="card-label" { "TechEmpower langs" }
                            div class="card-value" { (context.techempower_lang_count) }
                        }
                    }

                    section class="table-section" {
                        div class="section-header" {
                            div {
                                h2 { "Schulze Ranking" }
                                @if !hint.is_empty() {
                                    div class="hint" { (hint) }
                                }
                            }
                            (render_table_controls(context.full_output))
                        }
                        div class=(table_wrap_class) {
                            table {
                                (table_header)
                                tbody {
                                    (table_rows)
                                }
                            }
                        }
                    }

                    (downloads)

                    footer class="footer" {
                        div {
                            "Sources: "
                            a href="https://www.tiobe.com/tiobe-index/" target="_blank" rel="noopener noreferrer" { "TIOBE" }
                            ", "
                            a href="https://pypl.github.io/PYPL.html" target="_blank" rel="noopener noreferrer" { "PYPL" }
                            ", "
                            a href="https://tjpalmer.github.io/languish/" target="_blank" rel="noopener noreferrer" { "Languish" }
                            ", "
                            a href="https://benchmarksgame-team.pages.debian.net/benchmarksgame/box-plot-summary-charts.html" target="_blank" rel="noopener noreferrer" { "Benchmarks Game" }
                            ", "
                            a href="https://www.techempower.com/benchmarks/" target="_blank" rel="noopener noreferrer" { "TechEmpower" }
                            "."
                        }
                    }
                }
                script { (PreEscaped(REPORT_SCRIPT)) }
            }
        }
    }
    .into_string()
}

fn render_full_table_header() -> Markup {
    html! {
        thead {
            tr {
                (render_sortable_header("Pos", "index", ""))
                (render_sortable_header("Language", "text", ""))
                (render_sortable_header("T Rank", "num", "col-ranks"))
                (render_sortable_header("T Share", "num", "col-shares"))
                (render_sortable_header("T Trend", "num", "col-trends"))
                (render_sortable_header("P Rank", "num", "col-ranks"))
                (render_sortable_header("P Share", "num", "col-shares"))
                (render_sortable_header("P Trend", "num", "col-trends"))
                (render_sortable_header("L Rank", "num", "col-ranks"))
                (render_sortable_header("L Share", "num", "col-shares"))
                (render_sortable_header("L Trend", "num", "col-trends"))
                (render_sortable_header("BG", "num", "col-perf-detail"))
                (render_sortable_header("TE", "num", "col-perf-detail"))
                (render_sortable_header("Perf", "num", ""))
                (render_sortable_header("Wins", "num", ""))
            }
        }
    }
}

fn render_compact_table_header() -> Markup {
    html! {
        thead {
            tr {
                (render_sortable_header("Pos", "index", ""))
                (render_sortable_header("Language", "text", ""))
                (render_sortable_header("TIOBE %", "num", "col-shares"))
                (render_sortable_header("PYPL %", "num", "col-shares"))
                (render_sortable_header("Languish %", "num", "col-shares"))
                (render_sortable_header("BG", "num", "col-perf-detail"))
                (render_sortable_header("TE", "num", "col-perf-detail"))
                (render_sortable_header("Perf", "num", ""))
                (render_sortable_header("Wins", "num", ""))
            }
        }
    }
}

fn render_sortable_header(label: &str, sort: &str, class_name: &str) -> Markup {
    let aria_label = format!("Sort by {label}");
    html! {
        th data-sort=(sort) aria-sort="none" class=(class_name) {
            button class="sort-button" type="button" aria-label=(aria_label) {
                span class="sort-label" { (label) }
                span class="sort-icon" aria-hidden="true" {}
            }
        }
    }
}

fn render_table_controls(full_output: bool) -> Markup {
    html! {
        div class="table-controls" {
            span class="control-label" { "Columns" }
            (render_group_toggle("Popularity %", "shares", true))
            @if full_output {
                (render_group_toggle("Ranks", "ranks", false))
                (render_group_toggle("Trends", "trends", true))
            }
            (render_group_toggle("Perf details", "perf-detail", false))
        }
    }
}

fn render_group_toggle(label: &str, group: &str, enabled: bool) -> Markup {
    let mut class_name = String::from("toggle");
    if enabled {
        class_name.push_str(" is-on");
    }
    html! {
        button
            class=(class_name)
            type="button"
            data-group=(group)
            aria-pressed=(if enabled { "true" } else { "false" }) {
                (label)
        }
    }
}

fn render_full_table_row(record: &SchulzeRecord) -> Markup {
    let (t_trend, t_class) = format_trend_html(record.tiobe_trend);
    let (p_trend, p_class) = format_trend_html(record.pypl_trend);
    let (l_trend, l_class) = format_trend_html(record.languish_trend);
    let perf = format_perf_combined(record);
    html! {
        tr {
            td class="num" { (record.position) }
            td class="lang" { (&record.lang) }
            td class="num col-ranks" { (format_optional_rank(record.tiobe_rank)) }
            td class="num col-shares" { (format!("{:.2}", record.tiobe_share)) }
            td class="col-trends" {
                span class=(format!("trend {t_class}")) { (t_trend) }
            }
            td class="num col-ranks" { (format_optional_rank(record.pypl_rank)) }
            td class="num col-shares" { (format!("{:.2}", record.pypl_share)) }
            td class="col-trends" {
                span class=(format!("trend {p_class}")) { (p_trend) }
            }
            td class="num col-ranks" { (format_optional_rank(record.languish_rank)) }
            td class="num col-shares" { (format!("{:.2}", record.languish_share)) }
            td class="col-trends" {
                span class=(format!("trend {l_class}")) { (l_trend) }
            }
            td class="num col-perf-detail" { (format_optional_float(record.benchmark_score)) }
            td class="num col-perf-detail" { (format_optional_float(record.techempower_score)) }
            td class="num" { (perf) }
            td class="num" { (record.schulze_wins) }
        }
    }
}

fn render_compact_table_row(record: &SchulzeRecord) -> Markup {
    let perf = format_perf_combined(record);
    html! {
        tr {
            td class="num" { (record.position) }
            td class="lang" { (&record.lang) }
            td class="num col-shares" { (format!("{:.2}", record.tiobe_share)) }
            td class="num col-shares" { (format!("{:.2}", record.pypl_share)) }
            td class="num col-shares" { (format!("{:.2}", record.languish_share)) }
            td class="num col-perf-detail" { (format_optional_float(record.benchmark_score)) }
            td class="num col-perf-detail" { (format_optional_float(record.techempower_score)) }
            td class="num" { (perf) }
            td class="num" { (record.schulze_wins) }
        }
    }
}

fn render_full_table_rows(records: &[SchulzeRecord]) -> Markup {
    html! {
        @for record in records {
            (render_full_table_row(record))
        }
    }
}

fn render_compact_table_rows(records: &[SchulzeRecord], limit: usize) -> Markup {
    html! {
        @for record in records.iter().take(limit) {
            (render_compact_table_row(record))
        }
    }
}

fn render_download_item(label: &str, path: Option<&Path>, output_path: &Path) -> Markup {
    let content = path.map_or_else(
        || html! { span class="download-path" { "Not saved" } },
        |path| {
            let full_display = path.to_string_lossy();
            let display_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_else(|| full_display.as_ref());
            relative_link(output_path, path).map_or_else(
                || {
                    html! {
                        span class="download-path" title=(full_display.as_ref()) {
                            (display_name)
                        }
                    }
                },
                |rel| {
                    html! {
                        a class="download-link" href=(rel) title=(full_display.as_ref()) {
                            (display_name)
                        }
                    }
                },
            )
        },
    );

    html! {
        div class="download-item" {
            div class="download-label" { (label) }
            (content)
        }
    }
}

fn render_downloads(context: &HtmlReportContext<'_>) -> Markup {
    let items = [
        ("Schulze CSV", context.paths.schulze),
        ("Combined CSV", context.paths.rankings),
        ("Benchmarks CSV", context.paths.benchmarks),
    ];
    let any_saved = items.iter().any(|(_, path)| path.is_some());

    html! {
        section class="downloads" {
            h3 { "Downloads" }
            @if !any_saved {
                p class="muted" {
                    "No CSV files were saved. Use --save-schulze, --save-rankings, or --save-benchmarks."
                }
            } @else {
                div class="download-list" {
                    @for (label, path) in items {
                        (render_download_item(label, path, context.output_path))
                    }
                }
            }
            @if context.archive_csv {
                p class="downloads-note muted" {
                    "Popular free tools to open .gz: "
                    a href="https://www.7-zip.org/" target="_blank" rel="noopener noreferrer" { "7-Zip" }
                    ", "
                    a href="https://apps.apple.com/us/app/the-unarchiver/id425424353" target="_blank" rel="noopener noreferrer" { "The Unarchiver" }
                    ", "
                    a href="https://www.gnu.org/software/gzip/" target="_blank" rel="noopener noreferrer" { "GNU gzip" }
                    "."
                }
            }
        }
    }
}

fn relative_link(html_path: &Path, target: &Path) -> Option<String> {
    let html_dir = html_path.parent()?;
    let target_dir = target.parent()?;
    if html_dir == target_dir {
        target
            .file_name()
            .and_then(|name| name.to_str())
            .map(std::string::ToString::to_string)
    } else {
        None
    }
}

fn format_optional_rank(rank: Option<u32>) -> String {
    rank.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn format_optional_float(value: Option<f64>) -> String {
    match value {
        Some(v) if v.is_finite() => format!("{v:.2}"),
        _ => "-".to_string(),
    }
}

fn format_perf_combined(record: &SchulzeRecord) -> String {
    if record.benchmark_score.is_none() && record.techempower_score.is_none() {
        "-".to_string()
    } else {
        format!("{:.2}", record.perf_score)
    }
}

fn format_trend_html(trend: Option<f64>) -> (String, &'static str) {
    trend.map_or_else(
        || ("-".to_string(), "neutral"),
        |value| {
            let normalized = if value.abs() < 0.005 { 0.0 } else { value };
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

const GITHUB_REPO_URL: &str = "https://github.com/hexqnt/langrank";
const SCHULZE_METHOD_URL: &str = "https://en.wikipedia.org/wiki/Schulze_method";
const CDN_FONTS_GOOGLEAPIS: &str = "https://fonts.googleapis.com";
const CDN_FONTS_GSTATIC: &str = "https://fonts.gstatic.com";
const CDN_FONTS_STYLESHEET: &str = "https://fonts.googleapis.com/css2?family=Fraunces:wght@600;700&family=JetBrains+Mono:wght@400;500&family=Manrope:wght@400;500;600&display=swap";
const REPORT_URL: &str = "https://langrank.hexq.ru/";
const REPORT_DESCRIPTION: &str = "LangRank report ranks programming languages using the Schulze method, blending popularity and performance data from major indexes.";
const REPORT_KEYWORDS: &str = "programming languages, ranking, Schulze method, TIOBE, PYPL, Languish, Benchmarks Game, TechEmpower, performance metrics";
const REPORT_FAVICON: &str = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Crect width='64' height='64' rx='14' fill='%23f6f3ec'/%3E%3Ccircle cx='32' cy='32' r='20' fill='%23e07a5f'/%3E%3Ctext x='32' y='38' text-anchor='middle' font-family='sans-serif' font-size='20' fill='%23ffffff'%3ELR%3C/text%3E%3C/svg%3E";

const REPORT_STYLE: &str = r#"
:root {
  color-scheme: light;
  --bg-top: #f6f3ec;
  --bg-bottom: #efe7db;
  --ink: #1f1b16;
  --muted: #6b635b;
  --card: #ffffff;
  --accent: #e07a5f;
  --accent-strong: #c25335;
  --accent-cool: #3d405b;
  --accent-soft: #81b29a;
  --border: #e2d6c6;
  --shadow: 0 24px 60px rgba(28, 25, 23, 0.12);
  --row-odd: #ffffff;
  --row-even: #f9f8f3;
  --row-hover: #fbefec;
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  font-family: "Manrope", "Segoe UI", sans-serif;
  color: var(--ink);
  background:
    radial-gradient(circle at top left, #ffffff 0%, transparent 45%),
    radial-gradient(circle at 20% 10%, rgba(224, 122, 95, 0.18), transparent 55%),
    linear-gradient(150deg, var(--bg-top), var(--bg-bottom));
}

.page {
  max-width: 1200px;
  margin: 0 auto;
  padding: 48px 24px 64px;
}

.hero {
  background: linear-gradient(120deg, #ffffff, #fdf4ef);
  border: 1px solid var(--border);
  border-radius: 24px;
  padding: 32px 36px;
  box-shadow: var(--shadow);
  position: relative;
  overflow: hidden;
}

.hero::after {
  content: "";
  position: absolute;
  inset: auto -20% -40% auto;
  width: 320px;
  height: 320px;
  background: radial-gradient(circle, rgba(61, 64, 91, 0.25), transparent 65%);
  pointer-events: none;
}

.hero-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
}

.pill {
  display: inline-flex;
  align-items: center;
  padding: 6px 14px;
  border-radius: 999px;
  background: rgba(61, 64, 91, 0.12);
  color: var(--accent-cool);
  font-size: 13px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.github-link {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 8px 14px;
  border-radius: 999px;
  border: 1px solid rgba(61, 64, 91, 0.18);
  background: rgba(255, 255, 255, 0.75);
  color: var(--accent-cool);
  font-size: 13px;
  font-weight: 600;
  text-decoration: none;
  letter-spacing: 0.02em;
  backdrop-filter: blur(6px);
}

.github-link svg {
  width: 18px;
  height: 18px;
  fill: currentColor;
}

.github-link:hover {
  color: var(--accent-strong);
  border-color: rgba(192, 83, 53, 0.35);
}

h1 {
  font-family: "Fraunces", "Georgia", serif;
  font-size: clamp(2.4rem, 4vw, 3.2rem);
  margin: 16px 0 8px;
}

.subtitle {
  margin: 0 0 16px;
  color: var(--muted);
  max-width: 680px;
  line-height: 1.5;
}

.subtitle a {
  color: inherit;
  font-weight: 600;
  text-decoration: underline;
  text-decoration-color: rgba(224, 122, 95, 0.6);
  text-decoration-thickness: 2px;
  text-underline-offset: 3px;
}

.subtitle a:hover {
  color: var(--accent-strong);
  text-decoration-color: var(--accent-strong);
}

.meta {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 12px;
}

.label {
  display: block;
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--muted);
  margin-bottom: 4px;
}

.value {
  font-weight: 600;
}

.mono {
  font-family: "JetBrains Mono", "SFMono-Regular", ui-monospace, monospace;
}

.cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 16px;
  margin: 28px 0;
}

.card {
  background: var(--card);
  border-radius: 18px;
  padding: 18px 20px;
  border: 1px solid var(--border);
  box-shadow: 0 16px 40px rgba(34, 30, 24, 0.08);
}

.card-label {
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--muted);
  margin-bottom: 8px;
}

.card-value {
  font-size: 26px;
  font-weight: 600;
  color: var(--accent-cool);
}

.table-section {
  margin: 32px 0 24px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  gap: 16px;
  flex-wrap: wrap;
  margin-bottom: 16px;
}

.section-header h2 {
  margin: 0 0 6px;
  font-family: "Fraunces", "Georgia", serif;
  font-size: 1.8rem;
}

.hint {
  color: var(--muted);
  font-size: 13px;
}

.table-controls {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.control-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--muted);
  margin-right: 4px;
}

.toggle {
  border: 1px solid rgba(61, 64, 91, 0.25);
  background: rgba(255, 255, 255, 0.8);
  color: var(--muted);
  padding: 6px 12px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.02em;
  cursor: pointer;
  transition: background 0.2s ease, border-color 0.2s ease, color 0.2s ease;
}

.toggle.is-on {
  background: rgba(61, 64, 91, 0.12);
  border-color: rgba(61, 64, 91, 0.45);
  color: var(--accent-cool);
}

.toggle:focus-visible {
  outline: 2px solid rgba(61, 64, 91, 0.4);
  outline-offset: 2px;
}

.table-wrap {
  border-radius: 20px;
  overflow: auto;
  border: 1px solid var(--border);
  background: var(--card);
  box-shadow: var(--shadow);
  position: relative;
  --table-min-width: 980px;
  --sticky-col-2-left: 64px;
}


.table-wrap.table-full {
  max-height: 70vh;
}

.table-wrap.table-compact {
  --table-min-width: 720px;
}

.table-wrap.table-full:not(.show-ranks):not(.show-trends):not(.show-perf-detail) {
  --table-min-width: 720px;
}

.table-wrap.table-compact:not(.show-perf-detail) {
  --table-min-width: 640px;
}


table {
  width: 100%;
  border-collapse: collapse;
  min-width: var(--table-min-width);
}

thead th {
  position: sticky;
  top: 0;
  background: var(--accent-cool);
  color: #f8fafc;
  text-align: left;
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  padding: 0;
  z-index: 2;
}

thead th:first-child,
thead th:nth-child(2) {
  left: 0;
  z-index: 6;
}

thead th:nth-child(2) {
  left: var(--sticky-col-2-left);
}

thead th.is-active {
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.18), rgba(255, 255, 255, 0.05)),
    var(--accent-cool);
}

.sort-button {
  width: 100%;
  display: inline-flex;
  align-items: center;
  justify-content: flex-start;
  gap: 8px;
  padding: 14px 16px;
  background: transparent;
  border: none;
  color: inherit;
  text-align: left;
  font: inherit;
  text-transform: inherit;
  letter-spacing: inherit;
  cursor: pointer;
  white-space: nowrap;
}

.sort-button:focus-visible {
  outline: 2px solid rgba(255, 255, 255, 0.7);
  outline-offset: -2px;
  border-radius: 8px;
}

.sort-icon {
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 7px solid rgba(248, 250, 252, 0.7);
  transition: transform 0.2s ease, border-top-color 0.2s ease;
}

thead th.is-active .sort-icon {
  border-top-color: #ffffff;
}

thead th.is-asc .sort-icon {
  transform: rotate(180deg);
}

.col-shares,
.col-ranks,
.col-trends,
.col-perf-detail {
  display: none;
}

.table-wrap.show-shares .col-shares,
.table-wrap.show-ranks .col-ranks,
.table-wrap.show-trends .col-trends,
.table-wrap.show-perf-detail .col-perf-detail {
  display: table-cell;
}

tbody td {
  padding: 12px 16px;
  border-bottom: 1px solid rgba(226, 214, 198, 0.6);
  font-size: 14px;
}

tbody tr {
  background-color: var(--row-odd);
}

tbody tr:nth-child(even) {
  background-color: var(--row-even);
}

tbody tr:hover {
  background-color: var(--row-hover);
}

.table-wrap tbody td:first-child,
.table-wrap tbody td:nth-child(2) {
  position: sticky;
  background: inherit;
  z-index: 2;
}

.table-wrap tbody td:first-child {
  left: 0;
}

.table-wrap tbody td:nth-child(2) {
  left: var(--sticky-col-2-left);
  z-index: 3;
}


.num {
  text-align: right;
  font-variant-numeric: tabular-nums;
  font-family: "JetBrains Mono", "SFMono-Regular", ui-monospace, monospace;
}

.lang {
  font-weight: 600;
}

.trend {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
  min-width: 52px;
}

.trend.up {
  background: rgba(129, 178, 154, 0.2);
  color: #2f6f54;
}

.trend.down {
  background: rgba(224, 122, 95, 0.22);
  color: #8b2d17;
}

.trend.neutral {
  background: rgba(61, 64, 91, 0.12);
  color: var(--accent-cool);
}

.downloads {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 18px;
  padding: 20px 24px;
  box-shadow: 0 16px 40px rgba(34, 30, 24, 0.08);
}

.downloads h3 {
  margin: 0 0 12px;
  font-family: "Fraunces", "Georgia", serif;
  font-size: 1.4rem;
}

.download-list {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 12px;
}

.download-item {
  padding: 12px 14px;
  border-radius: 12px;
  border: 1px solid rgba(226, 214, 198, 0.7);
  background: rgba(246, 243, 236, 0.6);
}

.download-label {
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--muted);
  margin-bottom: 6px;
}

.download-link,
.download-path {
  color: var(--accent-strong);
  font-weight: 600;
  text-decoration: none;
  word-break: break-all;
}

.download-link:hover {
  text-decoration: underline;
}

.downloads-note {
  margin-top: 12px;
  font-size: 12px;
  line-height: 1.4;
}

.downloads-note a {
  color: var(--accent-strong);
  font-weight: 600;
  text-decoration: none;
}

.downloads-note a:hover {
  text-decoration: underline;
}

.muted {
  color: var(--muted);
}

.footer {
  margin-top: 28px;
  color: var(--muted);
  font-size: 13px;
  text-align: center;
}

.footer a {
  color: inherit;
  font-weight: 600;
  text-decoration: none;
  border-bottom: 1px solid rgba(61, 64, 91, 0.28);
  transition: color 0.2s ease, border-color 0.2s ease;
}

.footer a:hover {
  color: var(--accent-strong);
  border-color: var(--accent-strong);
}

@media (max-width: 720px) {
  .page {
    padding: 32px 16px 48px;
  }

  .hero {
    padding: 24px;
  }

  .hero-top {
    align-items: flex-start;
  }

  .section-header {
    align-items: flex-start;
  }
}
"#;

const REPORT_SCRIPT: &str = r#"
(() => {
  const wrap = document.querySelector(".table-wrap");
  if (!wrap) return;

  const table = wrap.querySelector("table");
  if (!table) return;

  const tbody = table.querySelector("tbody");
  if (!tbody) return;

  const rows = Array.from(tbody.querySelectorAll("tr"));
  rows.forEach((row, index) => {
    row.dataset.index = String(index);
  });

  const headers = Array.from(table.querySelectorAll("thead th[data-sort]"));

  const updateStickyOffsets = () => {
    const headRow = table.querySelector("thead tr");
    if (!headRow || headRow.children.length < 2) return;
    const firstWidth = headRow.children[0].getBoundingClientRect().width;
    const secondWidth = headRow.children[1].getBoundingClientRect().width;
    if (!Number.isFinite(firstWidth) || !Number.isFinite(secondWidth)) return;
    wrap.style.setProperty("--sticky-col-2-left", `${firstWidth}px`);
    wrap.style.setProperty("--sticky-cols-width", `${firstWidth + secondWidth}px`);
  };

  const setToggleState = (button, isOn) => {
    button.classList.toggle("is-on", isOn);
    button.setAttribute("aria-pressed", isOn ? "true" : "false");
  };

  const toggles = Array.from(document.querySelectorAll(".table-controls [data-group]"));
  toggles.forEach((button) => {
    const group = button.dataset.group;
    if (!group) return;
    const className = `show-${group}`;
    setToggleState(button, wrap.classList.contains(className));
    button.addEventListener("click", () => {
      const isOn = !wrap.classList.contains(className);
      wrap.classList.toggle(className, isOn);
      setToggleState(button, isOn);
      updateStickyOffsets();
    });
  });

  const parseNumber = (value) => {
    const cleaned = value.replace(/[%\s,]/g, "");
    if (!cleaned || cleaned === "-") return Number.NaN;
    const num = Number(cleaned);
    return Number.isFinite(num) ? num : Number.NaN;
  };

  const compareNumbers = (aVal, bVal, dir) => {
    const aInvalid = Number.isNaN(aVal);
    const bInvalid = Number.isNaN(bVal);
    if (aInvalid && bInvalid) return 0;
    if (aInvalid) return 1;
    if (bInvalid) return -1;
    return dir === "asc" ? aVal - bVal : bVal - aVal;
  };

  const compareText = (aVal, bVal, dir) => {
    const cmp = aVal.localeCompare(bVal, undefined, {
      numeric: true,
      sensitivity: "base",
    });
    return dir === "asc" ? cmp : -cmp;
  };

  const setActive = (activeTh, dir) => {
    headers.forEach((th) => {
      th.classList.remove("is-active", "is-asc", "is-desc");
      th.setAttribute("aria-sort", "none");
    });
    activeTh.classList.add("is-active");
    activeTh.classList.add(dir === "asc" ? "is-asc" : "is-desc");
    activeTh.setAttribute("aria-sort", dir === "asc" ? "ascending" : "descending");
  };

  const getCellText = (row, index) => {
    const cell = row.children[index];
    if (!cell) return "";
    return cell.textContent.trim();
  };

  if (headers.length > 0) {
    headers.forEach((th, index) => {
      const button = th.querySelector("button.sort-button");
      if (!button) return;
      button.addEventListener("click", () => {
        const sortType = th.dataset.sort;
        let dir = table.dataset.sortDir === "asc" ? "desc" : "asc";
        if (table.dataset.sortIndex !== String(index)) {
          dir = "asc";
        }
        if (sortType === "index") {
          dir = "asc";
        }

        table.dataset.sortIndex = String(index);
        table.dataset.sortDir = dir;
        setActive(th, dir);

        const sorted = rows.slice().sort((a, b) => {
          const aIndex = Number(a.dataset.index);
          const bIndex = Number(b.dataset.index);
          if (sortType === "index") {
            return aIndex - bIndex;
          }

          const aText = getCellText(a, index);
          const bText = getCellText(b, index);

          let cmp = 0;
          if (sortType === "num") {
            const aVal = parseNumber(aText);
            const bVal = parseNumber(bText);
            cmp = compareNumbers(aVal, bVal, dir);
          } else {
            cmp = compareText(aText, bText, dir);
          }

          if (cmp !== 0) return cmp;
          return aIndex - bIndex;
        });

        const fragment = document.createDocumentFragment();
        sorted.forEach((row) => fragment.appendChild(row));
        tbody.appendChild(fragment);
      });
    });
  }

  window.addEventListener("resize", () => {
    updateStickyOffsets();
  });

  updateStickyOffsets();
})();
"#;

const GITHUB_SVG: &str = r#"<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M12 2C6.48 2 2 6.58 2 12.26c0 4.53 2.87 8.38 6.84 9.74.5.1.68-.22.68-.48 0-.24-.01-.86-.01-1.7-2.78.62-3.37-1.38-3.37-1.38-.45-1.18-1.1-1.5-1.1-1.5-.9-.64.07-.63.07-.63 1 .07 1.52 1.05 1.52 1.05.9 1.57 2.36 1.12 2.94.86.09-.67.35-1.12.63-1.38-2.22-.26-4.56-1.14-4.56-5.07 0-1.12.39-2.04 1.03-2.76-.1-.26-.45-1.3.1-2.72 0 0 .84-.27 2.75 1.03a9.28 9.28 0 0 1 2.5-.35c.85 0 1.7.12 2.5.35 1.9-1.3 2.74-1.03 2.74-1.03.56 1.42.2 2.46.1 2.72.64.72 1.03 1.64 1.03 2.76 0 3.94-2.34 4.8-4.57 5.06.36.32.68.95.68 1.92 0 1.38-.01 2.49-.01 2.83 0 .26.18.58.69.48A10.07 10.07 0 0 0 22 12.26C22 6.58 17.52 2 12 2z"/></svg>"#;

use crate::SchulzeRecord;
use crate::write_output_file;
use anyhow::Result;
use chrono::{DateTime, Local};
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
    pub(crate) run_started_at: &'a DateTime<Local>,
    pub(crate) schulze_records: &'a [SchulzeRecord],
    pub(crate) full_output: bool,
    pub(crate) paths: HtmlReportPaths<'a>,
    pub(crate) output_path: &'a Path,
}

pub async fn save_html_report(output_path: &Path, context: &HtmlReportContext<'_>) -> Result<()> {
    let html = render_html_report(context);
    write_output_file(output_path, html.as_bytes()).await
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
    let title = format!(
        "LangRank Report - {}",
        context.run_started_at.format("%Y-%m-%d")
    );

    let mut html = String::new();
    html.push_str("<!doctype html>\n<html lang=\"en\">\n<head>\n");
    html.push_str("<meta charset=\"utf-8\">\n");
    html.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    html.push_str(&format!("<title>{}</title>\n", escape_html(&title)));
    html.push_str("<meta name=\"color-scheme\" content=\"light\">\n");
    html.push_str(&format!(
        "<link rel=\"preconnect\" href=\"{}\">\n",
        CDN_FONTS_GOOGLEAPIS
    ));
    html.push_str(&format!(
        "<link rel=\"preconnect\" href=\"{}\" crossorigin>\n",
        CDN_FONTS_GSTATIC
    ));
    html.push_str(&format!(
        "<link href=\"{}\" rel=\"stylesheet\">\n",
        CDN_FONTS_STYLESHEET
    ));
    html.push_str("<style>\n");
    html.push_str(REPORT_STYLE);
    html.push_str("\n</style>\n</head>\n<body>\n");
    html.push_str("<div class=\"page\">\n");
    html.push_str("<header class=\"hero\">\n");
    html.push_str("<div class=\"hero-top\">\n");
    html.push_str(&format!(
        "<div class=\"pill\">LangRank v{}</div>\n",
        env!("CARGO_PKG_VERSION")
    ));
    html.push_str(&format!(
        "<a class=\"github-link\" href=\"{}\" target=\"_blank\" rel=\"noopener\" aria-label=\"Open GitHub repository\">\n",
        GITHUB_REPO_URL
    ));
    html.push_str(GITHUB_SVG);
    html.push_str("<span>GitHub</span>\n");
    html.push_str("</a>\n");
    html.push_str("</div>\n");
    html.push_str("<h1>LangRank Report</h1>\n");
    html.push_str(&format!(
        "<p class=\"subtitle\">Aggregated language popularity and performance ranking using the <a href=\"{}\" target=\"_blank\" rel=\"noopener noreferrer\">Schulze method</a>.</p>\n",
        SCHULZE_METHOD_URL
    ));
    html.push_str("<div class=\"meta\">\n");
    html.push_str(&format!(
        "<div><span class=\"label\">Generated</span><span class=\"value mono\">{}</span></div>\n",
        escape_html(&generated_at)
    ));
    html.push_str(&format!(
        "<div><span class=\"label\">Coverage</span><span class=\"value mono\">{}</span></div>\n",
        escape_html(&showing)
    ));
    html.push_str("</div>\n");
    html.push_str("</header>\n");

    html.push_str("<section class=\"cards\">\n");
    html.push_str(&format!(
        "<div class=\"card\"><div class=\"card-label\">Ranked languages</div><div class=\"card-value\">{}</div></div>\n",
        total
    ));
    html.push_str(&format!(
        "<div class=\"card\"><div class=\"card-label\">TIOBE entries</div><div class=\"card-value\">{}</div></div>\n",
        context.tiobe_count
    ));
    html.push_str(&format!(
        "<div class=\"card\"><div class=\"card-label\">PYPL entries</div><div class=\"card-value\">{}</div></div>\n",
        context.pypl_count
    ));
    html.push_str(&format!(
        "<div class=\"card\"><div class=\"card-label\">Languish entries</div><div class=\"card-value\">{}</div></div>\n",
        context.languish_count
    ));
    html.push_str(&format!(
        "<div class=\"card\"><div class=\"card-label\">Benchmarks langs</div><div class=\"card-value\">{}</div></div>\n",
        context.benchmark_lang_count
    ));
    html.push_str("</section>\n");

    html.push_str("<section class=\"table-section\">\n");
    html.push_str("<div class=\"section-header\">\n");
    html.push_str("<div>\n");
    html.push_str("<h2>Schulze Ranking</h2>\n");
    if !hint.is_empty() {
        html.push_str(&format!(
            "<div class=\"hint\">{}</div>\n",
            escape_html(&hint)
        ));
    }
    html.push_str("</div>\n");
    html.push_str("</div>\n");
    html.push_str(&format!(
        "<div class=\"table-wrap {table_class}\">\n<table>\n"
    ));
    html.push_str(&table_header);
    html.push_str("<tbody>\n");
    html.push_str(&table_rows);
    html.push_str("</tbody>\n</table>\n</div>\n</section>\n");

    html.push_str(&downloads);

    html.push_str("<footer class=\"footer\">\n");
    html.push_str("<div>Sources: TIOBE, PYPL, Languish, Benchmarks Game.</div>\n");
    html.push_str("</footer>\n");
    html.push_str("</div>\n</body>\n</html>\n");
    html
}

fn render_full_table_header() -> String {
    let mut header = String::new();
    header.push_str("<thead><tr>");
    header.push_str("<th>Pos</th>");
    header.push_str("<th>Language</th>");
    header.push_str("<th>T Rank</th>");
    header.push_str("<th>T Share</th>");
    header.push_str("<th>T Trend</th>");
    header.push_str("<th>P Rank</th>");
    header.push_str("<th>P Share</th>");
    header.push_str("<th>P Trend</th>");
    header.push_str("<th>L Rank</th>");
    header.push_str("<th>L Share</th>");
    header.push_str("<th>L Trend</th>");
    header.push_str("<th>Perf (rel)</th>");
    header.push_str("<th>Wins</th>");
    header.push_str("</tr></thead>\n");
    header
}

fn render_compact_table_header() -> String {
    let mut header = String::new();
    header.push_str("<thead><tr>");
    header.push_str("<th>Pos</th>");
    header.push_str("<th>Language</th>");
    header.push_str("<th>TIOBE %</th>");
    header.push_str("<th>PYPL %</th>");
    header.push_str("<th>Languish %</th>");
    header.push_str("<th>Perf (rel)</th>");
    header.push_str("<th>Wins</th>");
    header.push_str("</tr></thead>\n");
    header
}

fn render_full_table_rows(records: &[SchulzeRecord]) -> String {
    let mut rows = String::new();
    for record in records {
        let (t_trend, t_class) = format_trend_html(record.tiobe_trend);
        let (p_trend, p_class) = format_trend_html(record.pypl_trend);
        let (l_trend, l_class) = format_trend_html(record.languish_trend);
        rows.push_str("<tr>");
        rows.push_str(&format!("<td class=\"num\">{}</td>", record.position));
        rows.push_str(&format!(
            "<td class=\"lang\">{}</td>",
            escape_html(&record.lang)
        ));
        rows.push_str(&format!(
            "<td class=\"num\">{}</td>",
            format_optional_rank(record.tiobe_rank)
        ));
        rows.push_str(&format!("<td class=\"num\">{:.2}</td>", record.tiobe_share));
        rows.push_str(&format!(
            "<td><span class=\"trend {t_class}\">{t_trend}</span></td>"
        ));
        rows.push_str(&format!(
            "<td class=\"num\">{}</td>",
            format_optional_rank(record.pypl_rank)
        ));
        rows.push_str(&format!("<td class=\"num\">{:.2}</td>", record.pypl_share));
        rows.push_str(&format!(
            "<td><span class=\"trend {p_class}\">{p_trend}</span></td>"
        ));
        rows.push_str(&format!(
            "<td class=\"num\">{}</td>",
            format_optional_rank(record.languish_rank)
        ));
        rows.push_str(&format!(
            "<td class=\"num\">{:.2}</td>",
            record.languish_share
        ));
        rows.push_str(&format!(
            "<td><span class=\"trend {l_class}\">{l_trend}</span></td>"
        ));
        rows.push_str(&format!(
            "<td class=\"num\">{}</td>",
            format_optional_float(record.benchmark_score)
        ));

        rows.push_str(&format!("<td class=\"num\">{}</td>", record.schulze_wins));
        rows.push_str("</tr>\n");
    }
    rows
}

fn render_compact_table_rows(records: &[SchulzeRecord], limit: usize) -> String {
    let mut rows = String::new();
    for record in records.iter().take(limit) {
        rows.push_str("<tr>");
        rows.push_str(&format!("<td class=\"num\">{}</td>", record.position));
        rows.push_str(&format!(
            "<td class=\"lang\">{}</td>",
            escape_html(&record.lang)
        ));
        rows.push_str(&format!("<td class=\"num\">{:.2}</td>", record.tiobe_share));
        rows.push_str(&format!("<td class=\"num\">{:.2}</td>", record.pypl_share));
        rows.push_str(&format!(
            "<td class=\"num\">{:.2}</td>",
            record.languish_share
        ));
        rows.push_str(&format!(
            "<td class=\"num\">{}</td>",
            format_optional_float(record.benchmark_score)
        ));
        rows.push_str(&format!("<td class=\"num\">{}</td>", record.schulze_wins));
        rows.push_str("</tr>\n");
    }
    rows
}

fn render_downloads(context: &HtmlReportContext<'_>) -> String {
    let items = [
        ("Schulze CSV", context.paths.schulze),
        ("Combined CSV", context.paths.rankings),
        ("Benchmarks CSV", context.paths.benchmarks),
    ];
    let mut any_saved = false;
    for (_, path) in &items {
        if path.is_some() {
            any_saved = true;
            break;
        }
    }

    let mut section = String::new();
    section.push_str("<section class=\"downloads\">\n");
    section.push_str("<h3>Downloads</h3>\n");
    if !any_saved {
        section.push_str("<p class=\"muted\">No CSV files were saved. Use --save-schulze, --save-rankings, or --save-benchmarks.</p>\n");
        section.push_str("</section>\n");
        return section;
    }

    section.push_str("<div class=\"download-list\">\n");
    for (label, path) in items {
        section.push_str("<div class=\"download-item\">\n");
        section.push_str(&format!(
            "<div class=\"download-label\">{}</div>\n",
            escape_html(label)
        ));
        if let Some(path) = path {
            let full_display = path.to_string_lossy();
            let display_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(full_display.as_ref());
            if let Some(rel) = relative_link(context.output_path, path) {
                section.push_str(&format!(
                    "<a class=\"download-link\" href=\"{}\" title=\"{}\">{}</a>\n",
                    escape_html(&rel),
                    escape_html(full_display.as_ref()),
                    escape_html(display_name)
                ));
            } else {
                section.push_str(&format!(
                    "<span class=\"download-path\" title=\"{}\">{}</span>\n",
                    escape_html(full_display.as_ref()),
                    escape_html(display_name)
                ));
            }
        } else {
            section.push_str("<span class=\"download-path\">Not saved</span>\n");
        }
        section.push_str("</div>\n");
    }
    section.push_str("</div>\n</section>\n");
    section
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

fn escape_html(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

const GITHUB_REPO_URL: &str = "https://github.com/hexqnt/langrank";
const SCHULZE_METHOD_URL: &str = "https://en.wikipedia.org/wiki/Schulze_method";
const CDN_FONTS_GOOGLEAPIS: &str = "https://fonts.googleapis.com";
const CDN_FONTS_GSTATIC: &str = "https://fonts.gstatic.com";
const CDN_FONTS_STYLESHEET: &str = "https://fonts.googleapis.com/css2?family=Fraunces:wght@600;700&family=JetBrains+Mono:wght@400;500&family=Manrope:wght@400;500;600&display=swap";

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

.table-wrap {
  border-radius: 20px;
  overflow: auto;
  border: 1px solid var(--border);
  background: var(--card);
  box-shadow: var(--shadow);
}

.table-wrap.table-full {
  max-height: 70vh;
}

table {
  width: 100%;
  border-collapse: collapse;
  min-width: 980px;
}

.table-wrap.table-compact table {
  min-width: 720px;
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
  padding: 14px 16px;
  z-index: 2;
}

tbody td {
  padding: 12px 16px;
  border-bottom: 1px solid rgba(226, 214, 198, 0.6);
  font-size: 14px;
}

tbody tr:nth-child(even) {
  background: rgba(246, 243, 236, 0.6);
}

tbody tr:hover {
  background: rgba(224, 122, 95, 0.12);
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

.muted {
  color: var(--muted);
}

.footer {
  margin-top: 28px;
  color: var(--muted);
  font-size: 13px;
  text-align: center;
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

  table {
    min-width: 720px;
  }
}
"#;

const GITHUB_SVG: &str = r#"<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M12 2C6.48 2 2 6.58 2 12.26c0 4.53 2.87 8.38 6.84 9.74.5.1.68-.22.68-.48 0-.24-.01-.86-.01-1.7-2.78.62-3.37-1.38-3.37-1.38-.45-1.18-1.1-1.5-1.1-1.5-.9-.64.07-.63.07-.63 1 .07 1.52 1.05 1.52 1.05.9 1.57 2.36 1.12 2.94.86.09-.67.35-1.12.63-1.38-2.22-.26-4.56-1.14-4.56-5.07 0-1.12.39-2.04 1.03-2.76-.1-.26-.45-1.3.1-2.72 0 0 .84-.27 2.75 1.03a9.28 9.28 0 0 1 2.5-.35c.85 0 1.7.12 2.5.35 1.9-1.3 2.74-1.03 2.74-1.03.56 1.42.2 2.46.1 2.72.64.72 1.03 1.64 1.03 2.76 0 3.94-2.34 4.8-4.57 5.06.36.32.68.95.68 1.92 0 1.38-.01 2.49-.01 2.83 0 .26.18.58.69.48A10.07 10.07 0 0 0 22 12.26C22 6.58 17.52 2 12 2z"/></svg>"#;

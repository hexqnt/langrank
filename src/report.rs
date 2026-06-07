use crate::formatting::{
    format_optional_float, format_optional_rank, format_perf_score, format_trend_with_class,
};
use crate::schulze::SchulzeRecord;
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

#[allow(clippy::too_many_lines)]
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
                meta name="color-scheme" content="light dark";
                link rel="preconnect" href=(CDN_FONTS_GOOGLEAPIS);
                link rel="preconnect" href=(CDN_FONTS_GSTATIC) crossorigin;
                link rel="stylesheet" href=(CDN_FONTS_STYLESHEET);
                script { (PreEscaped(THEME_BOOTSTRAP_SCRIPT)) }
                style { (PreEscaped(REPORT_STYLE)) }
            }
            body {
                div class="page" {
                    header class="hero" {
                        div class="hero-top" {
                            div class="pill" { "LangRank v" (env!("CARGO_PKG_VERSION")) }
                            div class="hero-actions" {
                                button
                                    class="hero-action theme-toggle"
                                    type="button"
                                    data-theme-toggle
                                    aria-label="Switch to dark theme"
                                    aria-pressed="false" {
                                        span class="theme-toggle-icon" aria-hidden="true" {}
                                        span data-theme-toggle-label { "Dark" }
                                }
                                a class="hero-action github-link"
                                    href=(GITHUB_REPO_URL)
                                    target="_blank"
                                    rel="noopener"
                                    aria-label="Open GitHub repository" {
                                        (PreEscaped(GITHUB_SVG))
                                        span { "GitHub" }
                                    }
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
    let (t_trend, t_class) = format_trend_with_class(record.tiobe_trend);
    let (p_trend, p_class) = format_trend_with_class(record.pypl_trend);
    let (l_trend, l_class) = format_trend_with_class(record.languish_trend);
    let perf = format_perf_score(
        record.perf_score,
        record.benchmark_score,
        record.techempower_score,
    );
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
    let perf = format_perf_score(
        record.perf_score,
        record.benchmark_score,
        record.techempower_score,
    );
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

const GITHUB_REPO_URL: &str = "https://github.com/hexqnt/langrank";
const SCHULZE_METHOD_URL: &str = "https://en.wikipedia.org/wiki/Schulze_method";
const CDN_FONTS_GOOGLEAPIS: &str = "https://fonts.googleapis.com";
const CDN_FONTS_GSTATIC: &str = "https://fonts.gstatic.com";
const CDN_FONTS_STYLESHEET: &str = "https://fonts.googleapis.com/css2?family=Fraunces:wght@600;700&family=JetBrains+Mono:wght@400;500&family=Manrope:wght@400;500;600&display=swap";
const REPORT_URL: &str = "https://langrank.hexq.ru/";
const REPORT_DESCRIPTION: &str = "LangRank report ranks programming languages using the Schulze method, blending popularity and performance data from major indexes.";
const REPORT_KEYWORDS: &str = "programming languages, ranking, Schulze method, TIOBE, PYPL, Languish, Benchmarks Game, TechEmpower, performance metrics";
const REPORT_FAVICON: &str = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Crect width='64' height='64' rx='14' fill='%23f6f3ec'/%3E%3Ccircle cx='32' cy='32' r='20' fill='%23e07a5f'/%3E%3Ctext x='32' y='38' text-anchor='middle' font-family='sans-serif' font-size='20' fill='%23ffffff'%3ELR%3C/text%3E%3C/svg%3E";

const REPORT_STYLE: &str = include_str!("report/style.css");

const THEME_BOOTSTRAP_SCRIPT: &str = r#"
(() => {
  try {
    const stored = localStorage.getItem("langrank-theme");
    const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
    const theme = stored === "dark" || stored === "light"
      ? stored
      : prefersDark ? "dark" : "light";
    document.documentElement.dataset.theme = theme;
  } catch (_) {
    document.documentElement.dataset.theme = "light";
  }
})();
"#;

const REPORT_SCRIPT: &str = r#"
(() => {
  const themeToggle = document.querySelector("[data-theme-toggle]");
  const themeToggleLabel = document.querySelector("[data-theme-toggle-label]");

  const setThemeToggleState = (theme) => {
    if (!themeToggle) return;
    const isDark = theme === "dark";
    themeToggle.setAttribute("aria-pressed", isDark ? "true" : "false");
    themeToggle.setAttribute(
      "aria-label",
      isDark ? "Switch to light theme" : "Switch to dark theme",
    );
    if (themeToggleLabel) {
      themeToggleLabel.textContent = isDark ? "Light" : "Dark";
    }
  };

  const currentTheme = document.documentElement.dataset.theme === "dark" ? "dark" : "light";
  setThemeToggleState(currentTheme);

  if (themeToggle) {
    themeToggle.addEventListener("click", () => {
      const nextTheme = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
      document.documentElement.dataset.theme = nextTheme;
      try {
        localStorage.setItem("langrank-theme", nextTheme);
      } catch (_) {
        // Хранилище может быть недоступно при строгих настройках браузера.
      }
      setThemeToggleState(nextTheme);
    });
  }

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

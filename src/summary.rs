use crate::SchulzeRecord;
use crate::formatting::{format_perf, format_trend};
use chrono::{DateTime, Local};
use colored::Colorize;
use std::path::Path;

pub struct SummaryPaths<'a> {
    pub(crate) benchmarks: Option<&'a Path>,
    pub(crate) rankings: Option<&'a Path>,
    pub(crate) schulze: Option<&'a Path>,
    pub(crate) html: Option<&'a Path>,
}

pub struct SummaryContext<'a> {
    pub(crate) tiobe_count: usize,
    pub(crate) pypl_count: usize,
    pub(crate) languish_count: usize,
    pub(crate) benchmark_lang_count: usize,
    pub(crate) techempower_lang_count: usize,
    pub(crate) run_started_at: &'a DateTime<Local>,
    pub(crate) paths: SummaryPaths<'a>,
    pub(crate) schulze_records: &'a [SchulzeRecord],
    pub(crate) full_output: bool,
}

pub fn print_summary(context: &SummaryContext<'_>) {
    println!();
    print_summary_header(context);
    print_summary_paths(&context.paths);
    println!();
    println!("{}", "Schulze Ranking".bold().bright_magenta());
    let table_width = print_schulze_table(context.schulze_records, context.full_output);
    if table_width > 0 {
        let divider = "=".repeat(table_width);
        println!("{}", divider.bright_cyan());
    }
}

fn print_summary_header(context: &SummaryContext<'_>) {
    println!(
        "{}",
        "====================== LangRank Update ======================"
            .bold()
            .bright_cyan()
    );
    println!(
        "{} {}",
        "Run started".bright_yellow().bold(),
        context
            .run_started_at
            .format("%Y-%m-%d %H:%M:%S %Z")
            .to_string()
            .bright_white()
    );
    println!(
        "{} {} | {} | {} | {} | {}",
        "Sources".bright_yellow().bold(),
        format!("TIOBE: {}", context.tiobe_count).bright_white(),
        format!("PYPL: {}", context.pypl_count).bright_white(),
        format!("Languish: {}", context.languish_count).bright_white(),
        format!("Benchmarks: {}", context.benchmark_lang_count).bright_white(),
        format!("TechEmpower: {}", context.techempower_lang_count).bright_white()
    );
}

fn print_summary_paths(paths: &SummaryPaths<'_>) {
    print_path_line(
        "Benchmarks CSV",
        paths.benchmarks,
        "not saved (use --save-benchmarks)",
    );
    print_path_line(
        "Combined CSV",
        paths.rankings,
        "not saved (use --save-rankings)",
    );
    print_path_line(
        "Schulze CSV",
        paths.schulze,
        "not saved (use --save-schulze)",
    );
    print_path_line("HTML Report", paths.html, "not saved (use --save-html)");
}

fn print_path_line(label: &str, path: Option<&Path>, hint: &str) {
    let label_colored = label.bright_yellow().bold();
    match path {
        Some(path) => println!(
            "{} {}",
            label_colored,
            format!("{}", path.display()).bright_white()
        ),
        None => println!("{} {}", label_colored, hint.bright_black()),
    }
}

fn print_schulze_table(records: &[SchulzeRecord], full_output: bool) -> usize {
    if records.is_empty() {
        let message = "No Schulze data available.";
        println!("{}", message.bright_black());
        return message.len();
    }

    if full_output {
        print_full_schulze_table(records)
    } else {
        print_compact_schulze_table(records)
    }
}

fn print_full_schulze_table(records: &[SchulzeRecord]) -> usize {
    let header = format!(
        "{:>3} | {:<13} | {:>6} | {:>6} | {:>7} | {:>6} | {:>6} | {:>7} | {:>6} | {:>6} | {:>7} | {:>6} | {:>6} | {:>6} | {:>4}",
        "Pos",
        "Language",
        "T Rank",
        "T%",
        "T Trend",
        "P Rank",
        "P%",
        "P Trend",
        "L Rank",
        "L%",
        "L Trend",
        "BG",
        "TE",
        "Perf",
        "Wins"
    );
    let separator = "----+---------------+--------+--------+---------+--------+--------+---------+--------+--------+---------+------+------+------+------+";
    let mut max_width = header.len().max(separator.len());
    println!("{}", header.bold().bright_white());
    println!("{}", separator.bright_black());

    for record in records {
        let tiobe_rank = record
            .tiobe_rank
            .map_or_else(|| "-".to_string(), |value| value.to_string());
        let pypl_rank = record
            .pypl_rank
            .map_or_else(|| "-".to_string(), |value| value.to_string());
        let languish_rank = record
            .languish_rank
            .map_or_else(|| "-".to_string(), |value| value.to_string());
        let tiobe_share = format!("{:.2}", record.tiobe_share);
        let pypl_share = format!("{:.2}", record.pypl_share);
        let tiobe_trend = format_trend(record.tiobe_trend);
        let pypl_trend = format_trend(record.pypl_trend);
        let languish_share = format!("{:.2}", record.languish_share);
        let languish_trend = format_trend(record.languish_trend);
        let bg = format_perf(record.benchmark_score);
        let te = record
            .techempower_score
            .map_or_else(|| "-".to_string(), |value| format!("{value:.2}"));
        let perf = if record.benchmark_score.is_none() && record.techempower_score.is_none() {
            "-".to_string()
        } else {
            format!("{:.2}", record.perf_score)
        };
        let line = format!(
            "{:>3} | {:<13} | {:>6} | {:>6} | {:>7} | {:>6} | {:>6} | {:>7} | {:>6} | {:>6} | {:>7} | {:>6} | {:>6} | {:>6} | {:>4}",
            record.position,
            record.lang,
            tiobe_rank,
            tiobe_share,
            tiobe_trend,
            pypl_rank,
            pypl_share,
            pypl_trend,
            languish_rank,
            languish_share,
            languish_trend,
            bg,
            te,
            perf,
            record.schulze_wins
        );
        max_width = max_width.max(line.len());
        println!("{}", line.bright_green());
    }

    max_width
}

fn print_compact_schulze_table(records: &[SchulzeRecord]) -> usize {
    let header = "Pos | Language      | TIOBE% | PYPL% | LANG% | BG | TE | Perf | Wins";
    let separator = "----+---------------+--------+-------+------+----+----+------+------";
    let mut max_width = header.len().max(separator.len());
    println!("{}", header.bold().bright_white());
    println!("{}", separator.bright_black());
    for record in records.iter().take(10) {
        let bg = format_perf(record.benchmark_score);
        let te = record
            .techempower_score
            .map_or_else(|| "-".to_string(), |value| format!("{value:.2}"));
        let perf = if record.benchmark_score.is_none() && record.techempower_score.is_none() {
            "-".to_string()
        } else {
            format!("{:.2}", record.perf_score)
        };
        let line = format!(
            "{:>3} | {:<13} | {:>6.2} | {:>5.2} | {:>5.2} | {:>4} | {:>4} | {:>4} | {:>4}",
            record.position,
            record.lang,
            record.tiobe_share,
            record.pypl_share,
            record.languish_share,
            bg,
            te,
            perf,
            record.schulze_wins
        );
        max_width = max_width.max(line.len());
        println!("{}", line.bright_green());
    }
    if records.len() > 10 {
        let message = format!(
            "... {} more entries (use --full-output to display all).",
            records.len() - 10
        );
        max_width = max_width.max(message.len());
        println!("{}", message.bright_black());
    }

    max_width
}

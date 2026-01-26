use anyhow::Result;
use colored::Colorize;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::future::Future;
use std::time::Duration;

const SPINNER_TICKS_BRAILLE_COLORED: [&str; 8] = [
    "\x1b[1;96m⠁\x1b[0m",
    "\x1b[1;96m⠂\x1b[0m",
    "\x1b[1;96m⠄\x1b[0m",
    "\x1b[1;96m⡀\x1b[0m",
    "\x1b[1;96m⢀\x1b[0m",
    "\x1b[1;96m⠠\x1b[0m",
    "\x1b[1;96m⠐\x1b[0m",
    "\x1b[1;96m⠈\x1b[0m",
];

const SPINNER_TICKS_BRAILLE_PLAIN: [&str; 8] = ["⠁", "⠂", "⠄", "⡀", "⢀", "⠠", "⠐", "⠈"];
const SPINNER_TICKS_ASCII: &str = "|/-\\";

const STAGE_TOTAL: u8 = 2;
const STAGE_FETCH: &str = "Получение данных";
const STAGE_COMPUTE: &str = "Расчёт";

#[derive(Clone, Copy)]
pub enum Stage {
    Fetch,
    Compute,
}

impl Stage {
    const fn index(self) -> u8 {
        match self {
            Self::Fetch => 1,
            Self::Compute => 2,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Fetch => STAGE_FETCH,
            Self::Compute => STAGE_COMPUTE,
        }
    }
}

pub struct ProgressState {
    multi: MultiProgress,
    style: ProgressStyle,
}

impl ProgressState {
    pub(crate) fn new(use_color: bool) -> Self {
        let use_ascii = is_dumb_term();
        let multi = MultiProgress::new();
        multi.set_draw_target(ProgressDrawTarget::stderr_with_hz(15));
        let style = ProgressStyle::with_template("{spinner} {msg}").unwrap();
        let style = if use_ascii {
            style.tick_chars(SPINNER_TICKS_ASCII)
        } else if use_color {
            style.tick_strings(&SPINNER_TICKS_BRAILLE_COLORED)
        } else {
            style.tick_strings(&SPINNER_TICKS_BRAILLE_PLAIN)
        };
        Self { multi, style }
    }

    pub(crate) fn spinner(&self, message: String) -> ProgressBar {
        let bar = self.multi.add(ProgressBar::new_spinner());
        bar.set_style(self.style.clone());
        bar.set_message(message);
        bar.enable_steady_tick(Duration::from_millis(100));
        bar
    }

    pub(crate) fn clear(&self) {
        let _ = self.multi.clear();
    }
}

fn is_dumb_term() -> bool {
    std::env::var("TERM").is_ok_and(|term| term.eq_ignore_ascii_case("dumb"))
}

fn format_stage_message(stage: Stage, label: &str) -> String {
    let prefix = format!("[{}/{}]", stage.index(), STAGE_TOTAL);
    format!(
        "{} {}: {}",
        prefix.bright_yellow().bold(),
        stage.label().bright_cyan().bold(),
        label.bright_white().bold()
    )
}

pub async fn run_with_spinner<T>(
    progress: &ProgressState,
    stage: Stage,
    label: &str,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    let message = format_stage_message(stage, label);
    let bar = progress.spinner(message);
    let result = fut.await;
    match &result {
        Ok(_) => bar.finish_with_message(format!(
            "{} {}",
            format_stage_message(stage, label),
            "done".bright_green().bold()
        )),
        Err(_) => bar.finish_with_message(format!(
            "{} {}",
            format_stage_message(stage, label),
            "failed".bright_red().bold()
        )),
    }
    result
}

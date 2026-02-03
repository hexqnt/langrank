use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{Shell, generate, generate_to};

pub const DEFAULT_RANKINGS_PATH: &str = "data/input/rankings.csv";
pub const DEFAULT_BENCHMARKS_PATH: &str = "data/input/benchmarksgame.csv";
pub const DEFAULT_SCHULZE_PATH: &str = "data/output/schulze_rankings.csv";
pub const DEFAULT_HTML_PATH: &str = "data/output/report.html";

pub const SAVE_RANKINGS_HELP: &str = "Save combined TIOBE/PYPL rankings to the given CSV file (defaults to data/input/rankings.csv when no path is provided). Use --archive-csv to store a .gz instead.";
pub const SAVE_BENCHMARKS_HELP: &str = "Save the downloaded benchmark dataset to the given CSV file (defaults to data/input/benchmarksgame.csv when no path is provided). Use --archive-csv to store a .gz instead.";
pub const SAVE_SCHULZE_HELP: &str = "Save the computed Schulze ranking to the given CSV file (defaults to data/output/schulze_rankings.csv when no path is provided). Use --archive-csv to store a .gz instead.";
pub const SAVE_HTML_HELP: &str = "Save the HTML report to the given file (defaults to data/output/report.html when no path is provided).";
pub const ARCHIVE_CSV_HELP: &str = "Archive saved CSV outputs into .gz files (recommended for publishing).";

#[derive(Debug, Parser)]
#[command(
    name = "lang_rank",
    about = "Fetch language popularity indexes (TIOBE, PYPL), benchmark data, and compute a Schulze ranking.",
    version = env!("CARGO_PKG_VERSION")
)]
pub struct Cli {
    #[arg(
        long,
        value_name = "FILE",
        num_args = 0..=1,
        default_missing_value = DEFAULT_RANKINGS_PATH,
        help = SAVE_RANKINGS_HELP
    )]
    pub save_rankings: Option<PathBuf>,
    #[arg(
        long,
        value_name = "FILE",
        num_args = 0..=1,
        default_missing_value = DEFAULT_BENCHMARKS_PATH,
        help = SAVE_BENCHMARKS_HELP
    )]
    pub save_benchmarks: Option<PathBuf>,
    #[arg(
        long,
        value_name = "FILE",
        num_args = 0..=1,
        default_missing_value = DEFAULT_SCHULZE_PATH,
        help = SAVE_SCHULZE_HELP
    )]
    pub save_schulze: Option<PathBuf>,
    #[arg(
        long,
        value_name = "FILE",
        num_args = 0..=1,
        default_missing_value = DEFAULT_HTML_PATH,
        help = SAVE_HTML_HELP
    )]
    pub save_html: Option<PathBuf>,
    #[arg(long, help = ARCHIVE_CSV_HELP)]
    pub archive_csv: bool,
    #[arg(
        long,
        help = "Print the complete Schulze table with every row and column instead of the abbreviated summary."
    )]
    pub full_output: bool,
    #[arg(long, help = "Disable progress spinner output.")]
    pub no_progress: bool,
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Generate shell completion scripts, optionally installing them for the current user.
    Completions {
        #[arg(value_enum, help = "Shell to generate completions for.")]
        shell: Shell,
        #[arg(
            long,
            value_name = "DIR",
            help = "Directory to write the completion script to."
        )]
        output_dir: Option<PathBuf>,
        #[arg(
            long,
            help = "Install the completion script into the default location for the selected shell."
        )]
        install: bool,
    },
}

pub fn handle_command(command: Commands) -> Result<()> {
    match command {
        Commands::Completions {
            shell,
            output_dir,
            install,
        } => generate_completions(shell, output_dir, install),
    }
}

fn generate_completions(shell: Shell, output_dir: Option<PathBuf>, install: bool) -> Result<()> {
    let mut command = Cli::command();
    let bin_name = command.get_name().to_string();

    let target_dir = if let Some(dir) = output_dir {
        Some(dir)
    } else if install {
        Some(default_install_dir(shell)?)
    } else {
        None
    };

    if let Some(dir) = target_dir {
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create completion directory {}", dir.display()))?;
        let path = generate_to(shell, &mut command, bin_name, &dir)
            .context("failed to write completion file")?;
        println!("Installed {shell:?} completions to {}", path.display());
    } else {
        let mut stdout = io::stdout().lock();
        generate(shell, &mut command, bin_name, &mut stdout);
        stdout
            .flush()
            .context("failed to flush completion output")?;
    }

    Ok(())
}

fn default_install_dir(shell: Shell) -> Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| {
        anyhow!("HOME environment variable is not set; use --output-dir to specify a path")
    })?;
    let mut path = PathBuf::from(home);

    match shell {
        Shell::Bash => {
            path.push(".local/share/bash-completion/completions");
            Ok(path)
        }
        Shell::Elvish => {
            path.push(".elvish/lib/completions");
            Ok(path)
        }
        Shell::Fish => {
            path.push(".config/fish/completions");
            Ok(path)
        }
        Shell::PowerShell => {
            path.push(".local/share/powershell/Scripts");
            Ok(path)
        }
        Shell::Zsh => {
            path.push(".local/share/zsh/site-functions");
            Ok(path)
        }
        other => Err(anyhow!(
            "no default install location for {other:?}; specify --output-dir"
        )),
    }
}

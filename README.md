# 🌟 LangRank

[🇺🇸 English](./README.md) · [🇷🇺 Русский](./README.ru.md)

[![CI](https://github.com/hexqnt/langrank/actions/workflows/ci.yml/badge.svg)](https://github.com/hexqnt/langrank/actions/workflows/ci.yml)
[![Cloudflare Pages](https://github.com/hexqnt/langrank/actions/workflows/deploy.yml/badge.svg)](https://github.com/hexqnt/langrank/actions/workflows/deploy.yml)

LangRank is a command-line tool that combines programming-language popularity and
performance data into a single ranking using the Schulze method.

[View the latest report](https://langrank.hexq.ru/) ·
[Download LangRank](https://github.com/hexqnt/langrank/releases/latest)

![LangRank ranking visualization](img/poster.gif)

## Installation

### Prebuilt binaries

Download the archive for your system from the
[latest GitHub release](https://github.com/hexqnt/langrank/releases/latest):

| System                     | Release asset                       |
| -------------------------- | ----------------------------------- |
| Linux x86_64               | `langrank-linux-x86_64-gnu.tar.gz`  |
| Linux x86_64, static build | `langrank-linux-x86_64-musl.tar.gz` |
| Windows x86_64             | `langrank-windows-x86_64-msvc.zip`  |
| macOS Apple Silicon        | `langrank-macos-aarch64.tar.gz`     |

On Linux or macOS, extract the archive and install the binary for the current user:

```bash
tar -xzf langrank-<platform>.tar.gz
mkdir -p ~/.local/bin
install -m 755 langrank ~/.local/bin/langrank
```

Make sure `~/.local/bin` is in your `PATH`, then verify the installation:

```bash
langrank --version
```

On Windows, extract the archive with File Explorer or PowerShell:

```powershell
Expand-Archive .\langrank-windows-x86_64-msvc.zip -DestinationPath .\langrank
.\langrank\langrank.exe --version
```

Move `langrank.exe` to a permanent directory and add that directory to `PATH` if
you want to run it as `langrank` from any terminal.

### From source

Install the [Rust toolchain](https://rustup.rs/) and Git, then let Cargo build and
install the current version from GitHub:

```bash
cargo install --git https://github.com/hexqnt/langrank.git --locked
langrank --version
```

Cargo installs the executable into `~/.cargo/bin` by default.

## Usage

Running LangRank without options fetches the latest data and prints the top 10:

```bash
langrank
```

An internet connection is required while the program fetches its source data.

Print the complete ranking or see every available option:

```bash
langrank --full-output
langrank --help
```

### Save data and reports

Output flags accept an optional path. When no path is supplied, LangRank uses the
default shown by `langrank --help`.

```bash
# Save the combined source data and final ranking as CSV
langrank --save-rankings --save-schulze

# Save a full HTML report to a custom path
langrank --save-html report.html --full-output

# Compress saved CSV files as .gz archives
langrank --save-rankings --save-schulze --archive-csv
```

HTML is minified by default. Use `--no-minify-html` when you need readable HTML
source.

### Shell completions

```bash
# Install Bash completions for the current user
langrank completions bash --install

# Print a fish completion script to stdout
langrank completions fish
```

## How the ranking works

LangRank treats TIOBE, PYPL, Languish, and the combined performance score as four
preference ballots. The performance score blends relative Benchmarks Game results
with the best TechEmpower framework score available for each language. The Schulze
method combines these ballots into the final order; a combined popularity and
performance score breaks ties.

Missing benchmark data is displayed as `-` and contributes zero to the relevant
performance component.

## Library use

The repository also provides a Rust library for fetching normalized ranking data.
Disable default features when the CLI and report-generation dependencies are not
needed:

```toml
[dependencies]
langrank = { git = "https://github.com/hexqnt/langrank.git", default-features = false }
```

The main entry point is `langrank::Fetcher`; individual source fetchers are also
available for applications that need more control.

## Data sources

- [TIOBE Index](https://www.tiobe.com/tiobe-index/)
- [PYPL Popularity of Programming Language](https://pypl.github.io/PYPL.html)
- [Languish](https://tjpalmer.github.io/languish/)
- [The Computer Language Benchmarks Game](https://benchmarksgame-team.pages.debian.net/benchmarksgame/)
- [TechEmpower Framework Benchmarks](https://www.techempower.com/benchmarks/)

#!/usr/bin/env bash

# Профилирование полной рабочей нагрузки langrank через Linux perf.

set -Eeuo pipefail

readonly PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROFILE_DIR="${PROFILE_DIR:-$PROJECT_DIR/target/profiling-data}"
readonly PERF_EVENTS="${PERF_EVENTS:-task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,branches,branch-misses,cache-references,cache-misses}"
readonly PERF_STAT_REPEATS="${PERF_STAT_REPEATS:-3}"
readonly PERF_RECORD_FREQUENCY="${PERF_RECORD_FREQUENCY:-499}"
readonly BINARY="$PROJECT_DIR/target/profiling/langrank"
declare -ar DEFAULT_ARGUMENTS=(
    --save-html dist/index.html
    --save-rankings dist/rankings.csv
    --save-schulze dist/schulze_rankings.csv
    --save-benchmarks dist/benchmarksgame.csv
    --full-output
    --archive-csv
)

usage() {
    cat <<'EOF'
Usage:
  ./profiling.sh [options] [-- <langrank arguments...>]

Options:
      --build-only    Build the profiling binary without running perf
      --record-only   Collect sampled call stacks only
      --stat-only     Collect hardware counters only
  -h, --help          Show this help

Without arguments the script profiles the complete report-generation workload.
Arguments after -- replace that default workload.
Program stdout is saved alongside the perf data instead of being printed.

Environment:
  PROFILE_DIR              Output directory (default: target/profiling-data)
  PERF_STAT_REPEATS        perf stat repeat count (default: 3)
  PERF_RECORD_FREQUENCY    perf record sampling frequency (default: 499)
  PERF_EVENTS              Comma-separated perf stat events

Examples:
  ./profiling.sh
  PERF_STAT_REPEATS=5 ./profiling.sh --stat-only
  ./profiling.sh -- --save-html dist/index.html --full-output
EOF
}

build_only=false
mode=all
custom_arguments=false
declare -a arguments=()

while (($# > 0)); do
    case "$1" in
        --build-only)
            build_only=true
            shift
            ;;
        --record-only)
            mode=record
            shift
            ;;
        --stat-only)
            mode=stat
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            arguments=("$@")
            custom_arguments=true
            break
            ;;
        *)
            echo "error: unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ "$custom_arguments" == false ]]; then
    arguments=("${DEFAULT_ARGUMENTS[@]}")
fi

if [[ ! "$PERF_STAT_REPEATS" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: PERF_STAT_REPEATS must be a positive integer" >&2
    exit 2
fi
if [[ ! "$PERF_RECORD_FREQUENCY" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: PERF_RECORD_FREQUENCY must be a positive integer" >&2
    exit 2
fi

cd -- "$PROJECT_DIR"
mkdir -p -- "$PROFILE_DIR"

profiling_rustflags="-C force-frame-pointers=yes"
if [[ -n "${RUSTFLAGS-}" ]]; then
    profiling_rustflags="$RUSTFLAGS $profiling_rustflags"
fi

echo "Building optimized profiling binary"
env RUSTFLAGS="$profiling_rustflags" cargo build --profile profiling
if [[ ! -x "$BINARY" ]]; then
    echo "error: Cargo did not produce executable $BINARY" >&2
    exit 1
fi
if [[ "$build_only" == true ]]; then
    echo "Built: $BINARY"
    exit 0
fi
if ! command -v perf >/dev/null 2>&1; then
    echo "error: perf is not installed or is not in PATH" >&2
    exit 1
fi

declare -ar workload_command=(env NO_COLOR=1 "$BINARY" "${arguments[@]}")

if [[ "$mode" != stat ]]; then
    echo "Recording call stacks"
    perf record \
        --freq "$PERF_RECORD_FREQUENCY" \
        --event cycles:u \
        --call-graph fp \
        --output "$PROFILE_DIR/perf.data" \
        -- "${workload_command[@]}" \
        >"$PROFILE_DIR/record.stdout"
fi

if [[ "$mode" != record ]]; then
    echo "Collecting counters ($PERF_STAT_REPEATS repeats)"
    perf stat \
        --repeat "$PERF_STAT_REPEATS" \
        --event "$PERF_EVENTS" \
        --output "$PROFILE_DIR/perf-stat.txt" \
        -- "${workload_command[@]}" \
        >"$PROFILE_DIR/stat.stdout"
fi

echo "Profiles written to: $PROFILE_DIR"
if [[ "$mode" != stat ]]; then
    echo "Inspect call stacks with: perf report --input $PROFILE_DIR/perf.data"
fi

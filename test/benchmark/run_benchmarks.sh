#!/bin/bash
# Main entry point for DuckDB level_pivot benchmarks
# Usage: ./test/benchmark/run_benchmarks.sh
# Environment:
#   SIZES       - Comma-separated list of dataset sizes (default: 1000,10000,100000)
#   ITERATIONS  - Number of iterations per benchmark (default: 3)
#   BENCHMARKS  - Comma-separated list of benchmarks to run (default: all)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# DuckDB paths
DUCKDB_BIN="${PROJECT_DIR}/build/release/duckdb"
EXTENSION_PATH="${PROJECT_DIR}/build/release/extension/level_pivot/level_pivot.duckdb_extension"
BENCH_SETUP_SQL="${SCRIPT_DIR}/bench_setup.sql"
BENCH_DATA_GEN_SQL="${SCRIPT_DIR}/bench_data_gen.sql"
BENCH_DB_PATH="/tmp/level_pivot_bench"

# Configuration
SIZES="${SIZES:-1000,10000,100000}"
ITERATIONS="${ITERATIONS:-3}"
BENCHMARKS="${BENCHMARKS:-full_scan,filtered_scan,prefix_scan,single_insert,batch_insert,update_same_identity,delete_single,delete_batch}"

# Source benchmark utilities
source "${SCRIPT_DIR}/bench_utils.sh"

# Cleanup trap
cleanup() {
    rm -rf "${BENCH_DB_PATH}"
}
trap cleanup EXIT

# Verify DuckDB binary exists
if [[ ! -x "${DUCKDB_BIN}" ]]; then
    echo "ERROR: DuckDB binary not found at ${DUCKDB_BIN}" >&2
    echo "Run 'make' first to build the extension." >&2
    exit 1
fi

if [[ ! -f "${EXTENSION_PATH}" ]]; then
    echo "ERROR: Extension not found at ${EXTENSION_PATH}" >&2
    echo "Run 'make' first to build the extension." >&2
    exit 1
fi

echo "========================================"
echo "level_pivot DuckDB Benchmarks"
echo "========================================"
echo ""
echo "DuckDB: ${DUCKDB_BIN}"
echo "Extension: ${EXTENSION_PATH}"
echo "Sizes: ${SIZES}"
echo "Iterations: ${ITERATIONS}"
echo "Benchmarks: ${BENCHMARKS}"
echo ""

# Initialize CSV output
CSV_FILE=$(init_csv_output)
echo "Results will be written to: ${CSV_FILE}"
echo ""

# Convert comma-separated lists to arrays
IFS=',' read -ra SIZE_ARRAY <<< "${SIZES}"
IFS=',' read -ra BENCHMARK_ARRAY <<< "${BENCHMARKS}"

# Source all benchmark scripts
for benchmark in "${BENCHMARK_ARRAY[@]}"; do
    benchmark_script="${SCRIPT_DIR}/benchmarks/${benchmark}.sh"
    if [[ -f "${benchmark_script}" ]]; then
        source "${benchmark_script}"
    else
        echo "WARNING: Benchmark script not found: ${benchmark_script}" >&2
    fi
done

# Run benchmarks
echo "========================================"
echo "Running Benchmarks"
echo "========================================"

for size in "${SIZE_ARRAY[@]}"; do
    echo ""
    echo "----------------------------------------"
    echo "Dataset size: ${size} rows"
    echo "----------------------------------------"

    # Clean and regenerate data for this size
    rm -rf "${BENCH_DB_PATH}"

    echo "Generating test data..."
    data_sql=$(sed "s/\${BENCH_SIZE}/${size}/g" "${BENCH_DATA_GEN_SQL}")
    run_duckdb "${data_sql}" >/dev/null

    for benchmark in "${BENCHMARK_ARRAY[@]}"; do
        echo ""
        echo "Running: ${benchmark}"

        sql_fn="sql_${benchmark}"
        attrs_var="ATTRS_PER_ROW_${benchmark}"
        attrs_per_row="${!attrs_var:-0}"

        if declare -f "${sql_fn}" >/dev/null 2>&1; then
            # Multi-iteration mode: run all iterations in one process (scan benchmarks)
            bench_sql=$("${sql_fn}" "${size}")

            timed_output=$(run_duckdb_multi "${bench_sql}" "${ITERATIONS}")
            if [[ $? -ne 0 ]]; then
                echo "  ERROR running ${benchmark}"
                continue
            fi

            # Parse interleaved timestamps and results
            # Format: t0, result1, t1, result2, t2, ..., resultN, tN
            mapfile -t lines <<< "${timed_output}"

            for iteration in $(seq 1 "${ITERATIONS}"); do
                t_prev="${lines[$((2 * (iteration - 1)))]}"
                t_curr="${lines[$((2 * iteration))]}"
                result_line="${lines[$((2 * iteration - 1))]}"

                duration_ms=$(awk "BEGIN {printf \"%.3f\", (${t_curr} - ${t_prev}) / 1000}")
                row_count=$(echo "${result_line}" | grep -oE '[0-9]+' | tail -1)

                record_result "${benchmark}" "${size}" "${iteration}" "${duration_ms}" "${row_count}" "${attrs_per_row}"
                print_progress "${benchmark}" "${size}" "${iteration}" "${ITERATIONS}" "${duration_ms}" "${row_count}" "${attrs_per_row}"
            done
        else
            # Per-iteration mode (for DML benchmarks with setup/teardown)
            for iteration in $(seq 1 "${ITERATIONS}"); do
                result=$(run_benchmark "${benchmark}" "${size}" "${iteration}" "${attrs_per_row}")
                duration_ms=$(echo "${result}" | cut -d',' -f1)
                rows_affected=$(echo "${result}" | cut -d',' -f2)

                if [[ "${duration_ms}" == "-1" ]]; then
                    echo "  [${iteration}/${ITERATIONS}] ERROR"
                else
                    print_progress "${benchmark}" "${size}" "${iteration}" "${ITERATIONS}" "${duration_ms}" "${rows_affected}" "${attrs_per_row}"
                fi
            done
        fi
    done
done

# Generate summary
generate_summary

echo ""
echo "========================================"
echo "Benchmarks Complete"
echo "========================================"
echo ""
echo "Results saved to: ${CSV_FILE}"

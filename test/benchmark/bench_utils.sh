#!/bin/bash
# Utility functions for DuckDB level_pivot benchmarks
# Sourced by run_benchmarks.sh and individual benchmark scripts

# CSV output file (set by init_csv_output)
CSV_FILE=""

# Initialize CSV output with header
# Usage: init_csv_output [output_dir]
init_csv_output() {
    local output_dir="${1:-${SCRIPT_DIR}/results}"
    mkdir -p "${output_dir}"

    local timestamp
    timestamp=$(date +%Y%m%d_%H%M%S)
    CSV_FILE="${output_dir}/benchmark_results_${timestamp}.csv"

    echo "benchmark,size,iteration,duration_ms,rows_affected,rows_per_sec,leveldb_keys_per_sec" > "${CSV_FILE}"
    echo "${CSV_FILE}"
}

# Get current time in nanoseconds (used by DML benchmarks that time multi-statement batches)
get_time_ns() {
    if date +%N >/dev/null 2>&1 && [[ "$(date +%N)" != "N" ]]; then
        # Linux with nanosecond support
        echo $(($(date +%s%N)))
    else
        # macOS or systems without nanosecond support - use perl for milliseconds
        perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000'
    fi
}

# Run SQL through DuckDB CLI with bench_setup.sql preamble
# Usage: run_duckdb "SQL"
run_duckdb() {
    local sql="$1"
    {
        echo "LOAD '${EXTENSION_PATH}';"
        cat "${BENCH_SETUP_SQL}"
        echo "${sql}"
    } | "${DUCKDB_BIN}" -csv -noheader -unsigned
}

# Run a benchmark SQL query N times in a single DuckDB process with in-process timing.
# Includes a warmup iteration (not timed). Returns interleaved timestamps and results.
# Usage: output=$(run_duckdb_multi "SQL" num_iterations)
# Output format (one value per line): t0, result1, t1, result2, t2, ..., resultN, tN
run_duckdb_multi() {
    local benchmark_sql="$1"
    local iterations="$2"

    local sql=""
    # Warmup (not timed)
    sql+="${benchmark_sql};"
    # Sentinel to mark start of timed section
    sql+="SELECT -999;"
    # Timed iterations (epoch_us for microsecond resolution)
    sql+="SELECT epoch_us(get_current_timestamp());"
    for i in $(seq 1 "$iterations"); do
        sql+="${benchmark_sql};"
        sql+="SELECT epoch_us(get_current_timestamp());"
    done

    local output
    output=$(run_duckdb "${sql}" 2>&1)
    local status=$?

    if [[ ${status} -ne 0 ]]; then
        echo "ERROR: ${output}" >&2
        return 1
    fi

    # Extract everything after the -999 sentinel
    echo "${output}" | sed -n '/-999/,$ p' | tail -n +2
}

# Time a SQL command using DuckDB's in-process timestamps for accurate measurement
# Usage: time_sql_cmd "SQL COMMAND" [expected_rows]
# Returns: duration_ms,rows_affected
# Note: level_pivot DML returns "true" not row counts, so pass expected_rows for DML
time_sql_cmd() {
    local sql="$1"
    local expected_rows="${2:-}"

    # Wrap query with in-process timestamp markers (epoch_us for microsecond resolution)
    local timed_sql="SELECT epoch_us(get_current_timestamp());${sql};SELECT epoch_us(get_current_timestamp());"

    local output
    output=$(run_duckdb "${timed_sql}" 2>&1)
    local status=$?

    if [[ ${status} -ne 0 ]]; then
        echo "ERROR: ${output}" >&2
        echo "-1,0"
        return 1
    fi

    # Timestamps are 16-digit epoch_us values (microseconds)
    local t_start t_end
    t_start=$(echo "${output}" | grep -oE '^[0-9]{15,}$' | head -1)
    t_end=$(echo "${output}" | grep -oE '^[0-9]{15,}$' | tail -1)

    local duration_ms
    duration_ms=$(awk "BEGIN {printf \"%.3f\", (${t_end} - ${t_start}) / 1000}")

    local row_count
    if [[ -n "${expected_rows}" ]]; then
        row_count="${expected_rows}"
    else
        # Extract row count: numeric lines that aren't 15+ digit timestamps
        row_count=$(echo "${output}" | grep -oE '^[0-9]+$' | grep -vE '^[0-9]{15,}$' | tail -1)
        if [[ -z "${row_count}" ]]; then
            row_count=0
        fi
    fi

    echo "${duration_ms},${row_count}"
}

# Record a benchmark result to CSV
# Usage: record_result benchmark_name size iteration duration_ms rows_affected [attrs_per_row]
record_result() {
    local benchmark="$1"
    local size="$2"
    local iteration="$3"
    local duration_ms="$4"
    local rows_affected="$5"
    local attrs_per_row="${6:-0}"

    local rows_per_sec leveldb_keys_per_sec
    if [[ "${duration_ms}" != "0" && "${duration_ms}" != "-1" && -n "${duration_ms}" ]]; then
        rows_per_sec=$(awk "BEGIN {printf \"%.2f\", ${rows_affected} * 1000 / ${duration_ms}}" 2>/dev/null || echo "0")
        if [[ "${attrs_per_row}" != "0" && -n "${attrs_per_row}" ]]; then
            leveldb_keys_per_sec=$(awk "BEGIN {printf \"%.2f\", ${rows_affected} * ${attrs_per_row} * 1000 / ${duration_ms}}" 2>/dev/null || echo "")
        else
            leveldb_keys_per_sec=""
        fi
    else
        rows_per_sec="0"
        leveldb_keys_per_sec=""
    fi

    echo "${benchmark},${size},${iteration},${duration_ms},${rows_affected},${rows_per_sec},${leveldb_keys_per_sec}" >> "${CSV_FILE}"
}

# Run a benchmark with setup and teardown (used for DML benchmarks)
# Usage: run_benchmark benchmark_name size iteration [attrs_per_row]
# Expects functions: setup_<benchmark>, run_<benchmark>, teardown_<benchmark>
run_benchmark() {
    local benchmark="$1"
    local size="$2"
    local iteration="$3"
    local attrs_per_row="${4:-0}"

    # Run setup if defined
    local setup_fn="setup_${benchmark}"
    if declare -f "${setup_fn}" >/dev/null 2>&1; then
        "${setup_fn}" "${size}" || return 1
    fi

    # Run the benchmark
    local run_fn="run_${benchmark}"
    if ! declare -f "${run_fn}" >/dev/null 2>&1; then
        echo "ERROR: run_${benchmark} not defined" >&2
        return 1
    fi

    local result
    result=$("${run_fn}" "${size}")
    local status=$?

    # Run teardown if defined
    local teardown_fn="teardown_${benchmark}"
    if declare -f "${teardown_fn}" >/dev/null 2>&1; then
        "${teardown_fn}" "${size}" || true
    fi

    if [[ ${status} -ne 0 ]]; then
        return 1
    fi

    # Parse result (duration_ms,rows_affected)
    local duration_ms rows_affected
    duration_ms=$(echo "${result}" | cut -d',' -f1)
    rows_affected=$(echo "${result}" | cut -d',' -f2)

    record_result "${benchmark}" "${size}" "${iteration}" "${duration_ms}" "${rows_affected}" "${attrs_per_row}"

    # Return result for display
    echo "${duration_ms},${rows_affected}"
}

# Generate summary statistics from CSV
# Usage: generate_summary
generate_summary() {
    echo ""
    echo "========================================"
    echo "Benchmark Summary"
    echo "========================================"
    echo ""
    printf "%-25s %8s %10s %10s %10s %12s %14s\n" "Benchmark" "Size" "Min(ms)" "Max(ms)" "Avg(ms)" "Rows/sec" "LDB Keys/sec"
    printf "%-25s %8s %10s %10s %10s %12s %14s\n" "-------------------------" "--------" "----------" "----------" "----------" "------------" "--------------"

    # Skip header, group by benchmark and size, calculate statistics
    tail -n +2 "${CSV_FILE}" | \
    awk -F',' '
    {
        key = $1 "," $2
        if (!(key in count)) {
            count[key] = 0
            sum[key] = 0
            min[key] = 999999999
            max[key] = 0
            rps_sum[key] = 0
            lkps_sum[key] = 0
            lkps_count[key] = 0
            order[++n] = key
        }
        count[key]++
        sum[key] += $4
        rps_sum[key] += $6
        if ($7 != "" && $7 + 0 > 0) {
            lkps_sum[key] += $7
            lkps_count[key]++
        }
        if ($4 < min[key]) min[key] = $4
        if ($4 > max[key]) max[key] = $4
    }
    END {
        for (i = 1; i <= n; i++) {
            key = order[i]
            split(key, parts, ",")
            benchmark = parts[1]
            size = parts[2]
            avg = sum[key] / count[key]
            rps_avg = rps_sum[key] / count[key]
            if (lkps_count[key] > 0) {
                lkps_avg = lkps_sum[key] / lkps_count[key]
                printf "%-25s %8d %10.1f %10.1f %10.1f %12.1f %14.1f\n", benchmark, size, min[key], max[key], avg, rps_avg, lkps_avg
            } else {
                printf "%-25s %8d %10.1f %10.1f %10.1f %12.1f %14s\n", benchmark, size, min[key], max[key], avg, rps_avg, "-"
            }
        }
    }
    '
}

# Print progress indicator
print_progress() {
    local benchmark="$1"
    local size="$2"
    local iteration="$3"
    local total_iterations="$4"
    local duration_ms="$5"
    local rows_affected="${6:-0}"
    local attrs_per_row="${7:-0}"

    local rate_str=""
    if [[ "${rows_affected}" -gt 0 && "${duration_ms}" != "0" ]]; then
        local rps
        rps=$(awk "BEGIN {printf \"%.0f\", ${rows_affected} * 1000 / ${duration_ms}}")
        rate_str="  (${rps} rows/sec"
        if [[ "${attrs_per_row}" != "0" && -n "${attrs_per_row}" ]]; then
            local kps
            kps=$(awk "BEGIN {printf \"%.0f\", ${rows_affected} * ${attrs_per_row} * 1000 / ${duration_ms}}")
            rate_str+=", ${kps} keys/sec"
        fi
        rate_str+=")"
    fi

    printf "  [%d/%d] %s @ %d rows: %.1f ms%s\n" \
        "${iteration}" "${total_iterations}" "${benchmark}" "${size}" "${duration_ms}" "${rate_str}"
}

#!/bin/bash
# Single-row INSERT benchmark
# Tests per-row write overhead with 100 individual INSERT statements

# Setup: ensure events table is clean
setup_single_insert() {
    local size="$1"
    run_duckdb "DELETE FROM db.bench_events" >/dev/null 2>&1 || true
}

# Run benchmark: Insert 100 single rows (all in one CLI invocation)
# Returns: duration_ms,rows_affected
run_single_insert() {
    local size="$1"
    local sql=""
    for i in $(seq 1 100); do
        sql+="INSERT INTO db.bench_events VALUES ('single_${i}', 'test', 'payload_${i}');"$'\n'
    done

    local start_ns end_ns duration_ms
    start_ns=$(get_time_ns)
    run_duckdb "${sql}" >/dev/null 2>&1
    end_ns=$(get_time_ns)
    duration_ms=$(awk "BEGIN {printf \"%.3f\", (${end_ns} - ${start_ns}) / 1000000}")

    echo "${duration_ms},100"
}

# Teardown: clean up inserted rows
teardown_single_insert() {
    local size="$1"
    run_duckdb "DELETE FROM db.bench_events WHERE event_id LIKE 'single_%'" >/dev/null 2>&1 || true
}

#!/bin/bash
# Single DELETE benchmark
# Tests key scan for deletion of individual rows

# Setup: create rows specifically for delete testing
setup_delete_single() {
    local size="$1"
    run_duckdb "INSERT INTO db.bench_events SELECT 'delete_single_' || i, 'delete_test', 'payload_' || i FROM generate_series(1, 100) AS t(i);" >/dev/null 2>&1 || true
}

# Run benchmark: Delete 100 rows one at a time (all in one CLI invocation)
# Returns: duration_ms,rows_affected
run_delete_single() {
    local size="$1"
    local sql=""
    for i in $(seq 1 100); do
        sql+="DELETE FROM db.bench_events WHERE event_id = 'delete_single_${i}';"$'\n'
    done

    local start_ns end_ns duration_ms
    start_ns=$(get_time_ns)
    run_duckdb "${sql}" >/dev/null 2>&1
    end_ns=$(get_time_ns)
    duration_ms=$(awk "BEGIN {printf \"%.3f\", (${end_ns} - ${start_ns}) / 1000000}")

    echo "${duration_ms},100"
}

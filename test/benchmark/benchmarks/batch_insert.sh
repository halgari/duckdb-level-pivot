#!/bin/bash
# Batch INSERT benchmark
# Tests WriteBatch efficiency with multi-row inserts

# Setup: ensure events table is clean
setup_batch_insert() {
    local size="$1"
    run_duckdb "DELETE FROM db.bench_events" >/dev/null 2>&1 || true
}

# Run benchmark: Insert 1000 rows in a single batch
# Returns: duration_ms,rows_affected
run_batch_insert() {
    local size="$1"
    time_sql_cmd "INSERT INTO db.bench_events SELECT 'batch_' || i, 'batch_test', 'payload_' || i FROM generate_series(1, 1000) AS t(i)" 1000
}

# Teardown: clean up inserted rows
teardown_batch_insert() {
    local size="$1"
    run_duckdb "DELETE FROM db.bench_events WHERE event_id LIKE 'batch_%'" >/dev/null 2>&1 || true
}

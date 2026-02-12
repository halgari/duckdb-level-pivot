#!/bin/bash
# Batch DELETE benchmark
# Tests batch deletion efficiency

# Setup: create rows specifically for batch delete testing
setup_delete_batch() {
    local size="$1"
    run_duckdb "INSERT INTO db.bench_events SELECT 'delete_batch_' || i, 'batch_delete_test', 'payload_' || i FROM generate_series(1, 1000) AS t(i);" >/dev/null 2>&1 || true
}

# Run benchmark: Delete 1000 rows in a single operation
# Returns: duration_ms,rows_affected
run_delete_batch() {
    local size="$1"
    time_sql_cmd "DELETE FROM db.bench_events WHERE event_id LIKE 'delete_batch_%'" 1000
}

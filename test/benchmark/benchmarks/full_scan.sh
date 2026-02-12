#!/bin/bash
# Full table scan benchmark
# Tests key parsing overhead on a complete table scan

# bench_users has 3 attribute columns: name, email, status
ATTRS_PER_ROW_full_scan=3

# Return the SQL query for multi-iteration mode
sql_full_scan() {
    echo "SELECT COUNT(*) FROM db.bench_users"
}

# Legacy single-iteration mode (used by run_benchmark)
run_full_scan() {
    local size="$1"
    time_sql_cmd "SELECT COUNT(*) FROM db.bench_users"
}

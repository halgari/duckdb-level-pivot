#!/bin/bash
# Filtered scan benchmark
# Tests WHERE clause pushdown to prefix scan on first identity column

# bench_users has 3 attribute columns: name, email, status
ATTRS_PER_ROW_filtered_scan=3

# Return the SQL query for multi-iteration mode
sql_filtered_scan() {
    echo "SELECT COUNT(*) FROM db.bench_users WHERE tenant = 'tenant_0'"
}

# Legacy single-iteration mode (used by run_benchmark)
run_filtered_scan() {
    local size="$1"
    time_sql_cmd "SELECT COUNT(*) FROM db.bench_users WHERE tenant = 'tenant_0'"
}

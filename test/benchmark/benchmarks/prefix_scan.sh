#!/bin/bash
# Multi-level prefix scan benchmark
# Tests WHERE clause pushdown with multiple identity columns

# bench_metrics has 2 attribute columns: value, timestamp
ATTRS_PER_ROW_prefix_scan=2

# Return the SQL query for multi-iteration mode
sql_prefix_scan() {
    echo "SELECT COUNT(*) FROM db.bench_metrics WHERE tenant = 'tenant_0' AND service = 'service_0'"
}

# Legacy single-iteration mode (used by run_benchmark)
run_prefix_scan() {
    local size="$1"
    time_sql_cmd "SELECT COUNT(*) FROM db.bench_metrics WHERE tenant = 'tenant_0' AND service = 'service_0'"
}

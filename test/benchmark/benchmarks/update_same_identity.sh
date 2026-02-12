#!/bin/bash
# Update same identity benchmark
# Tests incremental attribute updates (no key change)

# Run benchmark: Update only attribute columns for 100 rows
# This should be efficient as it only updates values, not keys
# Returns: duration_ms,rows_affected
run_update_same_identity() {
    local size="$1"
    time_sql_cmd "UPDATE db.bench_users SET status = 'updated', email = email || '.updated' WHERE tenant = 'tenant_0' AND user_id IN (SELECT user_id FROM db.bench_users WHERE tenant = 'tenant_0' LIMIT 100)" 100
}

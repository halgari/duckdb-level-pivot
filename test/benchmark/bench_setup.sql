-- Benchmark table setup for level_pivot DuckDB extension
-- Prepended to every DuckDB CLI invocation as preamble

ATTACH '/tmp/level_pivot_bench' AS db (TYPE level_pivot, CREATE_IF_MISSING true);

CALL level_pivot_create_table('db', 'bench_users', 'users##{tenant}##{user_id}##{attr}',
  ['tenant', 'user_id', 'name', 'email', 'status']);

CALL level_pivot_create_table('db', 'bench_metrics', 'metrics##{tenant}##{service}##{metric_id}##{attr}',
  ['tenant', 'service', 'metric_id', 'value', 'timestamp']);

CALL level_pivot_create_table('db', 'bench_events', 'events##{event_id}##{attr}',
  ['event_id', 'event_type', 'payload']);

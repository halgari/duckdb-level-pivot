-- Generate benchmark test data
-- ${BENCH_SIZE} is substituted by sed before execution

-- Generate users across 10 tenants
INSERT INTO db.bench_users
SELECT
    'tenant_' || (i % 10),
    'user_' || i,
    'User Name ' || i,
    'user' || i || '@example.com',
    CASE WHEN i % 3 = 0 THEN 'active' WHEN i % 3 = 1 THEN 'inactive' ELSE 'pending' END
FROM generate_series(1, ${BENCH_SIZE}) AS t(i);

-- Generate metrics across 5 tenants x 20 services
INSERT INTO db.bench_metrics
SELECT
    'tenant_' || (i % 5),
    'service_' || (i % 20),
    'metric_' || i,
    (random() * 1000)::INTEGER::VARCHAR,
    now()::VARCHAR
FROM generate_series(1, ${BENCH_SIZE}) AS t(i);

-- Events table is populated during insert benchmarks

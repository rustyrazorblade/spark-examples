-- Query shapes of the bulk-read benchmark,
-- over the cassandra-easy-stress BulkReadDataset STANDARD tables: fact, fact_ref, fact_dim, fact_dim2.
-- The format is in the QueryShape javadoc.  Expected values refer to the loader's EXPECTED output.
--
-- Joins that keep every fact row: fact.g_mid is in [0, mediumCardinality) and fact_dim has one row per
-- g_mid value; fact.ref_id is in [0, refRows) and fact_ref has one row per ref_id.  So Q9 and Q10 keep
-- count(*) rows.  Q19 keeps "rows joining fact_dim and fact_dim2".

-- shape: Q1
-- tables: fact
-- expect: c = fact.values.count(*)
SELECT count(*) AS c FROM fact;

-- Full export: every column of every row, consumed on the executors.  The checksum is the one
-- the loader and FullTableScan report for SELECT *.
-- shape: Q2
-- tables: fact
-- sink: checksum
-- expect: rows = fact.rows
-- expect: checksum = fact.checksum
-- expect: sum(i64) = fact.values.sum(i64)
SELECT * FROM fact;

-- shape: Q3
-- tables: fact
-- sink: checksum
-- expect: rows = fact.rows
-- expect: sum(i64) = fact.values.sum(i64)
SELECT pk, i64, bucket FROM fact;

-- Filters on the regular column bucket (uniform in 0..9999) at 0.1%, 1%, 10%, and 50%.  Every column of
-- the matching rows is returned.
-- shape: Q4_0.1
-- tables: fact
-- sink: checksum
-- expect: rows = fact.values.rows with bucket < 10
SELECT * FROM fact WHERE bucket < 10;

-- shape: Q4_1
-- tables: fact
-- sink: checksum
-- expect: rows = fact.values.rows with bucket < 100
SELECT * FROM fact WHERE bucket < 100;

-- shape: Q4_10
-- tables: fact
-- sink: checksum
-- expect: rows = fact.values.rows with bucket < 1000
SELECT * FROM fact WHERE bucket < 1000;

-- shape: Q4_50
-- tables: fact
-- sink: checksum
-- expect: rows = fact.values.rows with bucket < 5000
SELECT * FROM fact WHERE bucket < 5000;

-- shape: Q6
-- tables: fact
-- expect: c = fact.values.count(*)
-- expect: sum_i64 = fact.values.sum(i64)
-- expect: min_i32 = fact.values.min(i32)
-- expect: max_i32 = fact.values.max(i32)
SELECT count(*) AS c, sum(i64) AS sum_i64, min(i32) AS min_i32, max(i32) AS max_i32, avg(i64) AS avg_i64
FROM fact;

-- GROUP BY at about 10, 10K, and 10M groups (fewer at small sizes: one group per distinct value).
-- shape: Q7_low
-- tables: fact
-- sink: checksum
-- expect: rows = fact.values.distinct g_low
-- expect: sum(c) = fact.values.count(*)
-- expect: sum(s) = fact.values.sum(i64)
SELECT g_low, count(*) AS c, sum(i64) AS s FROM fact GROUP BY g_low;

-- shape: Q7_mid
-- tables: fact
-- sink: checksum
-- expect: rows = fact.values.distinct g_mid
-- expect: sum(c) = fact.values.count(*)
-- expect: sum(s) = fact.values.sum(i64)
SELECT g_mid, count(*) AS c, sum(i64) AS s FROM fact GROUP BY g_mid;

-- shape: Q7_high
-- tables: fact
-- sink: checksum
-- expect: rows = fact.values.distinct g_high
-- expect: sum(c) = fact.values.count(*)
-- expect: sum(s) = fact.values.sum(i64)
SELECT g_high, count(*) AS c, sum(i64) AS s FROM fact GROUP BY g_high;

-- shape: Q8
-- tables: fact
-- sink: checksum
-- expect: sum(c) = fact.values.rows with bucket < 1000
SELECT g_mid, count(*) AS c, sum(i64) AS s FROM fact WHERE bucket < 1000 GROUP BY g_mid;

-- shape: Q9
-- tables: fact, fact_dim
-- expect: sum(c) = fact.values.count(*)
-- expect: sum(s) = fact.values.sum(i64)
SELECT d.category, count(*) AS c, sum(f.i64) AS s
FROM fact f JOIN fact_dim d ON f.g_mid = d.g_mid
GROUP BY d.category;

-- shape: Q10
-- tables: fact, fact_ref
-- expect: c = fact.values.count(*)
-- expect: sum_i64 = fact.values.sum(i64)
SELECT count(*) AS c, sum(f.i64) AS sum_i64, sum(r.score) AS sum_score
FROM fact f JOIN fact_ref r ON f.ref_id = r.ref_id;

-- shape: Q11
-- tables: fact
-- expect: rows = 100
SELECT pk, event_time, row_id, i64, bucket FROM fact ORDER BY i64 DESC, row_id LIMIT 100;

-- shape: Q15
-- tables: fact
-- expect: d = fact.values.distinct g_high
SELECT count(DISTINCT g_high) AS d FROM fact;

-- shape: Q19
-- tables: fact, fact_dim, fact_dim2
-- expect: sum(c) = fact.values.rows joining fact_dim and fact_dim2
SELECT d.category, d2.tier, count(*) AS c, sum(f.i64) AS s
FROM fact f
JOIN fact_dim d ON f.g_mid = d.g_mid
JOIN fact_dim2 d2 ON f.dim2_id = d2.dim2_id
GROUP BY d.category, d2.tier;

-- 05_health_flags_test.sql —— 健康标志逻辑验证
-- 自包含测试：用临时表复现 health_flags 改接 meta-collect ODS 后的判定逻辑（2026-08-11）——
-- 表侧列由 v_paimon_meta_level_stats（按轮次 collected_at 截到分钟）提供。断言：
--   1. L0 阈值标志（>1000 → L0_PILEUP，=1000 边界不触发）与反压标志（>30000ms → BACKPRESSURE）；
--   2. 表侧列只落在对齐采集轮次的分钟桶上，中间分钟为 NULL（约每 3 分钟一轮属正常）。
-- 不触碰真实表；与其它 _test.sql 同风格。

DROP TABLE IF EXISTS test_health_level_stats;
DROP TABLE IF EXISTS test_health_write;
DROP TABLE IF EXISTS test_health_result;

-- 模拟 v_paimon_meta_level_stats：两轮采集（00:00:30 / 00:03:10），每轮含多个 level 行
CREATE TABLE test_health_level_stats (
  table_name VARCHAR(32),
  source_snapshot_id BIGINT,
  level INT,
  file_count BIGINT,
  collected_at DATETIME
) DUPLICATE KEY(table_name, source_snapshot_id, level)
DISTRIBUTED BY HASH(table_name) BUCKETS 1;

INSERT INTO test_health_level_stats VALUES
  ('wide_table', 10, 0, 1500, '2000-01-01 00:00:30'),  -- L0 超阈值
  ('wide_table', 10, 1,  100, '2000-01-01 00:00:30'),
  ('wide_table', 11, 0, 1000, '2000-01-01 00:03:10'),  -- 边界：=1000 不触发
  ('wide_table', 11, 1,  800, '2000-01-01 00:03:10'),
  ('wide_table', 11, 2,  200, '2000-01-01 00:03:10');

-- 模拟写入侧分钟桶（buckets + metrics_write_health 合并为一行一分钟）：00:01 反压超标
CREATE TABLE test_health_write (
  time_bucket_minute VARCHAR(20),
  max_checkpoint_start_delay_ms DOUBLE
) DUPLICATE KEY(time_bucket_minute)
DISTRIBUTED BY HASH(time_bucket_minute) BUCKETS 1;

INSERT INTO test_health_write VALUES
  ('2000-01-01 00:00:00',  5000),
  ('2000-01-01 00:01:00', 45000),
  ('2000-01-01 00:02:00',  5000),
  ('2000-01-01 00:03:00',  5000);

-- 复现 health_flags 的表侧聚合 + LEFT JOIN + 软标志判定（与视图同一套表达式，仅写入侧列裁剪为反压）
CREATE TABLE test_health_result AS
SELECT
  w.time_bucket_minute,
  t.level0_file_count,
  t.paimon_file_count,
  w.max_checkpoint_start_delay_ms,
  CASE WHEN t.level0_file_count > 1000 THEN 'L0_PILEUP' ELSE 'OK' END AS l0_flag,
  CASE WHEN w.max_checkpoint_start_delay_ms > 30000 THEN 'BACKPRESSURE' ELSE 'OK' END AS backpressure_flag
FROM test_health_write w
LEFT JOIN (
  -- 同分钟多轮采集时取较大者，保证每分钟一行（与视图 table_side 相同）
  SELECT
    time_bucket_minute,
    MAX(level0_file_count) AS level0_file_count,
    MAX(paimon_file_count) AS paimon_file_count
  FROM (
    SELECT
      DATE_FORMAT(collected_at, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
      source_snapshot_id,
      SUM(CASE WHEN level = 0 THEN file_count ELSE 0 END) AS level0_file_count,
      SUM(file_count) AS paimon_file_count
    FROM test_health_level_stats
    WHERE table_name = 'wide_table'
    GROUP BY DATE_FORMAT(collected_at, '%Y-%m-%d %H:%i:00'), source_snapshot_id
  ) s
  GROUP BY time_bucket_minute
) t ON w.time_bucket_minute = t.time_bucket_minute;

-- 预期（4行）：
-- 00:00: L0=1500 / 总数=1600 → L0_PILEUP，反压 OK
-- 00:01: 表侧 NULL（非轮次分钟）→ l0_flag OK，反压 BACKPRESSURE
-- 00:02: 表侧 NULL（非轮次分钟）→ l0_flag OK，反压 OK
-- 00:03: L0=1000（边界不触发）/ 总数=2000 → l0_flag OK，反压 OK

-- 断言1：软标志判定正确（L0 触发恰好 1 次、边界不触发、反压恰好 1 次）
SELECT
  '断言1: 软标志判定正确' AS test_description,
  COUNT(*) AS total_cases,
  SUM(CASE WHEN l0_flag = 'L0_PILEUP' THEN 1 ELSE 0 END) AS l0_triggered,
  SUM(CASE WHEN time_bucket_minute = '2000-01-01 00:03:00' AND l0_flag = 'OK' THEN 1 ELSE 0 END) AS l0_boundary_ok,
  SUM(CASE WHEN backpressure_flag = 'BACKPRESSURE' THEN 1 ELSE 0 END) AS bp_triggered,
  CASE WHEN COUNT(*) = 4
        AND SUM(CASE WHEN l0_flag = 'L0_PILEUP' THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN time_bucket_minute = '2000-01-01 00:00:00' AND l0_flag = 'L0_PILEUP' THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN time_bucket_minute = '2000-01-01 00:03:00' AND l0_flag = 'OK' THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN backpressure_flag = 'BACKPRESSURE' THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN time_bucket_minute = '2000-01-01 00:01:00' AND backpressure_flag = 'BACKPRESSURE' THEN 1 ELSE 0 END) = 1
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM test_health_result;

-- 断言2：表侧列只落在轮次分钟（2 行非 NULL / 2 行 NULL），且各 level 求和为文件总数
SELECT
  '断言2: 表侧列只落在轮次分钟且聚合正确' AS test_description,
  SUM(CASE WHEN level0_file_count IS NOT NULL THEN 1 ELSE 0 END) AS aligned_minutes,
  SUM(CASE WHEN level0_file_count IS NULL THEN 1 ELSE 0 END) AS gap_minutes,
  CASE WHEN SUM(CASE WHEN level0_file_count IS NOT NULL THEN 1 ELSE 0 END) = 2
        AND SUM(CASE WHEN level0_file_count IS NULL THEN 1 ELSE 0 END) = 2
        AND SUM(CASE WHEN time_bucket_minute = '2000-01-01 00:00:00'
                      AND level0_file_count = 1500 AND paimon_file_count = 1600 THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN time_bucket_minute = '2000-01-01 00:03:00'
                      AND level0_file_count = 1000 AND paimon_file_count = 2000 THEN 1 ELSE 0 END) = 1
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM test_health_result;

-- 详细结果
SELECT * FROM test_health_result ORDER BY time_bucket_minute;

-- 清理
DROP TABLE test_health_level_stats;
DROP TABLE test_health_write;
DROP TABLE test_health_result;

-- 预期输出：断言1 / 断言2 均 PASS

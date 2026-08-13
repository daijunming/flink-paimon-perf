-- 02_four_category_metrics_test.sql —— metrics_ingest_perf 聚合逻辑验证（自包含，不触碰真实分区表）
-- 背景（2026-08-12 现场核实）：'%ConstraintEnforcer%numRecordsOut' 命中 6 个 metric_name
--   = 2 算子 × 3 subtask——任务级链名 'Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map'
--   与算子级名 'ConstraintEnforcer[4]' 是同一条链的两种上报粒度、计数相同；不分算子直接
--   SUM 会把同一批记录算两遍（吞吐虚高约 2 倍）。
-- 复现 02 修复后的三层聚合（桶内 MAX → 单算子内跨 subtask SUM → 跨算子 MAX 去重）并断言：
--   断言0：样本确为 6 名 / 2 算子（防止样本被改回单粒度后断言1 形同虚设）；
--   断言1：去重后 total=(0, 54000, 108000)（重复计数会得 (0,108000,216000)）、
--          非空 rps 恒 300（180s 间隔差分守恒）、首桶 NULL。

DROP TABLE IF EXISTS test_ingest_input;

-- 模拟 metrics_view 层输入（metric_value / metric_ts 保持 varchar，与真实表一致）
CREATE TABLE test_ingest_input (
  metric_name  VARCHAR(200),
  metric_value VARCHAR(50),
  metric_ts    VARCHAR(50)
) DUPLICATE KEY(metric_name)
DISTRIBUTED BY HASH(metric_name) BUCKETS 1;

-- 2 种上报粒度 × 3 个 subtask 的累计计数器（两粒度值相同 = 同一股数据）：
--   t0=0 → t1=18000 → t2=36000（每 subtask 每 180s +18000 = 100 rps，作业级 300 rps）；
--   s0 在 t1 桶内两个采样点（17999@:00 / 18000@:30），验证桶内 MAX 取 18000。
-- 桶：946684800000 = 2000-01-01 00:00:00 UTC，+180s/桶。
INSERT INTO test_ingest_input VALUES
  -- 粒度1：任务级链名
  ('Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map.0.numRecordsOut', '0',     '946684800000'),
  ('Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map.1.numRecordsOut', '0',     '946684800000'),
  ('Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map.2.numRecordsOut', '0',     '946684800000'),
  ('Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map.0.numRecordsOut', '17999', '946684980000'),
  ('Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map.0.numRecordsOut', '18000', '946685010000'),
  ('Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map.1.numRecordsOut', '18000', '946684980000'),
  ('Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map.2.numRecordsOut', '18000', '946684980000'),
  ('Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map.0.numRecordsOut', '36000', '946685160000'),
  ('Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map.1.numRecordsOut', '36000', '946685160000'),
  ('Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map.2.numRecordsOut', '36000', '946685160000'),
  -- 粒度2：算子级名（值与粒度1 相同）
  ('ConstraintEnforcer[4].0.numRecordsOut', '0',     '946684800000'),
  ('ConstraintEnforcer[4].1.numRecordsOut', '0',     '946684800000'),
  ('ConstraintEnforcer[4].2.numRecordsOut', '0',     '946684800000'),
  ('ConstraintEnforcer[4].0.numRecordsOut', '17999', '946684980000'),
  ('ConstraintEnforcer[4].0.numRecordsOut', '18000', '946685010000'),
  ('ConstraintEnforcer[4].1.numRecordsOut', '18000', '946684980000'),
  ('ConstraintEnforcer[4].2.numRecordsOut', '18000', '946684980000'),
  ('ConstraintEnforcer[4].0.numRecordsOut', '36000', '946685160000'),
  ('ConstraintEnforcer[4].1.numRecordsOut', '36000', '946685160000'),
  ('ConstraintEnforcer[4].2.numRecordsOut', '36000', '946685160000');

-- 断言0：样本确为 6 个 metric_name、剥后缀后 2 个算子（样本完整性自锁，防退化为单粒度）
SELECT
  '断言0: 样本=6 metric_name / 2 算子' AS test_description,
  CASE WHEN COUNT(DISTINCT metric_name) = 6
        AND COUNT(DISTINCT REGEXP_REPLACE(metric_name, '\\.[0-9]+\\.numRecordsOut$', '')) = 2
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM test_ingest_input
WHERE metric_name LIKE '%ConstraintEnforcer%numRecordsOut';

-- 断言1：三层聚合（复现 02 正文修复后的逻辑）
SELECT
  '断言1: 双粒度样本去重后 total=(0,54000,108000)、非空rps恒300、首桶NULL' AS test_description,
  CASE WHEN COUNT(*) = 3
        AND MIN(records_out_total) = 0 AND MAX(records_out_total) = 108000
        AND SUM(records_out_total) = 162000   -- 锁定中值 54000；若重复计数此处=324000
        AND SUM(CASE WHEN throughput_rps IS NOT NULL AND throughput_rps <> 300 THEN 1 ELSE 0 END) = 0
        AND SUM(CASE WHEN throughput_rps IS NULL THEN 1 ELSE 0 END) = 1
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
  SELECT
    time_bucket_minute,
    records_out_total,
    (records_out_total - LAG(records_out_total) OVER (ORDER BY time_bucket_minute))
      / NULLIF(UNIX_TIMESTAMP(time_bucket_minute)
               - LAG(UNIX_TIMESTAMP(time_bucket_minute)) OVER (ORDER BY time_bucket_minute), 0)
      AS throughput_rps
  FROM (
    SELECT time_bucket_minute, MAX(op_total) AS records_out_total
    FROM (
      SELECT time_bucket_minute, operator_name, SUM(subtask_cum) AS op_total
      FROM (
        SELECT
          time_bucket_minute,
          REGEXP_REPLACE(metric_name, '\\.[0-9]+\\.numRecordsOut$', '') AS operator_name,
          subtask_cum
        FROM (
          SELECT
            FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
            metric_name,
            MAX(CAST(metric_value AS DOUBLE)) AS subtask_cum
          FROM test_ingest_input
          WHERE metric_name LIKE '%ConstraintEnforcer%numRecordsOut'
          GROUP BY 1, metric_name
        ) m
      ) s
      GROUP BY time_bucket_minute, operator_name
    ) o
    GROUP BY time_bucket_minute
  ) t
) r;

-- 清理
DROP TABLE IF EXISTS test_ingest_input;

-- 预期输出：断言0/断言1 result 均为 PASS

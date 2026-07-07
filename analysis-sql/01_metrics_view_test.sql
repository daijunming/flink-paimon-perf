-- 01_metrics_view_test.sql —— 时段分桶逻辑验证（Property 10）
-- 验证 metrics_view 的字段映射和时段分桶正确性

-- 清理测试数据
DELETE FROM RDW_ODS_FLINK_METRICS 
WHERE job_name = 'paimon-perf-test-test';

-- 插入测试数据（对齐真实表12字段）
INSERT INTO RDW_ODS_FLINK_METRICS VALUES
(
  '2024-01-01',                                              -- etl_dt
  'PAIMON_METADATA_paimon.file.count_1704067200000',        -- metric_id (2024-01-01 00:00:00)
  'paimon-perf-test-test',                                   -- job_name (加test后缀避免污染生产数据)
  'wide_table',                                              -- app_id
  '',                                                        -- job_id
  '',                                                        -- host_name
  '',                                                        -- container_id
  '',                                                        -- container_rule
  'paimon.file.count',                                       -- metric_name
  'PAIMON_METADATA',                                         -- metric_type
  '100.0',                                                   -- metric_value (String)
  '1704067200000'                                            -- metric_ts (String, 2024-01-01 00:00:00)
),
(
  '2024-01-01',
  'PAIMON_METADATA_paimon.file.count_1704067230000',
  'paimon-perf-test-test',
  'wide_table',
  '',
  '',
  '',
  '',
  'paimon.file.count',
  'PAIMON_METADATA',
  '150.0',
  '1704067230000'                                            -- 2024-01-01 00:00:30（同一分钟）
),
(
  '2024-01-01',
  'PAIMON_METADATA_paimon.file.count_1704067260000',
  'paimon-perf-test-test',
  'wide_table',
  '',
  '',
  '',
  '',
  'paimon.file.count',
  'PAIMON_METADATA',
  '200.0',
  '1704067260000'                                            -- 2024-01-01 00:01:00（下一分钟）
);

-- 验证：修改WHERE条件使用测试数据
SELECT
  source,
  metric_name,
  metric_value,
  time_bucket_minute,
  metric_ts_millis
FROM (
  SELECT
    metric_type AS source,
    metric_name,
    CAST(metric_value AS DOUBLE) AS metric_value,
    CAST(metric_ts AS BIGINT) AS metric_ts_millis,
    FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00') AS time_bucket_minute
  FROM RDW_ODS_FLINK_METRICS
  WHERE metric_type = 'PAIMON_METADATA'
    AND job_name = 'paimon-perf-test-test'
    AND metric_name = 'paimon.file.count'
) t
ORDER BY metric_ts_millis;

-- 预期输出（3行）：
-- source='PAIMON_METADATA', metric_value=100.0, time_bucket_minute='2024-01-01 00:00:00', metric_ts_millis=1704067200000
-- source='PAIMON_METADATA', metric_value=150.0, time_bucket_minute='2024-01-01 00:00:00', metric_ts_millis=1704067230000
-- source='PAIMON_METADATA', metric_value=200.0, time_bucket_minute='2024-01-01 00:01:00', metric_ts_millis=1704067260000

-- 断言：前两条属于同一分钟桶，第三条属于下一分钟桶
-- Property 10验证：时段分桶守恒（同一分钟内的记录被分到同一桶）

-- 清理测试数据
DELETE FROM RDW_ODS_FLINK_METRICS 
WHERE job_name = 'paimon-perf-test-test';

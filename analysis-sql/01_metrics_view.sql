-- 01_metrics_view.sql —— 指标视图（时段分桶）
-- 对齐真实表 RDW_ODS_FLINK_METRICS 结构（已验证2024-01-01）

-- 字段映射说明：
--   metric_type → source (PAIMON_METADATA / YARN / HDFS)
--   metric_value (varchar) → CAST AS DOUBLE
--   metric_ts (varchar) → CAST AS BIGINT，再转为时段分桶
--   过滤条件：job_name='paimon-perf-test' AND app_id='wide_table'

CREATE OR REPLACE VIEW metrics_view AS
SELECT
  metric_type AS source,                                      -- 来源标识
  metric_name,                                                 -- 指标名
  CAST(metric_value AS DOUBLE) AS metric_value,              -- varchar→double
  CAST(metric_ts AS BIGINT) AS metric_ts_millis,             -- varchar→bigint
  FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
  job_name,
  app_id,
  job_id,
  host_name,
  etl_dt
FROM RDW_ODS_FLINK_METRICS
WHERE metric_type IN ('PAIMON_METADATA', 'YARN', 'HDFS')      -- 过滤测试相关指标
  AND job_name = 'paimon-perf-test'                           -- 过滤测试作业
  AND app_id = 'wide_table';                                  -- 过滤测试表

-- 04_baseline_compare.sql —— 基线对比（任务 8.7，Requirements 8.1）
-- 阶段1 聚合存为 baseline_metrics，阶段2 结果 JOIN 对比：绝对差 v−b、比率 v/b（b≠0）、优劣判定。

-- 步骤1：建立阶段1 基线表（手动执行一次，阶段1 完成后将聚合结果插入此表）
CREATE TABLE IF NOT EXISTS baseline_metrics (
  metric_category VARCHAR(50),  -- 指标类别（ingest_perf / resource / compaction 等）
  metric_name VARCHAR(100),     -- 具体指标名
  baseline_value DOUBLE,        -- 基线值（阶段1 的平均值或最大值）
  metric_unit VARCHAR(50)       -- 单位（rps / sec / count / bytes）
) DUPLICATE KEY(metric_category, metric_name)
DISTRIBUTED BY HASH(metric_category) BUCKETS 1;

-- 步骤2：阶段1 完成后，插入基线数据（示例，实际需从 metrics_ingest_perf 等视图聚合）
-- INSERT INTO baseline_metrics VALUES
--   ('ingest_perf', 'throughput_rps', 25000.0, 'rps'),
--   ('resource', 'yarn_allocated_vcores', 50.0, 'cores'),
--   ('compaction', 'paimon_file_count', 1000.0, 'count');

-- 步骤3：阶段2 运行时，实时对比当前值与基线
CREATE VIEW IF NOT EXISTS baseline_compare AS
SELECT
  b.metric_category,
  b.metric_name,
  b.baseline_value,
  c.current_value,
  -- 绝对差：v - b
  (c.current_value - b.baseline_value) AS abs_diff,
  -- 比率：v / b（b≠0）
  CASE WHEN b.baseline_value <> 0
       THEN c.current_value / b.baseline_value
       ELSE NULL END AS ratio,
  -- 优劣判定（吞吐/资源利用率类指标：高为优；延迟/文件数类指标：低为优）
  CASE
    WHEN b.metric_name LIKE '%throughput%' OR b.metric_name LIKE '%qps%' THEN
      CASE WHEN c.current_value >= b.baseline_value THEN 'BETTER' ELSE 'WORSE' END
    WHEN b.metric_name LIKE '%latency%' OR b.metric_name LIKE '%file_count%' THEN
      CASE WHEN c.current_value <= b.baseline_value THEN 'BETTER' ELSE 'WORSE' END
    ELSE 'NEUTRAL'
  END AS trend,
  c.time_bucket_minute
FROM baseline_metrics b
LEFT JOIN (
  -- 当前阶段2 指标（从四类指标视图汇总，简化示意）
  SELECT
    'ingest_perf' AS metric_category,
    'throughput_rps' AS metric_name,
    throughput_rps AS current_value,
    time_bucket_minute
  FROM sla_check
  WHERE time_bucket_minute >= '2024-01-02 00:00:00'  -- 阶段2 开始时间（实际由编排脚本注入）
  
  UNION ALL
  
  SELECT
    'resource' AS metric_category,
    'yarn_allocated_vcores' AS metric_name,
    yarn_allocated_vcores AS current_value,
    time_bucket_minute
  FROM metrics_resource_compaction
  WHERE time_bucket_minute >= '2024-01-02 00:00:00'
  
  UNION ALL
  
  SELECT
    'compaction' AS metric_category,
    'paimon_file_count' AS metric_name,
    paimon_file_count AS current_value,
    time_bucket_minute
  FROM metrics_resource_compaction
  WHERE time_bucket_minute >= '2024-01-02 00:00:00'
) c ON b.metric_category = c.metric_category AND b.metric_name = c.metric_name
ORDER BY c.time_bucket_minute, b.metric_category, b.metric_name;

-- 说明：
-- 1. baseline_metrics 表需在阶段1 完成后手动（或由编排脚本）INSERT 基线值。
-- 2. baseline_compare 视图实时对比阶段2 当前值与基线，输出绝对差/比率/优劣趋势。
-- 3. 优劣判定规则：吞吐类指标"高为优"、延迟/文件数类指标"低为优"，可按需调整。
-- 4. 时间过滤 `>= '2024-01-02'` 由编排脚本注入阶段2 开始时间（或用标签区分阶段）。

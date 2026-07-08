-- 01_metrics_view_test.sql —— 时段分桶逻辑验证（Property 10）
-- 自包含测试：用临时输入表复现 metrics_view 的字段映射与分钟分桶表达式，断言分桶守恒。
-- 不触碰真实分区表 RDW_ODS_FLINK_METRICS（避免分区不存在 / 污染 / 误删风险），
-- 与 03/04/05 测试同风格（本地即可执行逻辑校验，无需真实数据）。

DROP TABLE IF EXISTS test_metrics_view_input;

-- 模拟真实表的原始字段类型：metric_value / metric_ts 均为 varchar
CREATE TABLE test_metrics_view_input (
  test_case     VARCHAR(100),
  metric_type   VARCHAR(50),
  metric_name   VARCHAR(100),
  metric_value  VARCHAR(50),
  metric_ts     VARCHAR(50)
) DUPLICATE KEY(test_case)
DISTRIBUTED BY HASH(test_case) BUCKETS 1;

-- 三条样本：前两条同一分钟（00:00:00 / 00:00:30），第三条下一分钟（00:01:00）
INSERT INTO test_metrics_view_input VALUES
  ('t0_同分钟',   'PAIMON_METADATA', 'paimon.file.count', '100.0', '946684800000'),
  ('t1_同分钟',   'PAIMON_METADATA', 'paimon.file.count', '150.0', '946684830000'),
  ('t2_下一分钟', 'PAIMON_METADATA', 'paimon.file.count', '200.0', '946684860000');

-- 复现 metrics_view 的映射 + 分桶表达式（与 01_metrics_view.sql 保持一致）
SELECT
  metric_type AS source,
  metric_name,
  CAST(metric_value AS DOUBLE) AS metric_value,
  CAST(metric_ts AS BIGINT) AS metric_ts_millis,
  FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00') AS time_bucket_minute
FROM test_metrics_view_input
ORDER BY CAST(metric_ts AS BIGINT);

-- 预期（3 行）：前两条 time_bucket_minute 相同（同一分钟桶），第三条为下一分钟。

-- 断言：3 条样本恰好落入 2 个分钟桶（Property 10：分钟分桶守恒）
SELECT
  '断言: 3条样本落入2个分钟桶' AS test_description,
  COUNT(DISTINCT FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00')) AS distinct_buckets,
  CASE WHEN COUNT(DISTINCT FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00')) = 2
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM test_metrics_view_input;

-- 清理
DROP TABLE IF EXISTS test_metrics_view_input;

-- 预期输出：断言 result = PASS

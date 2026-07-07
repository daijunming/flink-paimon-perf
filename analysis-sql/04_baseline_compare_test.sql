-- 04_baseline_compare_test.sql —— 基线对比逻辑验证（对应任务 8.8 Property 8）
-- 验证：给定基线值与当前值，断言绝对差/比率/优劣判定计算正确。

DROP TABLE IF EXISTS test_baseline_input;
DROP TABLE IF EXISTS test_baseline_result;

-- 创建测试输入表（基线值 vs 当前值）
CREATE TABLE test_baseline_input (
  test_case VARCHAR(100),
  metric_name VARCHAR(50),
  baseline_value DOUBLE,
  current_value DOUBLE,
  expected_abs_diff DOUBLE,
  expected_ratio DOUBLE,
  expected_trend VARCHAR(10)
) DUPLICATE KEY(test_case)
DISTRIBUTED BY HASH(test_case) BUCKETS 1;

-- 插入测试用例：覆盖吞吐类（高为优）/ 延迟类（低为优）/ 零基线边界
INSERT INTO test_baseline_input VALUES
  ('吞吐提升: 25000→28000', 'throughput_rps', 25000.0, 28000.0, 3000.0, 1.12, 'BETTER'),
  ('吞吐下降: 25000→22000', 'throughput_rps', 25000.0, 22000.0, -3000.0, 0.88, 'WORSE'),
  ('延迟降低: 150→120', 'e2e_latency_sec', 150.0, 120.0, -30.0, 0.8, 'BETTER'),
  ('延迟升高: 150→180', 'e2e_latency_sec', 150.0, 180.0, 30.0, 1.2, 'WORSE'),
  ('文件数降低: 1000→800', 'paimon_file_count', 1000.0, 800.0, -200.0, 0.8, 'BETTER'),
  ('零基线: 0→100', 'new_metric', 0.0, 100.0, 100.0, NULL, 'NEUTRAL');

-- 执行基线对比逻辑（模拟 04_baseline_compare.sql）
CREATE TABLE test_baseline_result AS
SELECT
  test_case,
  metric_name,
  baseline_value,
  current_value,
  (current_value - baseline_value) AS actual_abs_diff,
  expected_abs_diff,
  CASE WHEN baseline_value <> 0 THEN current_value / baseline_value ELSE NULL END AS actual_ratio,
  expected_ratio,
  CASE
    WHEN metric_name LIKE '%throughput%' OR metric_name LIKE '%qps%' THEN
      CASE WHEN current_value >= baseline_value THEN 'BETTER' ELSE 'WORSE' END
    WHEN metric_name LIKE '%latency%' OR metric_name LIKE '%file_count%' THEN
      CASE WHEN current_value <= baseline_value THEN 'BETTER' ELSE 'WORSE' END
    ELSE 'NEUTRAL'
  END AS actual_trend,
  expected_trend
FROM test_baseline_input;

-- 断言1：绝对差计算正确
SELECT
  '断言1: 绝对差 = current - baseline' AS test_description,
  COUNT(*) AS total_cases,
  SUM(CASE WHEN ABS(actual_abs_diff - expected_abs_diff) < 0.01 THEN 1 ELSE 0 END) AS passed_cases,
  CASE WHEN COUNT(*) = SUM(CASE WHEN ABS(actual_abs_diff - expected_abs_diff) < 0.01 THEN 1 ELSE 0 END)
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM test_baseline_result;

-- 断言2：比率计算正确（排除 NULL）
SELECT
  '断言2: 比率 = current / baseline (baseline≠0)' AS test_description,
  COUNT(*) AS total_cases,
  SUM(CASE WHEN (actual_ratio IS NULL AND expected_ratio IS NULL)
                OR ABS(actual_ratio - expected_ratio) < 0.01 THEN 1 ELSE 0 END) AS passed_cases,
  CASE WHEN COUNT(*) = SUM(CASE WHEN (actual_ratio IS NULL AND expected_ratio IS NULL)
                                     OR ABS(actual_ratio - expected_ratio) < 0.01 THEN 1 ELSE 0 END)
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM test_baseline_result;

-- 断言3：优劣判定正确
SELECT
  '断言3: 优劣趋势判定' AS test_description,
  COUNT(*) AS total_cases,
  SUM(CASE WHEN actual_trend = expected_trend THEN 1 ELSE 0 END) AS passed_cases,
  CASE WHEN COUNT(*) = SUM(CASE WHEN actual_trend = expected_trend THEN 1 ELSE 0 END)
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM test_baseline_result;

-- 详细结果
SELECT * FROM test_baseline_result ORDER BY test_case;

-- 清理
DROP TABLE test_baseline_input;
DROP TABLE test_baseline_result;

-- 预期输出：三个断言全部 PASS

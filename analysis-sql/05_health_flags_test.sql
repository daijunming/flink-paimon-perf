-- 05_health_flags_test.sql —— 健康标志逻辑验证
-- 自包含测试：用临时表复现 health_flags 的软标志判定（l0_flag / backpressure_flag / compaction_flag），断言正确。
-- 不触碰真实表；与其它 _test.sql 同风格。

DROP TABLE IF EXISTS test_health_input;
DROP TABLE IF EXISTS test_health_result;

CREATE TABLE test_health_input (
  test_case VARCHAR(100),
  level0_file_count DOUBLE,
  max_checkpoint_start_delay_ms DOUBLE,
  compaction_thread_busy_max DOUBLE,
  expected_l0_flag VARCHAR(24),
  expected_backpressure_flag VARCHAR(24),
  expected_compaction_flag VARCHAR(24)
) DUPLICATE KEY(test_case)
DISTRIBUTED BY HASH(test_case) BUCKETS 1;

-- 覆盖：正常 / L0 堆积 / 反压 / Compaction 饱和 / 边界 / 多触发
INSERT INTO test_health_input VALUES
  ('正常',                  100,   5000, 50, 'OK',        'OK',           'OK'),
  ('L0边界(=1000不触发)',   1000,  5000, 50, 'OK',        'OK',           'OK'),
  ('L0堆积(>1000)',         1500,  5000, 50, 'L0_PILEUP', 'OK',           'OK'),
  ('反压(>30000ms)',        100,  45000, 50, 'OK',        'BACKPRESSURE', 'OK'),
  ('Compaction饱和(>90)',   100,   5000, 95, 'OK',        'OK',           'COMPACTION_SATURATED'),
  ('合不过来(L0堆积+饱和)', 2000,  5000, 98, 'L0_PILEUP', 'OK',           'COMPACTION_SATURATED'),
  ('全触发',                2000, 60000, 99, 'L0_PILEUP', 'BACKPRESSURE', 'COMPACTION_SATURATED');

CREATE TABLE test_health_result AS
SELECT
  test_case,
  CASE WHEN level0_file_count > 1000 THEN 'L0_PILEUP' ELSE 'OK' END AS actual_l0_flag,
  expected_l0_flag,
  CASE WHEN max_checkpoint_start_delay_ms > 30000 THEN 'BACKPRESSURE' ELSE 'OK' END AS actual_backpressure_flag,
  expected_backpressure_flag,
  CASE WHEN compaction_thread_busy_max > 90 THEN 'COMPACTION_SATURATED' ELSE 'OK' END AS actual_compaction_flag,
  expected_compaction_flag
FROM test_health_input;

-- 断言：三个软标志判定均与预期一致
SELECT
  '断言: 健康标志判定正确' AS test_description,
  COUNT(*) AS total_cases,
  SUM(CASE WHEN actual_l0_flag = expected_l0_flag
            AND actual_backpressure_flag = expected_backpressure_flag
            AND actual_compaction_flag = expected_compaction_flag THEN 1 ELSE 0 END) AS passed_cases,
  CASE WHEN COUNT(*) = SUM(CASE WHEN actual_l0_flag = expected_l0_flag
            AND actual_backpressure_flag = expected_backpressure_flag
            AND actual_compaction_flag = expected_compaction_flag THEN 1 ELSE 0 END)
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM test_health_result;

-- 详细结果
SELECT * FROM test_health_result ORDER BY test_case;

-- 清理
DROP TABLE test_health_input;
DROP TABLE test_health_result;

-- 预期输出：7 个用例全部 PASS

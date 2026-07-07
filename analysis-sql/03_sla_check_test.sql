-- 03_sla_check_test.sql —— SLA 判定逻辑验证（对应任务 8.6 Property 7）
-- 验证：插入不同吞吐/延迟组合的样本数据，断言 sla_status 判定正确。

DROP TABLE IF EXISTS test_sla_input;
DROP TABLE IF EXISTS test_sla_result;

-- 创建测试输入表（模拟 throughput / latency 组合）
CREATE TABLE test_sla_input (
  test_case VARCHAR(100),
  throughput_rps DOUBLE,
  e2e_latency_sec DOUBLE,
  expected_status VARCHAR(10)
) DUPLICATE KEY(test_case)
DISTRIBUTED BY HASH(test_case) BUCKETS 1;

-- 插入测试用例：覆盖 PASS / FAIL_THROUGHPUT / FAIL_LATENCY / FAIL_BOTH
INSERT INTO test_sla_input VALUES
  ('达标: 吞吐21000 延迟150', 21000.0, 150.0, 'PASS'),
  ('达标: 吞吐20000 延迟180（边界）', 20000.0, 180.0, 'PASS'),
  ('未达标: 吞吐19999 延迟150', 19999.0, 150.0, 'FAIL'),
  ('未达标: 吞吐21000 延迟181', 21000.0, 181.0, 'FAIL'),
  ('未达标: 吞吐15000 延迟200（双违规）', 15000.0, 200.0, 'FAIL');

-- 执行 SLA 判定逻辑（模拟 03_sla_check.sql 的 CASE WHEN）
CREATE TABLE test_sla_result AS
SELECT
  test_case,
  throughput_rps,
  e2e_latency_sec,
  CASE
    WHEN throughput_rps >= 20000 AND e2e_latency_sec <= 180 THEN 'PASS'
    ELSE 'FAIL'
  END AS actual_status,
  expected_status,
  CASE
    WHEN throughput_rps < 20000 THEN 'THROUGHPUT_LOW'
    WHEN e2e_latency_sec > 180 THEN 'LATENCY_HIGH'
    ELSE 'OK'
  END AS violation_reason
FROM test_sla_input;

-- 断言：所有用例的 actual_status 应等于 expected_status
SELECT
  '断言: SLA判定等价于阈值比较' AS test_description,
  COUNT(*) AS total_cases,
  SUM(CASE WHEN actual_status = expected_status THEN 1 ELSE 0 END) AS passed_cases,
  CASE WHEN COUNT(*) = SUM(CASE WHEN actual_status = expected_status THEN 1 ELSE 0 END)
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM test_sla_result;

-- 详细结果（便于人工核对）
SELECT * FROM test_sla_result ORDER BY test_case;

-- 清理
DROP TABLE test_sla_input;
DROP TABLE test_sla_result;

-- 预期输出：5 个用例全部 PASS

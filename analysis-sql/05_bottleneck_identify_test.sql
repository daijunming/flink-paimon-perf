-- 05_bottleneck_identify_test.sql —— 瓶颈定位逻辑验证（对应任务 8.10 Property 9）
-- 验证：给定不同指标组合，断言瓶颈类别判定正确。

DROP TABLE IF EXISTS test_bottleneck_input;
DROP TABLE IF EXISTS test_bottleneck_result;

-- 创建测试输入表（模拟 SLA 状态 + 四类指标）
CREATE TABLE test_bottleneck_input (
  test_case VARCHAR(100),
  sla_status VARCHAR(10),
  throughput_rps DOUBLE,
  e2e_latency_sec DOUBLE,
  yarn_allocated_vcores DOUBLE,
  yarn_available_vcores DOUBLE,
  paimon_file_count DOUBLE,
  compact_count INT,
  total_commits INT,
  expected_bottleneck VARCHAR(50)
) DUPLICATE KEY(test_case)
DISTRIBUTED BY HASH(test_case) BUCKETS 1;

-- 插入测试用例：覆盖 NONE / RESOURCE_CPU / COMPACTION / WRITE_CONCURRENCY
INSERT INTO test_bottleneck_input VALUES
  ('达标: 全指标正常', 'PASS', 21000, 150, 40, 60, 1000, 10, 100, 'NONE'),
  
  ('资源瓶颈: YARN CPU>80%', 'FAIL', 18000, 150, 85, 15, 1000, 10, 100, 'RESOURCE_CPU'),
  
  ('Compaction瓶颈: 文件数>5000', 'FAIL', 21000, 200, 40, 60, 6000, 10, 100, 'COMPACTION'),
  
  ('Compaction瓶颈: Compact占比>50%', 'FAIL', 21000, 200, 40, 60, 2000, 60, 100, 'COMPACTION'),
  
  ('并发瓶颈: 吞吐低但资源正常', 'FAIL', 15000, 150, 40, 60, 1000, 10, 100, 'WRITE_CONCURRENCY'),
  
  ('延迟瓶颈: 延迟高归因Compaction', 'FAIL', 21000, 200, 40, 60, 1000, 10, 100, 'COMPACTION');

-- 执行瓶颈判定逻辑（模拟 05_bottleneck_identify.sql）
CREATE TABLE test_bottleneck_result AS
SELECT
  test_case,
  sla_status,
  CASE
    WHEN sla_status = 'PASS' THEN 'NONE'
    WHEN yarn_allocated_vcores / NULLIF(yarn_allocated_vcores + yarn_available_vcores, 0) > 0.8
         THEN 'RESOURCE_CPU'
    WHEN paimon_file_count > 5000
      OR (compact_count / NULLIF(total_commits, 0) > 0.5)
         THEN 'COMPACTION'
    WHEN throughput_rps < 20000 THEN 'WRITE_CONCURRENCY'
    WHEN e2e_latency_sec > 180 THEN 'COMPACTION'
    ELSE 'UNKNOWN'
  END AS actual_bottleneck,
  expected_bottleneck
FROM test_bottleneck_input;

-- 断言：所有用例的瓶颈判定应等于预期
SELECT
  '断言: 瓶颈定位逻辑正确' AS test_description,
  COUNT(*) AS total_cases,
  SUM(CASE WHEN actual_bottleneck = expected_bottleneck THEN 1 ELSE 0 END) AS passed_cases,
  CASE WHEN COUNT(*) = SUM(CASE WHEN actual_bottleneck = expected_bottleneck THEN 1 ELSE 0 END)
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM test_bottleneck_result;

-- 详细结果（便于人工核对）
SELECT * FROM test_bottleneck_result ORDER BY test_case;

-- 清理
DROP TABLE test_bottleneck_input;
DROP TABLE test_bottleneck_result;

-- 预期输出：6 个用例全部 PASS

-- 08_checkpoint_health_test.sql —— 快照推进健康度验证
-- 自包含测试：用临时表构造"正常→停滞→恢复"的快照序列，复现 checkpoint_health 的 LAG 逻辑，
-- 断言停滞时段（snapshot_id 增量=0）被标记为 STALL。不触碰真实分区表。

DROP TABLE IF EXISTS test_ckpt_input;

-- 模拟 checkpoint_health 内层子查询的输出（每分钟一行的快照号/提交时间）
CREATE TABLE test_ckpt_input (
  job_name VARCHAR(50),
  time_bucket_minute VARCHAR(20),
  snapshot_id DOUBLE,
  snapshot_commit_time_millis DOUBLE
) DUPLICATE KEY(job_name, time_bucket_minute)
DISTRIBUTED BY HASH(job_name) BUCKETS 1;

-- 场景：t0=100 → t1=101(+1,间隔60s) → t2=101(停滞,delta=0) → t3=103(恢复,+2)
INSERT INTO test_ckpt_input VALUES
  ('wide_table', '2000-01-01 00:00:00', 100, 946684800000),
  ('wide_table', '2000-01-01 00:01:00', 101, 946684860000),
  ('wide_table', '2000-01-01 00:02:00', 101, 946684860000),
  ('wide_table', '2000-01-01 00:03:00', 103, 946684980000);

-- 复现 checkpoint_health + checkpoint_stall_alert 的判定逻辑
SELECT
  job_name,
  time_bucket_minute,
  snapshot_id,
  snapshot_id_delta,
  commit_interval_sec,
  CASE
    WHEN snapshot_id_delta = 0 THEN 'STALL'
    WHEN commit_interval_sec > 180 THEN 'SLOW'
    ELSE 'OK'
  END AS health_status
FROM (
  SELECT
    job_name,
    time_bucket_minute,
    snapshot_id,
    snapshot_id - LAG(snapshot_id) OVER (
        PARTITION BY job_name ORDER BY time_bucket_minute) AS snapshot_id_delta,
    (snapshot_commit_time_millis - LAG(snapshot_commit_time_millis) OVER (
        PARTITION BY job_name ORDER BY time_bucket_minute)) / 1000.0 AS commit_interval_sec
  FROM test_ckpt_input
) t
ORDER BY time_bucket_minute;

-- 预期（4行）：
-- t0 00:00: delta=NULL, interval=NULL, OK（首行无前值）
-- t1 00:01: delta=1,    interval=60,   OK
-- t2 00:02: delta=0,    interval=0,    STALL（快照停滞）
-- t3 00:03: delta=2,    interval=120,  OK

-- 断言：恰好 1 个时段被判为 STALL（增量=0）
SELECT
  '断言: 停滞检测（delta=0 → STALL）' AS test_description,
  SUM(CASE WHEN snapshot_id_delta = 0 THEN 1 ELSE 0 END) AS stall_count,
  CASE WHEN SUM(CASE WHEN snapshot_id_delta = 0 THEN 1 ELSE 0 END) = 1
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
  SELECT snapshot_id - LAG(snapshot_id) OVER (
           PARTITION BY job_name ORDER BY time_bucket_minute) AS snapshot_id_delta
  FROM test_ckpt_input
) t;

-- 清理
DROP TABLE IF EXISTS test_ckpt_input;

-- 预期输出：断言 result = PASS

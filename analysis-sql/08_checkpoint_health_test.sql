-- 08_checkpoint_health_test.sql —— 快照推进健康度验证
-- 构造三个时段的快照数据：正常推进 → 停滞 → 恢复，验证 STALL 检测正确。

-- 清理测试数据
DELETE FROM RDW_ODS_FLINK_METRICS WHERE job_name = 'ckpt_health_test';

-- 插入测试数据（对齐真实表12字段）
-- 场景：t0 快照=100, t1 快照=101（正常+1）, t2 快照=101（停滞）, t3 快照=103（恢复）
INSERT INTO RDW_ODS_FLINK_METRICS VALUES
-- t0: 2024-01-01 00:00:00, snapshot_id=100, commit_time=00:00:00
('2024-01-01', 'PAIMON_METADATA_ckpt_health_test_paimon.snapshot.id_1704067200000',
 'ckpt_health_test', 'paimon_table_mornit', '', '', '', '',
 'paimon.snapshot.id', 'PAIMON_METADATA', '100.0', '1704067200000'),
('2024-01-01', 'PAIMON_METADATA_ckpt_health_test_paimon.snapshot.time.millis_1704067200000',
 'ckpt_health_test', 'paimon_table_mornit', '', '', '', '',
 'paimon.snapshot.time.millis', 'PAIMON_METADATA', '1704067200000.0', '1704067200000'),

-- t1: 00:01:00, snapshot_id=101（正常推进 +1），commit_time=00:01:00（间隔60秒）
('2024-01-01', 'PAIMON_METADATA_ckpt_health_test_paimon.snapshot.id_1704067260000',
 'ckpt_health_test', 'paimon_table_mornit', '', '', '', '',
 'paimon.snapshot.id', 'PAIMON_METADATA', '101.0', '1704067260000'),
('2024-01-01', 'PAIMON_METADATA_ckpt_health_test_paimon.snapshot.time.millis_1704067260000',
 'ckpt_health_test', 'paimon_table_mornit', '', '', '', '',
 'paimon.snapshot.time.millis', 'PAIMON_METADATA', '1704067260000.0', '1704067260000'),

-- t2: 00:02:00, snapshot_id=101（停滞，增量=0）
('2024-01-01', 'PAIMON_METADATA_ckpt_health_test_paimon.snapshot.id_1704067320000',
 'ckpt_health_test', 'paimon_table_mornit', '', '', '', '',
 'paimon.snapshot.id', 'PAIMON_METADATA', '101.0', '1704067320000'),
('2024-01-01', 'PAIMON_METADATA_ckpt_health_test_paimon.snapshot.time.millis_1704067320000',
 'ckpt_health_test', 'paimon_table_mornit', '', '', '', '',
 'paimon.snapshot.time.millis', 'PAIMON_METADATA', '1704067260000.0', '1704067320000'),

-- t3: 00:03:00, snapshot_id=103（恢复推进 +2）
('2024-01-01', 'PAIMON_METADATA_ckpt_health_test_paimon.snapshot.id_1704067380000',
 'ckpt_health_test', 'paimon_table_mornit', '', '', '', '',
 'paimon.snapshot.id', 'PAIMON_METADATA', '103.0', '1704067380000'),
('2024-01-01', 'PAIMON_METADATA_ckpt_health_test_paimon.snapshot.time.millis_1704067380000',
 'ckpt_health_test', 'paimon_table_mornit', '', '', '', '',
 'paimon.snapshot.time.millis', 'PAIMON_METADATA', '1704067380000.0', '1704067380000');

-- 验证1：快照推进明细（用内联子查询复现 checkpoint_health 逻辑，限定测试表）
SELECT
  job_name,
  time_bucket_minute,
  snapshot_id,
  snapshot_id - LAG(snapshot_id) OVER (
      PARTITION BY job_name ORDER BY time_bucket_minute) AS snapshot_id_delta,
  (snapshot_commit_time_millis - LAG(snapshot_commit_time_millis) OVER (
      PARTITION BY job_name ORDER BY time_bucket_minute)) / 1000.0 AS commit_interval_sec
FROM (
  SELECT
    job_name,
    FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
    MAX(CASE WHEN metric_name = 'paimon.snapshot.id' THEN CAST(metric_value AS DOUBLE) END) AS snapshot_id,
    MAX(CASE WHEN metric_name = 'paimon.snapshot.time.millis' THEN CAST(metric_value AS DOUBLE) END) AS snapshot_commit_time_millis
  FROM RDW_ODS_FLINK_METRICS
  WHERE metric_type = 'PAIMON_METADATA'
    AND job_name = 'ckpt_health_test'
    AND metric_name IN ('paimon.snapshot.id', 'paimon.snapshot.time.millis')
  GROUP BY job_name, FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00')
) t
ORDER BY time_bucket_minute;

-- 预期输出（4行）：
-- t0 00:00: snapshot_id=100, delta=NULL,  interval=NULL
-- t1 00:01: snapshot_id=101, delta=1,     interval=60   （正常）
-- t2 00:02: snapshot_id=101, delta=0,     interval=0    （STALL：快照停滞）
-- t3 00:03: snapshot_id=103, delta=2,     interval=120  （恢复）

-- 断言：t2 的 snapshot_id_delta=0 → 应被 checkpoint_stall_alert 标记为 STALL
-- 断言：snapshot_id 整体单调不减（100→101→101→103），符合快照演进语义

-- 清理测试数据
DELETE FROM RDW_ODS_FLINK_METRICS WHERE job_name = 'ckpt_health_test';

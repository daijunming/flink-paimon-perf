-- 08_checkpoint_health_test.sql —— 快照推进 + 数据可见新鲜度验证
-- 自包含测试：用临时表模拟 rdw_ods_paimon_meta_snapshots 的列形态（每轮全量重采，同轮共享
-- collected_at），构造"正常推进→停滞→恢复但间隔超标"的轮次序列，复现 checkpoint_health /
-- checkpoint_stall_alert 改接 ODS 后的判定逻辑（2026-08-11）。断言：
--   正常推进（增量>0 且间隔 ≤300s）→ OK；增量=0 → STALL；提交间隔 >5min（300s）→ STALE。
-- 不触碰真实分区表；与其它 _test.sql 同风格。

DROP TABLE IF EXISTS test_ckpt_snapshots;

CREATE TABLE test_ckpt_snapshots (
  table_name VARCHAR(32),
  snapshot_id BIGINT,
  collected_at DATETIME,
  commit_kind VARCHAR(32),
  commit_time DATETIME
) DUPLICATE KEY(table_name, snapshot_id, collected_at)
DISTRIBUTED BY HASH(table_name) BUCKETS 1;

-- 场景（轮次约 3 分钟一轮，同轮共享 collected_at；同一快照跨轮重复出现 = 全量重采覆盖）：
--   R1 00:00:10 最新 101（首行无前值）
--   R2 00:03:10 最新 102（+1，提交间隔 180s，OK）
--   R3 00:06:10 最新 102（停滞，delta=0，STALL）
--   R4 00:09:10 最新 103（+1，但距上次提交 360s > 300s，STALE 新鲜度超标）
INSERT INTO test_ckpt_snapshots VALUES
  ('wide_table', 100, '2000-01-01 00:00:10', 'APPEND', '2000-01-01 00:00:00'),
  ('wide_table', 101, '2000-01-01 00:00:10', 'APPEND', '2000-01-01 00:00:00'),
  ('wide_table', 101, '2000-01-01 00:03:10', 'APPEND', '2000-01-01 00:00:00'),
  ('wide_table', 102, '2000-01-01 00:03:10', 'APPEND', '2000-01-01 00:03:00'),
  ('wide_table', 102, '2000-01-01 00:06:10', 'APPEND', '2000-01-01 00:03:00'),
  ('wide_table', 102, '2000-01-01 00:09:10', 'APPEND', '2000-01-01 00:03:00'),
  ('wide_table', 103, '2000-01-01 00:09:10', 'APPEND', '2000-01-01 00:09:00');

-- 复现 checkpoint_health + checkpoint_stall_alert 的判定逻辑（与视图同一套表达式）
SELECT
  table_name,
  collect_round_time,
  latest_snapshot_id,
  latest_commit_time,
  snapshot_id_delta,
  commit_interval_sec,
  CASE
    WHEN snapshot_id_delta = 0 THEN 'STALL'
    WHEN commit_interval_sec > 300 THEN 'STALE'
    ELSE 'OK'
  END AS health_status
FROM (
  SELECT
    table_name,
    collected_at AS collect_round_time,
    latest_snapshot_id,
    latest_commit_time,
    latest_snapshot_id - LAG(latest_snapshot_id) OVER (
        PARTITION BY table_name ORDER BY collected_at) AS snapshot_id_delta,
    TIMESTAMPDIFF(SECOND,
        LAG(latest_commit_time) OVER (PARTITION BY table_name ORDER BY collected_at),
        latest_commit_time) AS commit_interval_sec
  FROM (
    SELECT
      table_name,
      collected_at,
      MAX(snapshot_id) AS latest_snapshot_id,
      MAX(commit_time) AS latest_commit_time
    FROM test_ckpt_snapshots
    WHERE table_name = 'wide_table'
    GROUP BY table_name, collected_at
  ) r
) t
ORDER BY collect_round_time;

-- 预期（4行）：
-- R1 00:00:10: delta=NULL,  interval=NULL, OK（首行无前值）
-- R2 00:03:10: delta=1,     interval=180,  OK（正常推进）
-- R3 00:06:10: delta=0,     interval=0,    STALL（快照停滞）
-- R4 00:09:10: delta=1,     interval=360,  STALE（提交间隔 >5min，新鲜度超标）

-- 断言：正常推进 1 轮（R2），STALL 恰好 1 轮（R3），STALE 恰好 1 轮（R4）
SELECT
  '断言: 正常推进=1 且 STALL=1 且 STALE=1' AS test_description,
  SUM(CASE WHEN snapshot_id_delta > 0 AND commit_interval_sec <= 300 THEN 1 ELSE 0 END) AS ok_count,
  SUM(CASE WHEN snapshot_id_delta = 0 THEN 1 ELSE 0 END) AS stall_count,
  SUM(CASE WHEN snapshot_id_delta <> 0 AND commit_interval_sec > 300 THEN 1 ELSE 0 END) AS stale_count,
  CASE WHEN SUM(CASE WHEN snapshot_id_delta > 0 AND commit_interval_sec <= 300 THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN snapshot_id_delta = 0 THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN snapshot_id_delta <> 0 AND commit_interval_sec > 300 THEN 1 ELSE 0 END) = 1
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
  SELECT
    latest_snapshot_id - LAG(latest_snapshot_id) OVER (
        PARTITION BY table_name ORDER BY collected_at) AS snapshot_id_delta,
    TIMESTAMPDIFF(SECOND,
        LAG(latest_commit_time) OVER (PARTITION BY table_name ORDER BY collected_at),
        latest_commit_time) AS commit_interval_sec
  FROM (
    SELECT
      table_name,
      collected_at,
      MAX(snapshot_id) AS latest_snapshot_id,
      MAX(commit_time) AS latest_commit_time
    FROM test_ckpt_snapshots
    WHERE table_name = 'wide_table'
    GROUP BY table_name, collected_at
  ) r
) t;

-- 清理
DROP TABLE IF EXISTS test_ckpt_snapshots;

-- 预期输出：断言 result = PASS

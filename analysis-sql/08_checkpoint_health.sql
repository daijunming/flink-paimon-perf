-- 08_checkpoint_health.sql —— 快照推进健康度 + 数据可见新鲜度
-- 利用 Paimon 元数据采集器已采集的 paimon.snapshot.id / paimon.snapshot.time.millis，
-- 验证 Flink checkpoint → Paimon commit 是否正常推进，并量化"数据可见新鲜度"。视图统一建在 RDW_DATA。
--
-- 为什么这就是新鲜度：Paimon 主键表的数据对下游可见 = snapshot 提交 = 写作业 checkpoint。
-- 所以"相邻快照提交间隔"≈ 数据从写入到可见的最大延迟。健康的分钟级引擎应满足：
--   1. snapshot_id 随时间单调递增（每次 checkpoint 产生新快照）；
--   2. 提交间隔稳定且 ≤ 新鲜度目标（本测试 ≤5min）。
-- 若 snapshot_id 长时间不变（增量=0）→ checkpoint 失败/写入阻塞，数据不再更新（新鲜度彻底破坏）。
--
-- 数据来源：job_name='wide_table' 的 Paimon 元数据（snapshot.id / snapshot.time.millis）。

-- ==================== 视图1：快照推进明细 ====================
-- 按时段取每个表的最新 snapshot_id 与提交时间，并用窗口函数算相邻增量与提交间隔。
CREATE OR REPLACE VIEW RDW_DATA.checkpoint_health AS
SELECT
  job_name,                                            -- 被监测的表名（多表区分）
  time_bucket_minute,
  snapshot_id,
  snapshot_commit_time_millis,
  -- 相邻时段快照号增量：>0 正常推进，=0 停滞，NULL 为首行
  snapshot_id - LAG(snapshot_id) OVER (
      PARTITION BY job_name ORDER BY time_bucket_minute
  ) AS snapshot_id_delta,
  -- 相邻快照提交时间间隔（秒）≈ 数据可见新鲜度：反映数据从写入到可见的最大延迟
  (snapshot_commit_time_millis - LAG(snapshot_commit_time_millis) OVER (
      PARTITION BY job_name ORDER BY time_bucket_minute
  )) / 1000.0 AS commit_interval_sec
FROM (
  -- 每个表 × 每分钟取最大快照号（同分钟多次采集取最新）
  SELECT
    job_name,
    time_bucket_minute,
    MAX(CASE WHEN metric_name = 'paimon.snapshot.id'
             THEN metric_value END) AS snapshot_id,
    MAX(CASE WHEN metric_name = 'paimon.snapshot.time.millis'
             THEN metric_value END) AS snapshot_commit_time_millis
  FROM RDW_DATA.metrics_view
  WHERE source = 'PAIMON_METADATA'
    AND metric_name IN ('paimon.snapshot.id', 'paimon.snapshot.time.millis')
  GROUP BY job_name, time_bucket_minute
) t;

-- ==================== 视图2：新鲜度 / 停滞告警 ====================
-- 筛选快照停滞（增量=0）或数据可见新鲜度超标（提交间隔 > 5min）的时段。
CREATE OR REPLACE VIEW RDW_DATA.checkpoint_stall_alert AS
SELECT
  job_name,
  time_bucket_minute,
  snapshot_id,
  snapshot_id_delta,
  commit_interval_sec,
  CASE
    WHEN snapshot_id_delta = 0 THEN 'STALL'
    WHEN commit_interval_sec > 300 THEN 'STALE'      -- 提交间隔 >5min，数据可见新鲜度超标
    ELSE 'OK'
  END AS health_status,
  CASE
    WHEN snapshot_id_delta = 0
         THEN CONCAT('快照停滞: snapshot_id 持续为 ', snapshot_id, '（checkpoint 失败/写入阻塞，数据不再更新）')
    WHEN commit_interval_sec > 300
         THEN CONCAT('新鲜度超标: 提交间隔 ', ROUND(commit_interval_sec, 1), ' 秒 > 300 秒（5min 目标）')
    ELSE '正常推进'
  END AS health_detail
FROM RDW_DATA.checkpoint_health
WHERE snapshot_id_delta IS NOT NULL           -- 排除每个表首行（无前值可比）
  AND (snapshot_id_delta = 0 OR commit_interval_sec > 300);

-- 说明：
-- 1. checkpoint_health：全量快照推进明细，含增量与提交间隔（≈数据可见新鲜度），供趋势观察。
-- 2. checkpoint_stall_alert：仅输出异常时段（STALL/STALE），供告警与瓶颈关联。
-- 3. 阈值 300 秒 = 数据新鲜度目标 ≤5min。写作业 checkpoint=3min、正常提交间隔约 180s，
--    故取 >300s 才判超标（避免把正常的 3min 提交节奏误判为异常）。
-- 4. STALL（增量=0）通常意味着 checkpoint 失败、反压严重或 Paimon 提交阻塞；
--    应结合 05_health_flags 的 l0_flag（L0 堆积）/ backpressure_flag 共同定位"写不进" vs "合不动"。
-- 5. 多表场景按 job_name 分区，各表独立判定。

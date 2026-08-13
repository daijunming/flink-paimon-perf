-- 08_checkpoint_health.sql —— 快照推进健康度 + 数据可见新鲜度
-- 数据源已改接 meta-collect ODS 快照表（2026-08-11）：原实现基于 metadata-collector 的
-- paimon.snapshot.id / paimon.snapshot.time.millis，已随采集器退役无新数据。
-- 现基于 RDW_DATA.rdw_ods_paimon_meta_snapshots（每轮全量重采未过期快照，过期行保留为长历史），
-- 语义不变：快照推进节奏 ≈ 数据可见延迟代理。视图统一建在 RDW_DATA。
--
-- 为什么这就是新鲜度：Paimon 主键表的数据对下游可见 = snapshot 提交 = 写作业 checkpoint。
-- 所以"相邻快照提交间隔"≈ 数据从写入到可见的最大延迟。健康的分钟级引擎应满足：
--   1. snapshot_id 随时间单调递增（每次 checkpoint 产生新快照）；
--   2. 提交间隔稳定且 ≤ 新鲜度目标（本测试 ≤5min）。
-- 若 snapshot_id 长时间不变（增量=0）→ checkpoint 失败/写入阻塞，数据不再更新（新鲜度彻底破坏）。
--
-- 数据形态须知（误读风险）：snapshots 表每轮全量重采会刷新所有未过期行的 collected_at，
-- 因此只有最新一轮的分组是完整的；历史轮次分组只含"恰在该轮之后过期"的少数快照，
-- 其 latest_snapshot_id 比当时真实最新值滞后约一个 retention 窗口。
-- 推进/停滞的趋势判定不受影响，但不要把历史行的绝对值当作当时的最新快照号。

-- ==================== 视图1：快照推进明细 ====================
-- 按采集轮次（同轮 INSERT 共享同一 collected_at）取最新 snapshot_id 与最新提交时间，
-- 并用窗口函数算相邻轮次增量与提交间隔。
CREATE OR REPLACE VIEW RDW_DATA.checkpoint_health AS
SELECT
  table_name,                                          -- 被监测的表名（多表区分）
  collected_at AS collect_round_time,                  -- 采集轮次时间（约每 3 分钟一轮）
  latest_snapshot_id,
  latest_commit_time,
  -- 相邻轮次最新快照号增量：>0 正常推进，=0 停滞，NULL 为首行
  latest_snapshot_id - LAG(latest_snapshot_id) OVER (
      PARTITION BY table_name ORDER BY collected_at
  ) AS snapshot_id_delta,
  -- 相邻轮次最新提交时间间隔（秒）≈ 数据可见新鲜度：反映数据从写入到可见的最大延迟
  TIMESTAMPDIFF(SECOND,
      LAG(latest_commit_time) OVER (PARTITION BY table_name ORDER BY collected_at),
      latest_commit_time
  ) AS commit_interval_sec
FROM (
  SELECT
    table_name,
    collected_at,
    MAX(snapshot_id) AS latest_snapshot_id,
    MAX(commit_time) AS latest_commit_time
  FROM RDW_DATA.rdw_ods_paimon_meta_snapshots
  WHERE table_name = 'wide_table'
  GROUP BY table_name, collected_at
) t;

-- ==================== 视图2：新鲜度 / 停滞告警 ====================
-- 筛选快照停滞（增量=0）或数据可见新鲜度超标（提交间隔 > 5min）的轮次。
CREATE OR REPLACE VIEW RDW_DATA.checkpoint_stall_alert AS
SELECT
  table_name,
  collect_round_time,
  latest_snapshot_id,
  snapshot_id_delta,
  commit_interval_sec,
  CASE
    WHEN snapshot_id_delta = 0 THEN 'STALL'
    WHEN commit_interval_sec > 300 THEN 'STALE'      -- 提交间隔 >5min，数据可见新鲜度超标
    ELSE 'OK'
  END AS health_status,
  CASE
    WHEN snapshot_id_delta = 0
         THEN CONCAT('快照停滞: snapshot_id 持续为 ', latest_snapshot_id, '（checkpoint 失败/写入阻塞，数据不再更新）')
    WHEN commit_interval_sec > 300
         THEN CONCAT('新鲜度超标: 提交间隔 ', ROUND(commit_interval_sec, 1), ' 秒 > 300 秒（5min 目标）')
    ELSE '正常推进'
  END AS health_detail
FROM RDW_DATA.checkpoint_health
WHERE snapshot_id_delta IS NOT NULL           -- 排除每个表首行（无前值可比）
  AND (snapshot_id_delta = 0 OR commit_interval_sec > 300);

-- 说明：
-- 1. checkpoint_health：按采集轮次的快照推进明细，含增量与提交间隔（≈数据可见新鲜度），供趋势观察。
-- 2. checkpoint_stall_alert：仅输出异常轮次（STALL/STALE），供告警与瓶颈关联。
-- 3. 阈值 300 秒 = 数据新鲜度目标 ≤5min。写作业 checkpoint=3min、正常提交间隔约 180s，
--    故取 >300s 才判超标（避免把正常的 3min 提交节奏误判为异常）。
-- 4. STALL（增量=0）通常意味着 checkpoint 失败、反压严重或 Paimon 提交阻塞；
--    应结合 05_health_flags 的 l0_flag（L0 堆积）/ backpressure_flag 共同定位"写不进" vs "合不动"。
-- 5. 采集轮次约每 3 分钟一轮，与 checkpoint 周期接近：单轮无新提交（增量=0）会偶发，
--    连续多轮 STALL 才是真停滞；历史轮次分组的整体滞后见文件头"数据形态须知"。
-- 6. 多表场景按 table_name 分区，各表独立判定；接入新表时放宽 WHERE 的表名过滤即可。

-- 08_checkpoint_health.sql —— 快照推进健康度（Requirements 7.x 补充）
-- 利用 Paimon 元数据采集器已采集的 paimon.snapshot.id / paimon.snapshot.time.millis，
-- 验证 Flink checkpoint → Paimon commit 是否正常推进。视图统一建在 RDW_DATA。
--
-- 健康的流式入湖应满足：
--   1. snapshot_id 随时间单调递增（每次 checkpoint 产生新快照）；
--   2. 相邻快照的提交时间间隔稳定（接近 checkpoint interval）；
-- 若 snapshot_id 长时间不变（增量=0），说明 checkpoint 失败或写入阻塞——
-- 这是端到端延迟超标（SLA: ≤3分钟）的根因之一。
--
-- 数据来源：job_name='wide_table' 的 Paimon 元数据（snapshot.id / snapshot.time.millis）。
-- 说明：metrics_view 已把 metric_type→source、metric_value→DOUBLE、metric_ts→分钟桶。

-- ==================== 视图1：快照推进明细 ====================
-- 按时段取每个表的最新 snapshot_id 与提交时间，并用窗口函数算相邻增量。
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
  -- 相邻快照提交时间间隔（秒）：反映 checkpoint 实际周期
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

-- ==================== 视图2：快照停滞告警 ====================
-- 筛选快照号停滞（增量=0）或推进过慢（间隔>180秒）的时段。
CREATE OR REPLACE VIEW RDW_DATA.checkpoint_stall_alert AS
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
  END AS health_status,
  CASE
    WHEN snapshot_id_delta = 0
         THEN CONCAT('快照停滞: snapshot_id 持续为 ', snapshot_id, '（checkpoint 可能失败）')
    WHEN commit_interval_sec > 180
         THEN CONCAT('推进过慢: 提交间隔 ', ROUND(commit_interval_sec, 1), ' 秒 > 180 秒')
    ELSE '正常推进'
  END AS health_detail
FROM RDW_DATA.checkpoint_health
WHERE snapshot_id_delta IS NOT NULL           -- 排除每个表首行（无前值可比）
  AND (snapshot_id_delta = 0 OR commit_interval_sec > 180);

-- 说明：
-- 1. checkpoint_health：全量快照推进明细，含增量与提交间隔，供趋势观察。
-- 2. checkpoint_stall_alert：仅输出异常时段（STALL/SLOW），供告警与瓶颈关联。
-- 3. STALL（增量=0）通常意味着 Flink checkpoint 失败、反压严重或 Paimon 提交阻塞，
--    应结合 05_bottleneck_identify 的 COMPACTION_LAG（L0 堆积）共同定位。
-- 4. 阈值 180 秒对齐 SLA 端到端延迟上限（≤3 分钟），可按 checkpoint interval 调整。
-- 5. 多表场景按 job_name 分区，各表独立判定推进健康度。

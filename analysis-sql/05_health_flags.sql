-- 05_health_flags.sql —— 可读健康标志（取代旧的瓶颈决策树）
-- 只呈现"看得见的事实 + 简单阈值标志",不替用户做武断的根因归因。
-- 每分钟一行，汇总四个观测维度并给出可调阈值的软标志：
--   * 写入吞吐（写入作业）              write_rps
--   * Compaction 线程繁忙度（独立作业） compaction_thread_busy_max（0~100）
--   * L0 堆积 / 文件总数（Paimon 表）    level0_file_count / paimon_file_count
--   * 反压信号（写入作业）              max_checkpoint_start_delay_ms
--   * Compaction 活跃度（Paimon commit kind） compact_ratio
--
-- 设计取舍（第一性原理）："合不过来"的两个直接证据是
--   (a) L0 是否持续堆积（Paimon 表侧）；(b) Compaction 线程是否接近满负荷（compaction 作业侧）。
--   写入吞吐(write_rps)与 compactionThreadBusy 并排看即可判断"写得快 vs 合得动":
--   write_rps 高 + busy 接近 100 + L0 持续涨 = 合不过来。快照推进/停滞另见 08_checkpoint_health。

CREATE OR REPLACE VIEW RDW_DATA.health_flags AS
WITH buckets AS (
  SELECT DISTINCT time_bucket_minute
  FROM RDW_DATA.metrics_view
  WHERE job_name IN ('DataStreamperf_paimon', 'wide_table', 'compaction_job')
)
SELECT
  b.time_bucket_minute,
  i.throughput_rps                         AS write_rps,
  c.compaction_thread_busy_max,
  c.avg_compaction_time_ms,
  r.level0_file_count,
  r.paimon_file_count,
  w.max_checkpoint_start_delay_ms,
  CASE WHEN u.total_commits > 0 THEN u.compact_count / u.total_commits ELSE NULL END AS compact_ratio,
  -- 软标志（阈值可调；仅陈述事实，不做最终结论）
  CASE WHEN r.level0_file_count > 1000 THEN 'L0_PILEUP' ELSE 'OK' END AS l0_flag,
  CASE WHEN w.max_checkpoint_start_delay_ms > 30000 THEN 'BACKPRESSURE' ELSE 'OK' END AS backpressure_flag,
  CASE WHEN c.compaction_thread_busy_max > 90 THEN 'COMPACTION_SATURATED' ELSE 'OK' END AS compaction_flag
FROM buckets b
LEFT JOIN RDW_DATA.metrics_ingest_perf        i ON b.time_bucket_minute = i.time_bucket_minute
LEFT JOIN RDW_DATA.metrics_compaction_job     c ON b.time_bucket_minute = c.time_bucket_minute
LEFT JOIN RDW_DATA.metrics_resource_compaction r ON b.time_bucket_minute = r.time_bucket_minute
LEFT JOIN RDW_DATA.metrics_write_health       w ON b.time_bucket_minute = w.time_bucket_minute
LEFT JOIN RDW_DATA.metrics_update_delete_eff  u ON b.time_bucket_minute = u.time_bucket_minute;

-- 说明：
-- 1. 阈值（L0>1000、反压>30000ms、Compaction 繁忙>90）是可调起点，按实测基线调整；只驱动软标志，不改原始值列。
-- 2. "合不过来"判读：l0_flag=L0_PILEUP 与 compaction_flag=COMPACTION_SATURATED 同时出现最有说服力。
-- 3. write_rps 与 compaction_thread_busy_max 并排给出，供人对照"写得快 vs 合得动"（不做单位不一致的速率相减）。
-- 4. 查询建议加时间范围：SELECT * FROM RDW_DATA.health_flags
--    WHERE time_bucket_minute BETWEEN '<起>' AND '<止>' ORDER BY time_bucket_minute;

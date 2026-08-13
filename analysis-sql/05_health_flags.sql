-- 05_health_flags.sql —— 可读健康标志（取代旧的瓶颈决策树）
-- 只呈现"看得见的事实 + 简单阈值标志",不替用户做武断的根因归因。
-- 每分钟一行，汇总仍存活的观测维度并给出可调阈值的软标志：
--   * 写入吞吐（写入作业）                  write_rps
--   * L0 堆积 / 文件总数（Paimon 表侧）     level0_file_count / paimon_file_count
--   * 反压信号（写入作业）                  max_checkpoint_start_delay_ms
--   * Compaction 活跃度（快照 commit_kind） compact_ratio
--
-- 输入来源（2026-08-11 采集器退役后改接）：
--   * 写入侧列（write_rps / 反压）不变，仍来自 Flink 指标链路（job_name='DataStreamperf_paimon'）。
--   * 表侧列改由 meta-collect 的 RDW_DATA.v_paimon_meta_level_stats 承载（原 metadata-collector
--     的 paimon.file.count / paimon.level.* 指标已退役）。meta-collect 约每 3 分钟一轮，
--     表侧列只落在对齐轮次的分钟桶上，中间分钟为 NULL 属正常，不是缺失。
--   * compact_ratio 来自 metrics_update_delete_eff（已改接 rdw_ods_paimon_meta_snapshots）。
--   * 已移除 compaction_thread_busy_max / avg_compaction_time_ms / compaction_flag：
--     其输入 metrics_compaction_job 在 crontab 批任务形态下本就无数据，永久 NULL 有误导性；
--     compaction 作业侧只能看 cron 日志与 YARN 应用历史（见 docs/写入与合并性能分析.md）。
--
-- 设计取舍（第一性原理）："合不过来"现只剩表侧直接证据——
--   L0 是否持续堆积 + COMPACT 提交是否活跃：
--   write_rps 高 + L0 持续涨 + compact_ratio 不升 = 合不过来。快照推进/停滞另见 08_checkpoint_health。

CREATE OR REPLACE VIEW RDW_DATA.health_flags AS
WITH buckets AS (
  SELECT DISTINCT time_bucket_minute
  FROM RDW_DATA.metrics_view
  WHERE job_name = 'DataStreamperf_paimon'
),
table_side AS (
  -- 同分钟多轮采集（crontab 重跑/手工补采）时取较大者，保证 health_flags 每分钟一行
  SELECT
    time_bucket_minute,
    MAX(level0_file_count) AS level0_file_count,
    MAX(paimon_file_count) AS paimon_file_count
  FROM (
    -- 按采集轮次（source_snapshot_id）汇总：level=0 的 file_count 即 L0，各 level 求和即文件总数
    SELECT
      DATE_FORMAT(collected_at, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
      source_snapshot_id,
      SUM(CASE WHEN level = 0 THEN file_count ELSE 0 END) AS level0_file_count,
      SUM(file_count) AS paimon_file_count
    FROM RDW_DATA.v_paimon_meta_level_stats
    WHERE table_name = 'wide_table'
    GROUP BY DATE_FORMAT(collected_at, '%Y-%m-%d %H:%i:00'), source_snapshot_id
  ) s
  GROUP BY time_bucket_minute
)
SELECT
  b.time_bucket_minute,
  i.throughput_rps                         AS write_rps,
  t.level0_file_count,
  t.paimon_file_count,
  w.max_checkpoint_start_delay_ms,
  CASE WHEN u.total_commits > 0 THEN u.compact_count / u.total_commits ELSE NULL END AS compact_ratio,
  -- 软标志（阈值可调；仅陈述事实，不做最终结论）
  CASE WHEN t.level0_file_count > 1000 THEN 'L0_PILEUP' ELSE 'OK' END AS l0_flag,
  CASE WHEN w.max_checkpoint_start_delay_ms > 30000 THEN 'BACKPRESSURE' ELSE 'OK' END AS backpressure_flag
FROM buckets b
LEFT JOIN RDW_DATA.metrics_ingest_perf       i ON b.time_bucket_minute = i.time_bucket_minute
LEFT JOIN table_side                         t ON b.time_bucket_minute = t.time_bucket_minute
LEFT JOIN RDW_DATA.metrics_write_health      w ON b.time_bucket_minute = w.time_bucket_minute
LEFT JOIN RDW_DATA.metrics_update_delete_eff u ON b.time_bucket_minute = u.time_bucket_minute;

-- 说明：
-- 1. 阈值（L0>1000、反压>30000ms）是可调起点，按实测基线调整；只驱动软标志，不改原始值列。
-- 2. "合不过来"判读：l0_flag=L0_PILEUP 且 compact_ratio 持续偏低最有说服力（compaction 作业侧指标已无来源）。
-- 3. 表侧列只在 meta-collect 轮次分钟有值（约每 3 分钟一行），中间分钟 NULL 属正常。
-- 4. 查询建议加时间范围：SELECT * FROM RDW_DATA.health_flags
--    WHERE time_bucket_minute BETWEEN '<起>' AND '<止>' ORDER BY time_bucket_minute;

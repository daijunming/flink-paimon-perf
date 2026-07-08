-- 02_four_category_metrics.sql —— 指标聚合（Requirements 7.1 / 7.2 / 7.4）
-- 按真实作业拓扑对齐（2026-07-07 核对）：写入作业 write-only，Compaction 由独立 action 作业完成。
--
-- 关键：Flink 指标是任务级，metric_name = '<算子名>.<subtask下标>.<指标短名>'，按 subtask 分行；
--       要得到作业级总量，必须「每个 subtask 取桶内累计最大值，再对各 subtask 求和」（不是直接 MAX）。
--
-- 数据源与 job_name：
--   写入作业   job_name='DataStreamperf_paimon'（算子链 Source:...->ConstraintEnforcer[..]->Map / Writer / Global Committer）
--   Compaction job_name='compaction_job'（独立 paimon action 作业）
--   Paimon 表  job_name='wide_table'（元数据采集器）
--   集群资源   job_name='cluster'（YARN/HDFS 采集器）
-- 读取性能（原类别3）：当前无 Flink 读作业 / 关联查询，无数据源，本文件不再产出占位视图。

-- ==================== 类别1a：写入吞吐（Requirements 7.1）====================
-- 源链路算子（含 ConstraintEnforcer）的 numRecordsOut：每个 subtask（=不同 metric_name）取桶内累计最大值，
-- 再对各 subtask 求和 = 该分钟末作业级累计写出条数；rps 由 03 用相邻桶差分/时段秒数得到。
-- 注意：修正最初样例的 'ConstraintEnforcer%'（前缀）——真实算子名以 'Source:' 开头，须用 '%ConstraintEnforcer%'（包含）。
CREATE OR REPLACE VIEW RDW_DATA.metrics_ingest_perf AS
SELECT
  time_bucket_minute,
  records_out_total,
  -- 写入吞吐(rps)：相邻桶累计差 / 实际间隔秒（按 time_bucket 实算，不写死 60，兼容采样缺口）；首桶为 NULL
  (records_out_total - LAG(records_out_total) OVER (ORDER BY time_bucket_minute))
    / NULLIF(UNIX_TIMESTAMP(time_bucket_minute)
             - LAG(UNIX_TIMESTAMP(time_bucket_minute)) OVER (ORDER BY time_bucket_minute), 0)
    AS throughput_rps
FROM (
  SELECT
    time_bucket_minute,
    SUM(subtask_cum) AS records_out_total          -- 各 subtask 累计求和 = 作业级累计写出条数
  FROM (
    SELECT
      time_bucket_minute,
      metric_name,                                 -- 每个 subtask 一个 metric_name
      MAX(metric_value) AS subtask_cum             -- 累计计数器：桶内取最大≈桶末值
    FROM RDW_DATA.metrics_view
    WHERE job_name = 'DataStreamperf_paimon'
      AND metric_name LIKE '%ConstraintEnforcer%numRecordsOut'  -- '%numRecordsOut' 天然排除 numRecordsOutPerSecond
    GROUP BY time_bucket_minute, metric_name
  ) s
  GROUP BY time_bucket_minute
) t;

-- ==================== 类别1b：写入健康 / 反压（Requirements 7.1）====================
-- checkpointStartDelayNanos 高 = 反压（checkpoint barrier 迟迟到不了该 task）。
-- 各算子各 subtask 均有该指标；桶内取最大（最差 task），纳秒→毫秒。
CREATE OR REPLACE VIEW RDW_DATA.metrics_write_health AS
SELECT
  time_bucket_minute,
  MAX(metric_value) / 1000000.0 AS max_checkpoint_start_delay_ms
FROM RDW_DATA.metrics_view
WHERE job_name = 'DataStreamperf_paimon'
  AND metric_name LIKE '%checkpointStartDelayNanos'
GROUP BY time_bucket_minute;

-- ==================== 类别2：更新与删除效率（Requirements 7.2）====================
-- Paimon commit kind：COMPACT=1.0 / APPEND=0.0。COMPACT 占比反映 Compaction 活跃度。
CREATE OR REPLACE VIEW RDW_DATA.metrics_update_delete_eff AS
SELECT
  time_bucket_minute,
  AVG(metric_value) AS avg_commit_kind,
  SUM(CASE WHEN metric_value = 1.0 THEN 1 ELSE 0 END) AS compact_count,
  COUNT(*) AS total_commits
FROM RDW_DATA.metrics_view
WHERE job_name = 'wide_table'
  AND metric_name = 'paimon.last.commit.kind'
GROUP BY time_bucket_minute;

-- ==================== 类别4a：集群资源 + Paimon 文件/Level（Requirements 7.4）====================
-- YARN/HDFS 来自 job_name='cluster'（资源采集器）；Paimon 文件数/Level 来自 job_name='wide_table'。
-- 这些都是每类每时段单序列，直接 MAX 即可（非 subtask 分行）。
CREATE OR REPLACE VIEW RDW_DATA.metrics_resource_compaction AS
SELECT
  time_bucket_minute,
  -- YARN（cluster 级）
  MAX(CASE WHEN metric_name = 'yarn.allocated.vcores'    THEN metric_value END) AS yarn_allocated_vcores,
  MAX(CASE WHEN metric_name = 'yarn.available.vcores'    THEN metric_value END) AS yarn_available_vcores,
  MAX(CASE WHEN metric_name = 'yarn.allocated.memory.mb' THEN metric_value END) AS yarn_allocated_memory_mb,
  MAX(CASE WHEN metric_name = 'yarn.available.memory.mb' THEN metric_value END) AS yarn_available_memory_mb,
  -- HDFS（cluster 级）
  MAX(CASE WHEN metric_name = 'hdfs.capacity.used.bytes'      THEN metric_value END) AS hdfs_used_bytes,
  MAX(CASE WHEN metric_name = 'hdfs.capacity.total.bytes'     THEN metric_value END) AS hdfs_total_bytes,
  MAX(CASE WHEN metric_name = 'hdfs.capacity.remaining.bytes' THEN metric_value END) AS hdfs_remaining_bytes,
  -- Paimon 文件总数 + Level 分布（L0 堆积 = Compaction 跟不上写入）
  MAX(CASE WHEN metric_name = 'paimon.file.count' THEN metric_value END) AS paimon_file_count,
  MAX(CASE WHEN metric_name = 'paimon.level.file.count.L0' THEN metric_value END) AS level0_file_count,
  MAX(CASE WHEN metric_name = 'paimon.level.file.count.L1' THEN metric_value END) AS level1_file_count,
  MAX(CASE WHEN metric_name = 'paimon.level.file.count.L2' THEN metric_value END) AS level2_file_count,
  MAX(CASE WHEN metric_name = 'paimon.level.file.count.L3' THEN metric_value END) AS level3_file_count,
  MAX(CASE WHEN metric_name = 'paimon.level.file.count.L4' THEN metric_value END) AS level4_file_count,
  MAX(CASE WHEN metric_name = 'paimon.level.file.count.L5' THEN metric_value END) AS level5_file_count,
  MAX(CASE WHEN metric_name = 'paimon.level.size.bytes.L0' THEN metric_value END) AS level0_size_bytes,
  MAX(CASE WHEN metric_name = 'paimon.level.size.bytes.L1' THEN metric_value END) AS level1_size_bytes,
  MAX(CASE WHEN metric_name = 'paimon.level.size.bytes.L2' THEN metric_value END) AS level2_size_bytes,
  MAX(CASE WHEN metric_name = 'paimon.level.size.bytes.L3' THEN metric_value END) AS level3_size_bytes,
  MAX(CASE WHEN metric_name = 'paimon.level.size.bytes.L4' THEN metric_value END) AS level4_size_bytes,
  MAX(CASE WHEN metric_name = 'paimon.level.size.bytes.L5' THEN metric_value END) AS level5_size_bytes
FROM RDW_DATA.metrics_view
WHERE (job_name = 'cluster'
        AND metric_name IN ('yarn.allocated.vcores', 'yarn.available.vcores',
                            'yarn.allocated.memory.mb', 'yarn.available.memory.mb',
                            'hdfs.capacity.used.bytes', 'hdfs.capacity.total.bytes',
                            'hdfs.capacity.remaining.bytes'))
   OR (job_name = 'wide_table'
        AND metric_name IN ('paimon.file.count',
                            'paimon.level.file.count.L0', 'paimon.level.file.count.L1', 'paimon.level.file.count.L2',
                            'paimon.level.file.count.L3', 'paimon.level.file.count.L4', 'paimon.level.file.count.L5',
                            'paimon.level.size.bytes.L0', 'paimon.level.size.bytes.L1', 'paimon.level.size.bytes.L2',
                            'paimon.level.size.bytes.L3', 'paimon.level.size.bytes.L4', 'paimon.level.size.bytes.L5'))
GROUP BY time_bucket_minute;

-- ==================== 类别4b：Compaction 作业开销（Requirements 7.4）====================
-- 独立 compaction 作业（job_name='compaction_job'）本质是普通 Flink 任务：既有 Flink 标准指标，
-- 也有 Paimon 桥接指标（Compaction Metrics）。这里用 Paimon Compaction Metrics 直接度量"合"的开销：
--   * compactionThreadBusy（0~100）：Compaction 线程繁忙度，接近 100 = 合并近满负荷（合不过来的先兆）
--   * avgCompactionTime：单次 compaction 平均耗时（ms）
-- Paimon 桥接指标 metric_name 形如
--   '<算子>.<subtask>.paimon.table.<表>.partition.<..>.bucket.<..>.compaction.<短名>'，
-- 故用后缀匹配（LIKE '%<短名>'）跨 subtask/partition/bucket 命中；繁忙度取最大(最差)与平均。
CREATE OR REPLACE VIEW RDW_DATA.metrics_compaction_job AS
SELECT
  time_bucket_minute,
  MAX(CASE WHEN metric_name LIKE '%compactionThreadBusy' THEN metric_value END) AS compaction_thread_busy_max,
  AVG(CASE WHEN metric_name LIKE '%compactionThreadBusy' THEN metric_value END) AS compaction_thread_busy_avg,
  AVG(CASE WHEN metric_name LIKE '%avgCompactionTime'    THEN metric_value END) AS avg_compaction_time_ms
FROM RDW_DATA.metrics_view
WHERE job_name = 'compaction_job'
  AND (metric_name LIKE '%compactionThreadBusy' OR metric_name LIKE '%avgCompactionTime')
GROUP BY time_bucket_minute;

-- 说明：
-- 1. 写入吞吐/ compaction 吞吐都做了「subtask 求和」——这是任务级指标的正确聚合方式。
-- 2. 类别1 来自写入作业，类别4 的 Compaction 开销来自独立 compaction 作业 + 集群资源 + Paimon 文件/Level。
-- 3. 无读作业，故不产出"读取性能"视图。
-- 4. 查询这些视图时建议在 time_bucket_minute 上加时间范围过滤以利分区裁剪。

-- 02_four_category_metrics.sql —— 指标聚合（Requirements 7.1 / 7.2 / 7.4）
-- 按真实作业拓扑对齐（2026-07-07 核对）：写入作业 write-only，Compaction 由独立 action 作业完成。
--
-- 关键：Flink 指标是任务级，metric_name = '<算子名>.<subtask下标>.<指标短名>'，按 subtask 分行；
--       要得到作业级总量，必须「桶内取 MAX → 单算子内跨 subtask 求和 → 跨算子取 MAX 去重」：
--       同一指标可能被任务级（算子链全名）与算子级（单算子名）两种粒度重复上报、计数相同，
--       不分算子直接 SUM 会重复计数（2026-08-12 现场核实，见类别1a）。
--
-- 数据源与 job_name（2026-08-11 采集器退役后口径）：
--   写入作业   job_name='DataStreamperf_paimon'（算子链 Source:...->ConstraintEnforcer[..]->Map / Writer / Global Committer）
--   Compaction job_name='compaction_job'（旧流式常驻形态；现行 crontab 批任务不上报指标，视图仅历史有效）
--   Paimon 表  改由 meta-collect ODS 承载（rdw_ods_paimon_meta_snapshots 等）——
--              metadata-collector（job_name='wide_table'）2026-08 退役，paimon.* 指标无新数据
--   集群资源   resource-collector（job_name='cluster'）2026-08 退役，无替代通路，
--              原 metrics_resource_compaction 视图随之删除（见下文 4a 说明）
-- 读取性能（原类别3）：流式读作业（streaming_read_job）已接入，吞吐/反压/读写对照见
-- 09_streaming_read.sql；本文件不重复产出。点查/批 OLAP 仍无作业，不出对应视图。

-- ==================== 类别1a：写入吞吐（Requirements 7.1）====================
-- 源链路算子（含 ConstraintEnforcer）的 numRecordsOut 是累计计数器，聚合三层：
--   ① 桶内 MAX（≈桶末值）→ ② 单算子内跨 subtask SUM（=该算子看到的作业级累计）→ ③ 跨算子 MAX。
-- ③ 不可省（2026-08-12 现场核实）：该 LIKE 命中 6 个 metric_name = 2 算子 × 3 subtask——
--   任务级链名 'Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map' 与算子级名
--   'ConstraintEnforcer[4]' 是同一条链的两种上报粒度、计数相同；不分算子直接 SUM 会把同一批
--   记录算两遍（吞吐虚高约 2 倍）。同链两粒度计数近似相等，跨算子 MAX≈真实总量（若
--   ConstraintEnforcer 丢弃违约记录，MAX 取偏源头侧的值）。
-- 注意：LIKE 须用 '%ConstraintEnforcer%'（包含）——真实算子名以 'Source:' 开头（旧版前缀
-- 'ConstraintEnforcer%' 匹配不到任务级链名）。
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
    MAX(op_total) AS records_out_total             -- ③ 跨算子 MAX 去重（两粒度计数相同，不可 SUM）
  FROM (
    SELECT
      time_bucket_minute,
      operator_name,
      SUM(subtask_cum) AS op_total                 -- ② 单算子内：各 subtask 累计求和
    FROM (
      SELECT
        time_bucket_minute,
        -- 剥掉末尾 '<subtask>.numRecordsOut' 得到算子名（两种上报粒度在此分开）
        REGEXP_REPLACE(metric_name, '\\.[0-9]+\\.numRecordsOut$', '') AS operator_name,
        subtask_cum
      FROM (
        SELECT
          time_bucket_minute,
          metric_name,
          MAX(metric_value) AS subtask_cum         -- ① 累计计数器：桶内取最大≈桶末值
        FROM RDW_DATA.metrics_view
        WHERE job_name = 'DataStreamperf_paimon'
          AND metric_name LIKE '%ConstraintEnforcer%numRecordsOut'  -- '%numRecordsOut' 天然排除 numRecordsOutPerSecond
        GROUP BY time_bucket_minute, metric_name
      ) m
    ) s
    GROUP BY time_bucket_minute, operator_name
  ) o
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
-- 数据源已改接 meta-collect ODS 快照表（2026-08-11）：原信号 paimon.last.commit.kind
-- 随 metadata-collector 退役无新数据；ODS 快照表逐提交记录 commit_kind 与 commit_time，
-- 是更准的来源（原口径只采到采集周期内"最后一次 commit kind"，同周期多次提交被覆盖）。
-- 列名 total_commits / compact_count 保持不变（05_health_flags 直接引用）；
-- 原 avg_commit_kind（0/1 标志的均值）由 compact_ratio 取代，语义相同。
CREATE OR REPLACE VIEW RDW_DATA.metrics_update_delete_eff AS
SELECT
  DATE_FORMAT(commit_time, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
  COUNT(*) AS total_commits,
  SUM(CASE WHEN commit_kind = 'COMPACT' THEN 1 ELSE 0 END) AS compact_count,
  SUM(CASE WHEN commit_kind = 'COMPACT' THEN 1 ELSE 0 END) / COUNT(*) AS compact_ratio
FROM RDW_DATA.rdw_ods_paimon_meta_snapshots
WHERE table_name = 'wide_table'
GROUP BY DATE_FORMAT(commit_time, '%Y-%m-%d %H:%i:00');

-- ==================== 类别4a：集群资源 + Paimon 文件/Level —— 已删除 ====================
-- 原 metrics_resource_compaction 视图已删除（2026-08-11）：
--   * yarn.*/hdfs.* 信号随 resource-collector 退役，无替代通路，资源观测随之退役；
--   * Paimon 文件数/Level 分布由 meta-collect 的 RDW_DATA.v_paimon_meta_level_stats 覆盖
--    （逐 Snapshot 粒度，比原聚合指标更准），此处不再重复建视图。

-- ==================== 类别4b：Compaction 作业开销（Requirements 7.4）====================
-- 注意：现行合并为 crontab 批任务 paimon-compact（几十秒退出），不上报指标，本视图无数据属预期；
-- 以下口径仅流式常驻 compaction_job 形态有效。
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
-- 1. 写入吞吐做了「桶内 MAX → 单算子内跨 subtask 求和 → 跨算子 MAX 去重」——任务级指标的正确聚合方式（去重不可省，见类别1a）。
-- 2. 类别1 来自写入作业；类别2 来自 meta-collect ODS 快照表；类别4 仅剩 Compaction 开销视图（仅历史有效）。
-- 3. 读取性能由 09_streaming_read.sql 产出，本文件不重复建视图。
-- 4. 查询这些视图时建议在 time_bucket_minute 上加时间范围过滤以利分区裁剪。

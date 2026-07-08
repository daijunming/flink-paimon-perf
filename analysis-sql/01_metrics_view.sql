-- 01_metrics_view.sql —— 指标基础视图（时段分桶）
-- 对齐真实作业拓扑与 RDW_ODS_FLINK_METRICS 写入约定（2026-07-07 核对）。
--
-- 本测试相关指标来自四个 job_name（不是 app_id；app_id 与业务无关，不要用它过滤）：
--   1) 写入作业（write-only 入湖）  job_name = 'DataStreamperf_paimon'
--      Flink 原生任务级指标，metric_name 形如 '<算子名>.<subtask下标>.<指标短名>'，例如
--      'Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map.0.numRecordsOut'、
--      'Writer(write-only) : wide_table.0.checkpointStartDelayNanos'。按 subtask 分行。
--   2) Compaction 作业（独立 paimon action）job_name = 'compaction_job'
--      写入作业只写不合并，合并由该独立作业完成，其 Flink 指标反映 Compaction 开销。
--   3) Paimon 表元数据采集器          job_name = 'wide_table'（= 被监测表名）
--      metric_name 如 paimon.file.count / paimon.snapshot.id / paimon.snapshot.time.millis /
--      paimon.last.commit.kind / paimon.level.file.count.L0..L5 / paimon.level.size.bytes.L0..L5。
--   4) YARN/HDFS 资源采集器            job_name = 'cluster'（采集器打 tags.table='cluster'）
--      metric_name 如 yarn.allocated.vcores / hdfs.capacity.used.bytes 等，metric_type=YARN/HDFS。
--
-- 字段映射：metric_type→source，metric_value(varchar)→DOUBLE，metric_ts(varchar)→BIGINT。
-- 命名：视图统一建在 RDW_DATA；若 RDW_ODS_FLINK_METRICS 不在 RDW_DATA 库，改下方 FROM 的库名限定。
-- 提示：本视图不硬编码 etl_dt；查询时请在 etl_dt / time_bucket_minute 上加过滤以利分区裁剪。
-- 提示：四个 job_name 是当前这轮压测的作业名/表名，换表或换作业名时按需调整白名单。

CREATE OR REPLACE VIEW RDW_DATA.metrics_view AS
SELECT
  job_name,                                                   -- 区分来源作业/表
  app_id,
  metric_type AS source,                                      -- 来源标识（Paimon 元数据=PAIMON_METADATA，资源=YARN/HDFS）
  metric_name,                                                -- Flink 原生指标为 '<算子>.<subtask>.<短名>'
  CAST(metric_value AS DOUBLE) AS metric_value,               -- varchar→double
  CAST(metric_ts AS BIGINT) AS metric_ts_millis,              -- varchar→bigint（毫秒）
  FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
  job_id,
  host_name,
  etl_dt
FROM RDW_DATA.RDW_ODS_FLINK_METRICS
WHERE job_name IN ('DataStreamperf_paimon', 'compaction_job', 'wide_table', 'cluster');

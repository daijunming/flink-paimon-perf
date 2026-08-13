-- 01_metrics_view.sql —— 指标基础视图（时段分桶）
-- 对齐真实作业拓扑与 RDW_ODS_FLINK_METRICS 写入约定（2026-07-07 核对）。
--
-- 本测试相关指标来自五个 job_name（不是 app_id；app_id 与业务无关，不要用它过滤）：
--   1) 写入作业（write-only 入湖）  job_name = 'DataStreamperf_paimon'
--      Flink 原生任务级指标，metric_name 形如 '<算子名>.<subtask下标>.<指标短名>'，例如
--      'Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map.0.numRecordsOut'、
--      'Writer(write-only) : wide_table.0.checkpointStartDelayNanos'。按 subtask 分行。
--   2) Compaction 作业（独立 paimon action）job_name = 'compaction_job'
--      写入作业只写不合并，合并由该独立作业完成，其 Flink 指标反映 Compaction 开销。
--   3) Paimon 表元数据采集器          job_name = 'wide_table'（= 被监测表名）
--      metric_name 如 paimon.file.count / paimon.snapshot.id / paimon.snapshot.time.millis /
--      paimon.last.commit.kind / paimon.level.file.count.L0..L5 / paimon.level.size.bytes.L0..L5。
--      ※ 2026-08 现场退役（metadata-collector 停止部署运行），不再有新数据；白名单保留
--        仅为可查历史。表侧元数据信号改由 meta-collect ODS（rdw_ods_paimon_meta_*）承载，
--        消费方见 05_health_flags.sql / 08_checkpoint_health.sql 与 scripts/meta-collect/sr/。
--   4) YARN/HDFS 资源采集器            job_name = 'cluster'（采集器打 tags.table='cluster'）
--      metric_name 如 yarn.allocated.vcores / hdfs.capacity.used.bytes 等，metric_type=YARN/HDFS。
--      ※ 2026-08 现场退役（resource-collector 停止部署运行），无替代通路，资源信号随之退役；
--        白名单保留仅为可查历史。
--   5) 流式读作业（读 Paimon changelog）job_name = 'streaming_read_job'
--      scripts/sql/07_streaming_read.sql 提交（blackhole sink，只测流读）；与写入作业同管道上报，
--      同为任务级 '<算子>.<subtask>.<短名>' 按 subtask 分行，分析见 09_streaming_read.sql。
--
-- 字段映射：metric_type→source，metric_value(varchar)→DOUBLE，metric_ts(varchar)→BIGINT。
-- 命名：视图统一建在 RDW_DATA；若 RDW_ODS_FLINK_METRICS 不在 RDW_DATA 库，改下方 FROM 的库名限定。
-- 提示：本视图不硬编码 etl_dt；查询时请在 etl_dt / time_bucket_minute 上加过滤以利分区裁剪。
-- 提示：五个 job_name 是当前这轮压测的作业名/表名，换表或换作业名时按需调整白名单。

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
WHERE job_name IN ('DataStreamperf_paimon', 'compaction_job', 'wide_table', 'cluster',
                   'streaming_read_job');  -- 流式读作业（07_streaming_read.sql，09 的口径基于它）

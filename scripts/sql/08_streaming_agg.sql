-- 08_streaming_agg.sql —— 流式全表聚合作业（对 Paimon changelog 做持续聚合）
-- 目的：以流模式对 wide_table 做全表 running 聚合（COUNT/AVG/SUM/MAX）,
--   观测持续聚合的状态开销与更新延迟,以及与写入作业并发时的相互影响。
-- 说明：
--   * 主键表 + changelog-producer=input → 流读产出变更流；无 GROUP BY 的全局聚合维护单行
--     running 结果,随上游 +I/-U/+U/-D 持续更新,并向下游发出更新（retract）流。
--   * scan.mode 默认读全量 + 续读,故 COUNT 等反映全表当前值 + 持续变化；
--       只统计新增 → 给 source 加 /*+ OPTIONS('scan.mode'='latest') */。
--   * sink 用 print 观测更新（print 支持 retract/upsert 流）。
--   * 建议以独立流作业提交（如 job_name=streaming_agg_job）。

SET 'execution.runtime-mode' = 'streaming';

CREATE TEMPORARY TABLE default_catalog.default_database.streaming_agg_sink (
  total_records BIGINT,
  avg_c21_decimal DECIMAL(38,4),
  sum_c21_decimal DECIMAL(38,4),
  max_event_time BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO default_catalog.default_database.streaming_agg_sink
SELECT
  COUNT(*) AS total_records,
  AVG(c21_decimal) AS avg_c21_decimal,
  SUM(c21_decimal) AS sum_c21_decimal,
  MAX(event_time) AS max_event_time
FROM paimon_obs.paimon_database.wide_table;

-- 说明：
-- 1. 全局聚合（无 GROUP BY）在流模式下维护单行 running 结果,随 changelog 持续更新（含回撤）。
-- 2. 状态开销/更新延迟通过 Flink metrics（busyTimeMsPerSecond、checkpoint 时长、state size）观测。
-- 3. 与 05 写入作业并发运行时,观测两者吞吐/延迟的相互影响。

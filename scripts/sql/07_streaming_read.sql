-- 07_streaming_read.sql —— 流式查询作业（流式读取 Paimon 主键表 changelog）
-- 目的：以流模式持续读取 wide_table,测量流式读取吞吐/延迟,以及与写入作业并发时的相互影响。
-- 说明：
--   * wide_table 为主键表 + changelog-producer=input,流读产出输入变更流（+I/+U/-U/-D）。
--   * scan.mode 默认：读取当前快照全量 + 后续变更（"全表 + 增量"）；
--       只看增量 → 加 /*+ OPTIONS('scan.mode'='latest') */；显式全量+续读 → 'latest-full'。
--   * sink 用 blackhole：丢弃结果,只测流读本身、不引入 sink 开销；读性能看 source 算子指标。
--   * 建议以独立流作业提交（如 job_name=streaming_read_job），叠加在写入作业之上观测读写冲突。

SET 'execution.runtime-mode' = 'streaming';

CREATE TEMPORARY TABLE default_catalog.default_database.streaming_read_sink (
  pk BIGINT,
  c1_bigint BIGINT,
  c21_decimal DECIMAL(20,4),
  c41_string STRING,
  event_time BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO default_catalog.default_database.streaming_read_sink
SELECT pk, c1_bigint, c21_decimal, c41_string, event_time
FROM paimon_obs.paimon_database.wide_table;

-- 说明：
-- 1. 读性能看本作业 Paimon source 算子的 numRecordsIn/numRecordsOut(每秒) 与
--    currentFetchEventTimeLag(流读滞后) 等指标。
-- 2. blackhole 仅测流读；如需看内容,临时把 'connector' 改成 'print'。
-- 3. 与 05 写入作业并发运行时,观测写入吞吐与本作业流读延迟是否相互影响。

-- 02_sink_paimon.sql —— 创建 Paimon 100 列主键宽表（对齐真实环境 DDL）
-- 真实表：paimon_obs.paimon_database.wide_table
-- 列：pk + c1..c20 BIGINT + c21..c40 DECIMAL(20,4) + c41..c89 STRING + c90..c99 BIGINT(epoch毫秒) + event_time BIGINT
-- 合计 1+20+20+49+10 = 100 列业务字段 + event_time，主键 pk。
--
-- 关键（对齐真实环境）：
--   * bucket = 3（固定）：与写入作业 parallelism=3、Kafka 3 分区对齐。不再是 ${BUCKET_NUM} 变量，
--     也不是旧脚本臆想的 63/15——Flink SQL 的 SET 变量注入本就不生效，且真实就是 3。
--   * merge-engine=deduplicate + sequence.field=event_time：高频 update 时同一 pk 按 event_time 毫秒"新值胜出"。
--   * changelog-producer=input，snapshot.num-retained.min=10。
--   * 表上【不】放写入/compaction 调优选项。真实拓扑是"写入作业 write-only + 独立 compaction 作业"：
--       - 写入侧参数（write-only=true / sink.parallelism / write-buffer-* 等）由 05_ingest_insert.sql 的
--         INSERT `/*+ OPTIONS(...) */` 动态 hint 传入；
--       - compaction 调优（compaction-trigger / merge-max-file-num 等）由独立 compaction 作业的
--         --table_conf 传入（见 06_compaction_job.sh）。
-- 提交方式：preflight 阶段一次性执行（与 01_catalog.sql 一起）。

CREATE TABLE IF NOT EXISTS paimon_obs.paimon_database.wide_table (
  pk BIGINT,
  c1_bigint BIGINT, c2_bigint BIGINT, c3_bigint BIGINT, c4_bigint BIGINT, c5_bigint BIGINT,
  c6_bigint BIGINT, c7_bigint BIGINT, c8_bigint BIGINT, c9_bigint BIGINT, c10_bigint BIGINT,
  c11_bigint BIGINT, c12_bigint BIGINT, c13_bigint BIGINT, c14_bigint BIGINT, c15_bigint BIGINT,
  c16_bigint BIGINT, c17_bigint BIGINT, c18_bigint BIGINT, c19_bigint BIGINT, c20_bigint BIGINT,
  c21_decimal DECIMAL(20,4), c22_decimal DECIMAL(20,4), c23_decimal DECIMAL(20,4), c24_decimal DECIMAL(20,4),
  c25_decimal DECIMAL(20,4), c26_decimal DECIMAL(20,4), c27_decimal DECIMAL(20,4), c28_decimal DECIMAL(20,4),
  c29_decimal DECIMAL(20,4), c30_decimal DECIMAL(20,4), c31_decimal DECIMAL(20,4), c32_decimal DECIMAL(20,4),
  c33_decimal DECIMAL(20,4), c34_decimal DECIMAL(20,4), c35_decimal DECIMAL(20,4), c36_decimal DECIMAL(20,4),
  c37_decimal DECIMAL(20,4), c38_decimal DECIMAL(20,4), c39_decimal DECIMAL(20,4), c40_decimal DECIMAL(20,4),
  c41_string STRING, c42_string STRING, c43_string STRING, c44_string STRING, c45_string STRING,
  c46_string STRING, c47_string STRING, c48_string STRING, c49_string STRING, c50_string STRING,
  c51_string STRING, c52_string STRING, c53_string STRING, c54_string STRING, c55_string STRING,
  c56_string STRING, c57_string STRING, c58_string STRING, c59_string STRING, c60_string STRING,
  c61_string STRING, c62_string STRING, c63_string STRING, c64_string STRING, c65_string STRING,
  c66_string STRING, c67_string STRING, c68_string STRING, c69_string STRING, c70_string STRING,
  c71_string STRING, c72_string STRING, c73_string STRING, c74_string STRING, c75_string STRING,
  c76_string STRING, c77_string STRING, c78_string STRING, c79_string STRING, c80_string STRING,
  c81_string STRING, c82_string STRING, c83_string STRING, c84_string STRING, c85_string STRING,
  c86_string STRING, c87_string STRING, c88_string STRING, c89_string STRING,
  c90_ts BIGINT, c91_ts BIGINT, c92_ts BIGINT, c93_ts BIGINT, c94_ts BIGINT,
  c95_ts BIGINT, c96_ts BIGINT, c97_ts BIGINT, c98_ts BIGINT, c99_ts BIGINT,
  event_time BIGINT,
  PRIMARY KEY (pk) NOT ENFORCED
) WITH (
  'bucket' = '3',
  'merge-engine' = 'deduplicate',
  'sequence.field' = 'event_time',
  'changelog-producer' = 'input',
  'snapshot.num-retained.min' = '10'
);

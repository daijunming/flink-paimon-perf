-- 05_ingest_insert.sql —— 入湖 INSERT 主脚本（write-only，对齐真实作业 DataStreamperf_paimon）
-- 从 kafka_source 直通写入 wide_table，event_time 原样透传。
-- 相同 pk 的 update 由主键表 merge-engine=deduplicate + sequence.field=event_time 去重。
--
-- 关键（对齐真实环境）：
--   * 写入参数走 INSERT 的 `/*+ OPTIONS(...) */` 动态 hint（不放在建表 WITH）：
--       - write-only=true：写入作业只写不合并——真实算子名就是 `Writer(write-only) : wide_table`，
--         合并由独立 compaction 作业完成（见 06_compaction_job.sh）。
--       - sink.parallelism=3、write-buffer-spillable、write-buffer-size、sink.use-managed-memory-allocator、
--         parquet.enable.dictionary=false、read.batch-size：写入侧内存/格式调优。
--       - num-sorted-run.compaction-trigger / write.merge-max-file-num 在 write-only 下对本作业不生效
--         （合并在 compaction 作业），此处保留仅为与真实作业参数一致。
--   * 运行参数（parallelism.default=3、checkpoint、mini-batch、not-null-enforcer=ERROR 等）不在本 SQL 里 SET，
--     而在平台作业的运行参数 JSON（见 job-run-params.json）——这才是真实提交形态。

INSERT INTO paimon_obs.paimon_database.wide_table
/*+ OPTIONS(
  'write-only' = 'true',
  'sink.parallelism' = '3',
  'sink.use-managed-memory-allocator' = 'true',
  'write-buffer-spillable' = 'true',
  'write-buffer-size' = '64 m',
  'num-sorted-run.compaction-trigger' = '3',
  'write.merge-max-file-num' = '6',
  'parquet.enable.dictionary' = 'false',
  'read.batch-size' = '512'
) */
SELECT
  pk,
  c1_bigint, c2_bigint, c3_bigint, c4_bigint, c5_bigint,
  c6_bigint, c7_bigint, c8_bigint, c9_bigint, c10_bigint,
  c11_bigint, c12_bigint, c13_bigint, c14_bigint, c15_bigint,
  c16_bigint, c17_bigint, c18_bigint, c19_bigint, c20_bigint,
  c21_decimal, c22_decimal, c23_decimal, c24_decimal, c25_decimal,
  c26_decimal, c27_decimal, c28_decimal, c29_decimal, c30_decimal,
  c31_decimal, c32_decimal, c33_decimal, c34_decimal, c35_decimal,
  c36_decimal, c37_decimal, c38_decimal, c39_decimal, c40_decimal,
  c41_string, c42_string, c43_string, c44_string, c45_string,
  c46_string, c47_string, c48_string, c49_string, c50_string,
  c51_string, c52_string, c53_string, c54_string, c55_string,
  c56_string, c57_string, c58_string, c59_string, c60_string,
  c61_string, c62_string, c63_string, c64_string, c65_string,
  c66_string, c67_string, c68_string, c69_string, c70_string,
  c71_string, c72_string, c73_string, c74_string, c75_string,
  c76_string, c77_string, c78_string, c79_string, c80_string,
  c81_string, c82_string, c83_string, c84_string, c85_string,
  c86_string, c87_string, c88_string, c89_string,
  c90_ts, c91_ts, c92_ts, c93_ts, c94_ts,
  c95_ts, c96_ts, c97_ts, c98_ts, c99_ts,
  event_time
FROM default_catalog.default_database.kafka_source;

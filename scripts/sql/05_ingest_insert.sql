-- 05_ingest_insert.sql —— 入湖 INSERT 主脚本
-- 基于真实环境：paimon_obs.paimon_database.wide_table
-- 从 Kafka source 直通写入 Paimon 主键宽表，event_time 原样透传（不重新赋值），
-- 供端到端延迟探针计算 now - MAX(event_time)。
-- 相同 pk 的 update 由主键表 merge-engine=deduplicate + sequence.field=event_time 语义去重。
--
-- 提交前置：本脚本依赖 kafka_source（临时表，会话级）与 paimon_cat.perf.wide_table。
-- 推荐提交方式（同一 sql-client 会话内按序执行）：
--   flink sql-client \
--     -i scripts/sql/init_${PHASE}.sql \      # 阶段参数 SET + 变量
--     -i scripts/sql/03_source_kafka.sql \    # 建 Kafka source 临时表
--     -f scripts/sql/05_ingest_insert.sql     # 执行入湖 INSERT
-- （01_catalog.sql / 02_sink_paimon.sql 已在 preflight 阶段一次性建好）
--
-- 降低主键表写入开销：关闭 upsert 物化、丢弃 not-null 违例（测试数据无 null 主键）。
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

INSERT INTO paimon_obs.paimon_database.wide_table
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

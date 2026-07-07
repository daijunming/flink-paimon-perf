-- 03_source_kafka.sql —— 创建 Kafka source 临时表（CDC 格式）
-- 列名/类型与 02_sink_paimon.sql 及生成器 WideRecord.toJson() 三处严格对齐。
-- format=ogg-json：生成器产出 OGG CDC 格式 JSON（op_type=I/U/D），支持 INSERT/UPDATE/DELETE 三种操作。
-- 不设 WATERMARK：入湖为直通 INSERT，不做窗口/事件时间运算；端到端延迟由探针查 MAX(event_time) 计算。
-- topic / bootstrap.servers / scan.startup.mode 由运行环境与阶段脚本注入，仓库内不填真值。
-- 三级命名：平台存在多个catalog/database，所有表必须显式指定避免冲突。

CREATE TEMPORARY TABLE default_catalog.default_database.kafka_source (
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
  event_time BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '${KAFKA_TOPIC}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'job_paimon_wide_ingest',
  'scan.startup.mode' = '${SCAN_STARTUP_MODE}',
  'format' = 'ogg-json'
);

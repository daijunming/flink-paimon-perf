-- 06_point_lookup.sql —— Flink 点查作业（模拟实时特征查询，Requirements 7.3）
-- 从 Kafka 读取点查请求（pk列表），Lookup Join Paimon 表返回结果，写入 print sink 观测。
-- 用于验证：Paimon 主键表的点查延迟、并发读写冲突下的查询性能。

-- 前置：需另建一个 Kafka topic（如 lookup_requests）持续写入点查请求。
-- 可用简单脚本周期随机生成 pk 写入，或复用生成器产出的 pk 子集。

-- 降低 Lookup Join 开销：启用异步模式 + 缓存
SET 'table.exec.async-lookup.buffer-capacity' = '1000';
SET 'table.exec.async-lookup.timeout' = '3min';

-- 点查请求 source（临时表，三级命名）
CREATE TEMPORARY TABLE default_catalog.default_database.lookup_requests (
  request_id STRING,
  pk BIGINT,
  request_time BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '${LOOKUP_REQUESTS_TOPIC}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'job_paimon_point_lookup',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

-- 点查结果 sink（print，实际可改为写回 Kafka 或其他 sink，三级命名）
CREATE TEMPORARY TABLE default_catalog.default_database.lookup_results_print (
  request_id STRING,
  pk BIGINT,
  c1_bigint BIGINT,
  c21_decimal DECIMAL(20,4),
  c41_string STRING,
  event_time BIGINT,
  lookup_latency_ms BIGINT  -- 查询耗时（request_time 到当前时刻）
) WITH (
  'connector' = 'print'
);

-- Lookup Join：点查请求 LEFT JOIN Paimon 表（查不到返回 NULL）
INSERT INTO default_catalog.default_database.lookup_results_print
SELECT
  r.request_id,
  r.pk,
  w.c1_bigint,
  w.c21_decimal,
  w.c41_string,
  w.event_time,
  (UNIX_TIMESTAMP() * 1000 - r.request_time) AS lookup_latency_ms
FROM default_catalog.default_database.lookup_requests r
LEFT JOIN paimon_obs.paimon_database.wide_table FOR SYSTEM_TIME AS OF r.request_time AS w
  ON r.pk = w.pk;

-- 说明：
-- 1. FOR SYSTEM_TIME AS OF：Temporal Join 语法，Paimon 作为维表被 Lookup。
-- 2. lookup_latency_ms：粗略估算点查延迟（request_time 到当前时刻），实际需 Flink metrics 精确测量。
-- 3. 若无 lookup_requests topic，本作业无数据流入，处于空跑状态（不影响入湖作业）。
-- 4. 阶段2 若要验证并发读写冲突，需与 05_ingest_insert.sql 同时运行并观测查询延迟变化。

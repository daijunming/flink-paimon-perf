-- 20_collect_sampling.sql.tpl —— 按时间采样:每轮都提交,无论是否有新 Snapshot
--
-- 由 bin/collect_once.sh 渲染 ${...} 占位符后,经 sql-client -f 以 batch 模式提交。
-- 两条 INSERT 合并在一个 STATEMENT SET 里 = 一个 YARN 作业:
--   * $consumers:消费进度采样。只有 consumer_id / next_snapshot_id 两列;
--     消费滞后(lag)在分析侧用 snapshots 表的最大 snapshot_id 关联计算。
--     表无任何 consumer 时输出 0 行,属正常情况。
--   * $options:表显式配置(DDL WITH 项)采样。行数极少,每轮采样的代价可忽略,
--     换来"配置何时变化"的完整溯源;不代表默认配置全集。
--
-- 占位符:${PAIMON_WAREHOUSE} ${PAIMON_CATALOG} ${PAIMON_DATABASE} ${PAIMON_TABLE}
--          ${KAFKA_BOOTSTRAP_SERVERS} ${KAFKA_WITH_EXTRA}
--          ${TOPIC_CONSUMERS} ${TOPIC_OPTIONS} ${COLLECTOR_RUN_ID}

SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'pipeline.name' = 'paimon_meta_collect_sampling';
SET 'parallelism.default' = '1';

CREATE CATALOG IF NOT EXISTS paimon_obs WITH (
  'type' = 'paimon',
  'warehouse' = '${PAIMON_WAREHOUSE}'
);

CREATE TEMPORARY TABLE kafka_meta_consumers (
  catalog_name            STRING,
  database_name           STRING,
  table_name              STRING,
  collected_at            STRING,
  consumer_id             STRING,
  collector_run_id        STRING,
  next_snapshot_id        BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '${TOPIC_CONSUMERS}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}'${KAFKA_WITH_EXTRA},
  'format' = 'json'
);

CREATE TEMPORARY TABLE kafka_meta_options (
  catalog_name            STRING,
  database_name           STRING,
  table_name              STRING,
  collected_at            STRING,
  option_key              STRING,
  option_value            STRING,
  collector_run_id        STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${TOPIC_OPTIONS}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}'${KAFKA_WITH_EXTRA},
  'format' = 'json'
);

EXECUTE STATEMENT SET BEGIN

INSERT INTO kafka_meta_consumers
SELECT
  '${PAIMON_CATALOG}'  AS catalog_name,
  '${PAIMON_DATABASE}' AS database_name,
  '${PAIMON_TABLE}'    AS table_name,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS collected_at,
  consumer_id,
  '${COLLECTOR_RUN_ID}' AS collector_run_id,
  next_snapshot_id
FROM paimon_obs.paimon_database.`${PAIMON_TABLE}$consumers`;

-- 源列 key/value 为保留字,需反引号引用;写入时改名 option_key/option_value。
INSERT INTO kafka_meta_options
SELECT
  '${PAIMON_CATALOG}'  AS catalog_name,
  '${PAIMON_DATABASE}' AS database_name,
  '${PAIMON_TABLE}'    AS table_name,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS collected_at,
  `key`   AS option_key,
  `value` AS option_value,
  '${COLLECTOR_RUN_ID}' AS collector_run_id
FROM paimon_obs.paimon_database.`${PAIMON_TABLE}$options`;

END;

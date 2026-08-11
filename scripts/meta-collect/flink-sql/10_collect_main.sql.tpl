-- 10_collect_main.sql.tpl —— 主采集(简化版:无游标、无 hint,每轮都提交)
--
-- 由 bin/collect_once.sh 渲染 ${...} 占位符后,经 sql-client -f 以 batch 模式提交。
-- 六条 INSERT 合并在一个 STATEMENT SET 里 = 一个 YARN 作业。
--
-- 简化版口径(趋势观测优先):
--   * $snapshots / $statistics:每轮全量重采未过期部分(snapshot.time-retained 限定行数,
--     通常几十行);SR 主键覆盖,重复采集天然幂等。SR 侧历史不受 Paimon 过期影响,
--     过期快照的行保留,反而形成超出 retention 的长历史。
--   * $files / $manifests:不用 OPTIONS hint,读当前最新状态;
--     source_snapshot_id 由作业内 (SELECT MAX(snapshot_id) FROM $snapshots) 打标。
--     两个源在同一作业内几乎同时规划,误标窗口为毫秒级;即便误标,
--     下一轮按正确 snapshot_id 主键覆盖即自愈。
--   * partitions / buckets 汇总:由同一份 $files 当前态聚合生成。
--
-- 占位符:${PAIMON_WAREHOUSE} ${PAIMON_CATALOG} ${PAIMON_DATABASE} ${PAIMON_TABLE}
--          ${KAFKA_BOOTSTRAP_SERVERS} ${KAFKA_WITH_EXTRA}
--          ${TOPIC_SNAPSHOTS} ${TOPIC_STATISTICS} ${TOPIC_FILES} ${TOPIC_MANIFESTS}
--          ${TOPIC_PARTITIONS} ${TOPIC_BUCKETS} ${COLLECTOR_RUN_ID}

SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'pipeline.name' = 'paimon_meta_collect_main';
SET 'parallelism.default' = '1';

CREATE CATALOG IF NOT EXISTS paimon_obs WITH (
  'type' = 'paimon',
  'warehouse' = '${PAIMON_WAREHOUSE}'
);

CREATE TEMPORARY TABLE kafka_meta_snapshots (
  catalog_name            STRING,
  database_name           STRING,
  table_name              STRING,
  snapshot_id             BIGINT,
  collector_run_id        STRING,
  collected_at            STRING,
  schema_id               BIGINT,
  commit_user             STRING,
  commit_identifier       BIGINT,
  commit_kind             STRING,
  commit_time             STRING,
  base_manifest_list      STRING,
  delta_manifest_list     STRING,
  changelog_manifest_list STRING,
  total_record_count      BIGINT,
  delta_record_count      BIGINT,
  changelog_record_count  BIGINT,
  watermark               BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '${TOPIC_SNAPSHOTS}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}'${KAFKA_WITH_EXTRA},
  'format' = 'json'
);

CREATE TEMPORARY TABLE kafka_meta_statistics (
  catalog_name            STRING,
  database_name           STRING,
  table_name              STRING,
  snapshot_id             BIGINT,
  collector_run_id        STRING,
  collected_at            STRING,
  schema_id               BIGINT,
  merged_record_count     BIGINT,
  merged_record_size      BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '${TOPIC_STATISTICS}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}'${KAFKA_WITH_EXTRA},
  'format' = 'json'
);

CREATE TEMPORARY TABLE kafka_meta_files (
  catalog_name            STRING,
  database_name           STRING,
  table_name              STRING,
  source_snapshot_id      BIGINT,
  file_path               STRING,
  file_path_md5           STRING,
  collector_run_id        STRING,
  collected_at            STRING,
  partition_value         STRING,
  bucket                  INT,
  file_format             STRING,
  schema_id               BIGINT,
  level                   INT,
  record_count            BIGINT,
  file_size_in_bytes      BIGINT,
  min_sequence_number     BIGINT,
  max_sequence_number     BIGINT,
  creation_time           STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${TOPIC_FILES}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}'${KAFKA_WITH_EXTRA},
  'format' = 'json'
);

CREATE TEMPORARY TABLE kafka_meta_manifests (
  catalog_name            STRING,
  database_name           STRING,
  table_name              STRING,
  source_snapshot_id      BIGINT,
  file_name               STRING,
  collector_run_id        STRING,
  collected_at            STRING,
  file_size               BIGINT,
  num_added_files         INT,
  num_deleted_files       INT,
  schema_id               BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '${TOPIC_MANIFESTS}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}'${KAFKA_WITH_EXTRA},
  'format' = 'json'
);

CREATE TEMPORARY TABLE kafka_meta_partitions (
  catalog_name            STRING,
  database_name           STRING,
  table_name              STRING,
  source_snapshot_id      BIGINT,
  partition_value         STRING,
  collector_run_id        STRING,
  collected_at            STRING,
  record_count            BIGINT,
  file_count              BIGINT,
  file_size_in_bytes      BIGINT,
  last_update_time        STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${TOPIC_PARTITIONS}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}'${KAFKA_WITH_EXTRA},
  'format' = 'json'
);

CREATE TEMPORARY TABLE kafka_meta_buckets (
  catalog_name            STRING,
  database_name           STRING,
  table_name              STRING,
  source_snapshot_id      BIGINT,
  partition_value         STRING,
  bucket                  INT,
  collector_run_id        STRING,
  collected_at            STRING,
  record_count            BIGINT,
  file_count              BIGINT,
  file_size_in_bytes      BIGINT,
  last_update_time        STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${TOPIC_BUCKETS}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}'${KAFKA_WITH_EXTRA},
  'format' = 'json'
);

EXECUTE STATEMENT SET BEGIN

-- 全量重采未过期快照;表从未提交过时 0 行,属正常。
INSERT INTO kafka_meta_snapshots
SELECT
  '${PAIMON_CATALOG}'  AS catalog_name,
  '${PAIMON_DATABASE}' AS database_name,
  '${PAIMON_TABLE}'    AS table_name,
  snapshot_id,
  '${COLLECTOR_RUN_ID}' AS collector_run_id,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS collected_at,
  schema_id,
  commit_user,
  commit_identifier,
  commit_kind,
  DATE_FORMAT(commit_time, 'yyyy-MM-dd HH:mm:ss') AS commit_time,
  base_manifest_list,
  delta_manifest_list,
  changelog_manifest_list,
  total_record_count,
  delta_record_count,
  changelog_record_count,
  watermark
FROM paimon_obs.paimon_database.`${PAIMON_TABLE}$snapshots`;

-- $statistics 由 Paimon 提交时产出;未产出统计的表查询为 0 行,属正常。
-- colstat 为复合类型,第一阶段不采集。
INSERT INTO kafka_meta_statistics
SELECT
  '${PAIMON_CATALOG}'  AS catalog_name,
  '${PAIMON_DATABASE}' AS database_name,
  '${PAIMON_TABLE}'    AS table_name,
  snapshot_id,
  '${COLLECTOR_RUN_ID}' AS collector_run_id,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS collected_at,
  schema_id,
  `mergedRecordCount` AS merged_record_count,
  `mergedRecordSize`  AS merged_record_size
FROM paimon_obs.paimon_database.`${PAIMON_TABLE}$statistics`;

-- 注意:Paimon 1.1 的 $files 没有 file_source 列(后续版本才有),不要加。
-- partition_value:wide_table 为非分区表,固定空串;接入分区表时需扩展此列。
-- 表从未提交过时 $files 为 0 行,CROSS JOIN 不产生输出行,source_snapshot_id 不会出现 NULL。
-- file_path_md5:SR 侧 files 表的代理主键(原路径直接进主键超 SR 主键字节限制),必须随行产出。
INSERT INTO kafka_meta_files
SELECT
  '${PAIMON_CATALOG}'  AS catalog_name,
  '${PAIMON_DATABASE}' AS database_name,
  '${PAIMON_TABLE}'    AS table_name,
  s.latest_snapshot_id AS source_snapshot_id,
  f.file_path,
  MD5(f.file_path) AS file_path_md5,
  '${COLLECTOR_RUN_ID}' AS collector_run_id,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS collected_at,
  '' AS partition_value,
  f.bucket,
  f.file_format,
  f.schema_id,
  f.level,
  f.record_count,
  f.file_size_in_bytes,
  f.min_sequence_number,
  f.max_sequence_number,
  DATE_FORMAT(f.creation_time, 'yyyy-MM-dd HH:mm:ss') AS creation_time
FROM paimon_obs.paimon_database.`${PAIMON_TABLE}$files` f
CROSS JOIN (
  SELECT MAX(snapshot_id) AS latest_snapshot_id
  FROM paimon_obs.paimon_database.`${PAIMON_TABLE}$snapshots`
) s;

INSERT INTO kafka_meta_manifests
SELECT
  '${PAIMON_CATALOG}'  AS catalog_name,
  '${PAIMON_DATABASE}' AS database_name,
  '${PAIMON_TABLE}'    AS table_name,
  s.latest_snapshot_id AS source_snapshot_id,
  f.file_name,
  '${COLLECTOR_RUN_ID}' AS collector_run_id,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS collected_at,
  f.file_size,
  f.num_added_files,
  f.num_deleted_files,
  f.schema_id
FROM paimon_obs.paimon_database.`${PAIMON_TABLE}$manifests` f
CROSS JOIN (
  SELECT MAX(snapshot_id) AS latest_snapshot_id
  FROM paimon_obs.paimon_database.`${PAIMON_TABLE}$snapshots`
) s;

-- 分区汇总:非分区表恒为单行(partition_value='');接入分区表时改为 GROUP BY f.`partition` 展开。
INSERT INTO kafka_meta_partitions
SELECT
  '${PAIMON_CATALOG}'  AS catalog_name,
  '${PAIMON_DATABASE}' AS database_name,
  '${PAIMON_TABLE}'    AS table_name,
  s.latest_snapshot_id AS source_snapshot_id,
  '' AS partition_value,
  '${COLLECTOR_RUN_ID}' AS collector_run_id,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS collected_at,
  SUM(f.record_count)        AS record_count,
  COUNT(*)                   AS file_count,
  SUM(f.file_size_in_bytes)  AS file_size_in_bytes,
  DATE_FORMAT(MAX(f.creation_time), 'yyyy-MM-dd HH:mm:ss') AS last_update_time
FROM paimon_obs.paimon_database.`${PAIMON_TABLE}$files` f
CROSS JOIN (
  SELECT MAX(snapshot_id) AS latest_snapshot_id
  FROM paimon_obs.paimon_database.`${PAIMON_TABLE}$snapshots`
) s
GROUP BY s.latest_snapshot_id;

-- Bucket 汇总:识别 Bucket 倾斜/单 Bucket 过大的事实来源。
INSERT INTO kafka_meta_buckets
SELECT
  '${PAIMON_CATALOG}'  AS catalog_name,
  '${PAIMON_DATABASE}' AS database_name,
  '${PAIMON_TABLE}'    AS table_name,
  s.latest_snapshot_id AS source_snapshot_id,
  '' AS partition_value,
  f.bucket,
  '${COLLECTOR_RUN_ID}' AS collector_run_id,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS collected_at,
  SUM(f.record_count)        AS record_count,
  COUNT(*)                   AS file_count,
  SUM(f.file_size_in_bytes)  AS file_size_in_bytes,
  DATE_FORMAT(MAX(f.creation_time), 'yyyy-MM-dd HH:mm:ss') AS last_update_time
FROM paimon_obs.paimon_database.`${PAIMON_TABLE}$files` f
CROSS JOIN (
  SELECT MAX(snapshot_id) AS latest_snapshot_id
  FROM paimon_obs.paimon_database.`${PAIMON_TABLE}$snapshots`
) s
GROUP BY s.latest_snapshot_id, f.bucket;

END;

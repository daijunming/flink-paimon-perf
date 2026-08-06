-- 03_routine_load.sql —— Kafka → StarRocks 元数据 ODS 表的 Routine Load
--
-- 依赖:01_ods_tables.sql(表必须先建好)。
-- 执行方式:StarRocks 客户端执行一次;执行前把 ${...} 占位符替换为真实环境值。
--
-- 说明:
--   * topic 名与 SR 表名一一对应(rdw_ods_paimon_meta_*)。
--   * 采集侧 Flink kafka sink(format=json)输出的字段名与表列名完全一致,
--     因此不显式写 jsonpaths,由 StarRocks 按列名自动映射。
--   * PRIMARY KEY 表重复装载同主键记录即覆盖,Routine Load 重发/重放安全。
--   * 若现场走"既有 Flink 链路"入库而非 Routine Load,本文件可不执行,
--     但需保证该链路同样按列名映射到本组表。
--   * Kerberos 环境下的 Kafka 安全属性见文件末尾注释块。

USE RDW_DATA;

CREATE ROUTINE LOAD RDW_DATA.rl_rdw_ods_paimon_meta_snapshots ON rdw_ods_paimon_meta_snapshots
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_batch_interval" = "20",
  "strict_mode" = "false"
)
FROM KAFKA (
  "kafka_broker_list" = "${KAFKA_BOOTSTRAP_SERVERS}",
  "kafka_topic" = "rdw_ods_paimon_meta_snapshots",
  "property.group.id" = "paimon_meta_collect_sr",
  "property.kafka_default_offsets" = "OFFSET_END"
);

CREATE ROUTINE LOAD RDW_DATA.rl_rdw_ods_paimon_meta_statistics ON rdw_ods_paimon_meta_statistics
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_batch_interval" = "20",
  "strict_mode" = "false"
)
FROM KAFKA (
  "kafka_broker_list" = "${KAFKA_BOOTSTRAP_SERVERS}",
  "kafka_topic" = "rdw_ods_paimon_meta_statistics",
  "property.group.id" = "paimon_meta_collect_sr",
  "property.kafka_default_offsets" = "OFFSET_END"
);

CREATE ROUTINE LOAD RDW_DATA.rl_rdw_ods_paimon_meta_files ON rdw_ods_paimon_meta_files
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_batch_interval" = "20",
  "strict_mode" = "false"
)
FROM KAFKA (
  "kafka_broker_list" = "${KAFKA_BOOTSTRAP_SERVERS}",
  "kafka_topic" = "rdw_ods_paimon_meta_files",
  "property.group.id" = "paimon_meta_collect_sr",
  "property.kafka_default_offsets" = "OFFSET_END"
);

CREATE ROUTINE LOAD RDW_DATA.rl_rdw_ods_paimon_meta_manifests ON rdw_ods_paimon_meta_manifests
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_batch_interval" = "20",
  "strict_mode" = "false"
)
FROM KAFKA (
  "kafka_broker_list" = "${KAFKA_BOOTSTRAP_SERVERS}",
  "kafka_topic" = "rdw_ods_paimon_meta_manifests",
  "property.group.id" = "paimon_meta_collect_sr",
  "property.kafka_default_offsets" = "OFFSET_END"
);

CREATE ROUTINE LOAD RDW_DATA.rl_rdw_ods_paimon_meta_partitions ON rdw_ods_paimon_meta_partitions
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_batch_interval" = "20",
  "strict_mode" = "false"
)
FROM KAFKA (
  "kafka_broker_list" = "${KAFKA_BOOTSTRAP_SERVERS}",
  "kafka_topic" = "rdw_ods_paimon_meta_partitions",
  "property.group.id" = "paimon_meta_collect_sr",
  "property.kafka_default_offsets" = "OFFSET_END"
);

CREATE ROUTINE LOAD RDW_DATA.rl_rdw_ods_paimon_meta_buckets ON rdw_ods_paimon_meta_buckets
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_batch_interval" = "20",
  "strict_mode" = "false"
)
FROM KAFKA (
  "kafka_broker_list" = "${KAFKA_BOOTSTRAP_SERVERS}",
  "kafka_topic" = "rdw_ods_paimon_meta_buckets",
  "property.group.id" = "paimon_meta_collect_sr",
  "property.kafka_default_offsets" = "OFFSET_END"
);

CREATE ROUTINE LOAD RDW_DATA.rl_rdw_ods_paimon_meta_consumers ON rdw_ods_paimon_meta_consumers
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_batch_interval" = "20",
  "strict_mode" = "false"
)
FROM KAFKA (
  "kafka_broker_list" = "${KAFKA_BOOTSTRAP_SERVERS}",
  "kafka_topic" = "rdw_ods_paimon_meta_consumers",
  "property.group.id" = "paimon_meta_collect_sr",
  "property.kafka_default_offsets" = "OFFSET_END"
);

CREATE ROUTINE LOAD RDW_DATA.rl_rdw_ods_paimon_meta_options ON rdw_ods_paimon_meta_options
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_batch_interval" = "20",
  "strict_mode" = "false"
)
FROM KAFKA (
  "kafka_broker_list" = "${KAFKA_BOOTSTRAP_SERVERS}",
  "kafka_topic" = "rdw_ods_paimon_meta_options",
  "property.group.id" = "paimon_meta_collect_sr",
  "property.kafka_default_offsets" = "OFFSET_END"
);

CREATE ROUTINE LOAD RDW_DATA.rl_rdw_ods_paimon_meta_collect_runs ON rdw_ods_paimon_meta_collect_runs
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_batch_interval" = "20",
  "strict_mode" = "false"
)
FROM KAFKA (
  "kafka_broker_list" = "${KAFKA_BOOTSTRAP_SERVERS}",
  "kafka_topic" = "rdw_ods_paimon_meta_collect_runs",
  "property.group.id" = "paimon_meta_collect_sr",
  "property.kafka_default_offsets" = "OFFSET_END"
);

-- ==================== Kerberos 环境补充(可选)====================
-- Kafka 启用 SASL 时,在每条 FROM KAFKA 子句中追加(FE 节点需可访问 keytab):
--   "property.security.protocol" = "SASL_PLAINTEXT",
--   "property.sasl.mechanism" = "GSSAPI",
--   "property.sasl.kerberos.service.name" = "kafka",
--   "property.sasl.kerberos.keytab" = "/path/to/xxx.keytab",
--   "property.sasl.kerberos.principal" = "xxx@REALM"
-- 运维常用命令:
--   SHOW ROUTINE LOAD FOR rl_rdw_ods_paimon_meta_snapshots;
--   PAUSE/RESUME/STOP ROUTINE LOAD FOR rl_rdw_ods_paimon_meta_snapshots;

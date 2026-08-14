-- 04_physical_ods.sql —— 物理维度 ODS 表 + Routine Load(HDFS 侧事实,补系统表盲区)
--
-- 定位:$files 只含当前态数据文件(Paimon 1.1 无 file_source 列),changelog 文件与
-- 表目录真实占用只能从 HDFS 采(bin/collect_physical.sh:du -s + ls -R 按文件名前缀分类)。
-- 本表只存可复核事实;"总占用 vs 有效数据"的包袱拆解在分析层做
-- (口径见 docs/写入与合并性能分析.md 3.2 第 5 条)。
--
-- 依赖:无(独立于 01_ods_tables.sql,单独执行即可)。
-- 执行方式:StarRocks 客户端执行一次;执行前把 ${...} 占位符替换为真实环境值。
-- 对应 Kafka topic:rdw_ods_paimon_meta_physical(数据量极小,1 个分区即可)。
-- Kerberos 环境的 Kafka 安全属性追加方式见 03_routine_load.sql 末尾注释块。

USE RDW_DATA;

-- 物理维度采样:每轮一行(collect_physical.sh,crontab 建议每 10 分钟)。
-- 用于回答:表目录真实占用趋势、changelog 文件堆积、物理数据文件 vs $files 当前态
-- 的差值(历史包袱=快照窗口旧文件+changelog+孤儿文件)、元数据文件膨胀。
CREATE TABLE IF NOT EXISTS rdw_ods_paimon_meta_physical (
  catalog_name            VARCHAR(16)  NOT NULL,
  database_name           VARCHAR(24)  NOT NULL,
  table_name              VARCHAR(32)  NOT NULL,
  collected_at            DATETIME     NOT NULL COMMENT '采样时间',
  collector_run_id        VARCHAR(64)  NULL,
  total_bytes             BIGINT       NULL COMMENT 'du 逻辑大小(未乘副本)',
  total_bytes_replicated  BIGINT       NULL COMMENT 'du 含副本大小(Hadoop3 du 第二列;老版本无此列时=total_bytes)',
  file_count_total        BIGINT       NULL COMMENT '表目录下文件总数(ls -R)',
  data_file_count         BIGINT       NULL,
  data_bytes              BIGINT       NULL COMMENT 'data-* 物理合计,含快照窗口内旧文件/孤儿文件,恒≥$files 当前态',
  changelog_file_count    BIGINT       NULL,
  changelog_bytes         BIGINT       NULL COMMENT 'changelog-* 文件(changelog-producer 产物,系统表采不到)',
  other_file_count        BIGINT       NULL COMMENT 'manifest/snapshot/schema/index 等其余文件',
  other_bytes             BIGINT       NULL
) PRIMARY KEY(catalog_name, database_name, table_name, collected_at)
COMMENT 'Paimon 表物理维度采样(HDFS du/ls,collect_physical.sh 采集)'
DISTRIBUTED BY HASH(table_name) BUCKETS 3
PROPERTIES ("replication_num" = "3");  -- 按集群 BE 规模调整

CREATE ROUTINE LOAD RDW_DATA.rl_rdw_ods_paimon_meta_physical ON rdw_ods_paimon_meta_physical
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_batch_interval" = "20",
  "strict_mode" = "false"
)
FROM KAFKA (
  "kafka_broker_list" = "${KAFKA_BOOTSTRAP_SERVERS}",
  "kafka_topic" = "rdw_ods_paimon_meta_physical",
  "property.group.id" = "paimon_meta_collect_sr",
  "property.kafka_default_offsets" = "OFFSET_END"
);

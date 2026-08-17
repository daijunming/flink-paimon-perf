-- 05_kafka_offsets_ods.sql —— Kafka topic 位移 ODS 表 + Routine Load + 生产速率视图
--
-- 定位:collect_kafka_offsets.sh 周期采集各分区 latest offset(原始事实,只存不判);
-- 生产速率(条/秒)由本文件视图对相邻采样做"位移差/实际间隔秒"得到——
-- 与 analysis-sql 的吞吐口径一致:采样周期抖动、漏轮都按实际间隔秒归一,不影响正确性。
--
-- 依赖:无(独立于 01_ods_tables.sql,单独执行即可)。
-- 执行方式:StarRocks 客户端执行一次;执行前把 ${...} 占位符替换为真实环境值。
-- 对应 Kafka topic:rdw_ods_kafka_topic_offsets(1 个分区即可,每轮每分区一行)。
-- Kerberos 环境的 Kafka 安全属性追加方式见 03_routine_load.sql 末尾注释块。

USE RDW_DATA;

CREATE TABLE IF NOT EXISTS rdw_ods_kafka_topic_offsets (
  topic            VARCHAR(128) NOT NULL,
  partition_id     INT          NOT NULL,
  collected_at     DATETIME     NOT NULL COMMENT '采样时间',
  collector_run_id VARCHAR(64)  NULL,
  end_offset       BIGINT       NULL COMMENT '分区末端位移(latest,下一条待写入位置)'
) PRIMARY KEY(topic, partition_id, collected_at)
COMMENT 'Kafka topic 分区末端位移采样(collect_kafka_offsets.sh)'
DISTRIBUTED BY HASH(topic) BUCKETS 3
PROPERTIES ("replication_num" = "3");  -- 按集群 BE 规模调整

CREATE ROUTINE LOAD RDW_DATA.rl_rdw_ods_kafka_topic_offsets ON rdw_ods_kafka_topic_offsets
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_batch_interval" = "20",
  "strict_mode" = "false"
)
FROM KAFKA (
  "kafka_broker_list" = "${KAFKA_BOOTSTRAP_SERVERS}",
  "kafka_topic" = "rdw_ods_kafka_topic_offsets",
  "property.group.id" = "paimon_meta_collect_sr",
  "property.kafka_default_offsets" = "OFFSET_END"
);

-- 生产速率视图:topic 级合计位移的相邻采样差分/实际间隔秒;首轮为 NULL。
-- 漏一轮时自动按 120s 等实际间隔归一;分区扩缩容造成的单点跳变属真实事件,不归一。
CREATE OR REPLACE VIEW v_kafka_topic_write_rate AS
SELECT
  topic,
  collected_at,
  topic_end_offset,
  topic_end_offset - LAG(topic_end_offset) OVER (PARTITION BY topic ORDER BY collected_at) AS delta_records,
  (topic_end_offset - LAG(topic_end_offset) OVER (PARTITION BY topic ORDER BY collected_at))
    / NULLIF(UNIX_TIMESTAMP(collected_at)
             - LAG(UNIX_TIMESTAMP(collected_at)) OVER (PARTITION BY topic ORDER BY collected_at), 0)
    AS records_per_sec
FROM (
  SELECT topic, collected_at, SUM(end_offset) AS topic_end_offset
  FROM rdw_ods_kafka_topic_offsets
  GROUP BY topic, collected_at
) t;

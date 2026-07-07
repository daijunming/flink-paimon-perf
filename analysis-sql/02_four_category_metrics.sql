-- 02_four_category_metrics.sql —— 四类指标聚合（任务 8.3，Requirements 7.1-7.4）
-- 基于 metrics_view，按时段聚合计算四类指标。
-- 注意：本测试主要聚焦入湖写入性能，读取/查询类指标无专门作业产生，仅占位示意。

-- ==================== 类别1：写入与入湖性能（Requirements 7.1）====================
-- 数据源：入湖 Flink 作业 metrics（若 Flink 自动上报到 RDW_ODS_FLINK_METRICS）
-- 本实现假设 Flink 上报 numRecordsOut（累计写出条数）；若无则需由编排脚本调 REST API 补采。
CREATE VIEW IF NOT EXISTS metrics_ingest_perf AS
SELECT
  time_bucket_minute,
  -- 写入吞吐（条/秒）：相邻时段的 numRecordsOut 差值 / 时段秒数
  -- 简化：假设每分钟采样一次，取该桶内最大 metric_value（累计值）作为瞬时吞吐估算
  MAX(CASE WHEN metric_name = 'numRecordsOut' THEN metric_value ELSE 0 END) AS records_out_total,
  -- 入湖总耗时（秒）：从首条到末条的时间跨度（若有 processingTime 等指标可直接用）
  -- 占位：实际需从 Flink metrics 获取，这里仅示意结构
  0 AS ingest_duration_sec,
  -- 并发写入能力：可从 taskmanager 并行度或 Paimon bucket 数反映（静态配置，非时序指标）
  -- 占位：后续在 SLA 判定中关联静态参数
  NULL AS write_concurrency
FROM metrics_view
WHERE source = 'FLINK'  -- 假设 Flink metrics 也写入该表，source='FLINK'
  AND metric_name IN ('numRecordsOut', 'processingTime')
GROUP BY time_bucket_minute
ORDER BY time_bucket_minute;

-- 说明：若 Flink metrics 未自动上报到 RDW_ODS_FLINK_METRICS，需编排脚本调 REST API 补采并插入。

-- ==================== 类别2：更新与删除效率（Requirements 7.2）====================
-- 数据源：Paimon 元数据采集器（paimon.last.commit.kind：COMPACT=1.0 / APPEND=0.0 / 其他=0.5）
CREATE VIEW IF NOT EXISTS metrics_update_delete_eff AS
SELECT
  time_bucket_minute,
  -- commit kind 分布（COMPACT 占比反映 Update/Delete 触发 Compaction 频率）
  AVG(CASE WHEN metric_name = 'paimon.last.commit.kind' THEN metric_value ELSE NULL END) AS avg_commit_kind,
  SUM(CASE WHEN metric_name = 'paimon.last.commit.kind' AND metric_value = 1.0 THEN 1 ELSE 0 END) AS compact_count,
  COUNT(CASE WHEN metric_name = 'paimon.last.commit.kind' THEN 1 ELSE NULL END) AS total_commits
FROM metrics_view
WHERE source = 'PAIMON_METADATA'
  AND metric_name = 'paimon.last.commit.kind'
GROUP BY time_bucket_minute
ORDER BY time_bucket_minute;

-- ==================== 类别3：读取与查询性能（Requirements 7.3）====================
-- 占位：本测试无专门读取作业，若后续添加点查/OLAP 查询作业，可从其 metrics 获取延迟/QPS。
CREATE VIEW IF NOT EXISTS metrics_read_query_perf AS
SELECT
  time_bucket_minute,
  NULL AS query_latency_ms,
  NULL AS query_qps
FROM metrics_view
WHERE 1=0  -- 占位，无数据
GROUP BY time_bucket_minute;

-- ==================== 类别4：资源消耗与 Compaction 开销（Requirements 7.4）====================
-- 数据源：YARN/HDFS 资源采集器 + Paimon 元数据（文件数/Level 分布）
CREATE VIEW IF NOT EXISTS metrics_resource_compaction AS
SELECT
  time_bucket_minute,
  -- YARN CPU/内存（allocated/available）
  MAX(CASE WHEN metric_name = 'yarn.allocated.vcores' THEN metric_value ELSE NULL END) AS yarn_allocated_vcores,
  MAX(CASE WHEN metric_name = 'yarn.available.vcores' THEN metric_value ELSE NULL END) AS yarn_available_vcores,
  MAX(CASE WHEN metric_name = 'yarn.allocated.memory.mb' THEN metric_value ELSE NULL END) AS yarn_allocated_memory_mb,
  -- HDFS 存储利用率
  MAX(CASE WHEN metric_name = 'hdfs.capacity.used.bytes' THEN metric_value ELSE NULL END) AS hdfs_used_bytes,
  MAX(CASE WHEN metric_name = 'hdfs.capacity.total.bytes' THEN metric_value ELSE NULL END) AS hdfs_total_bytes,
  -- Paimon 小文件合并开销（文件总数 / Level 分布反映 Compaction 压力）
  MAX(CASE WHEN metric_name = 'paimon.file.count' THEN metric_value ELSE NULL END) AS paimon_file_count,
  -- Level-0 文件数（LSM 健康度核心指标：L0 堆积说明写入速度 > Compaction 速度）
  -- level 已编码进 metric_name（paimon.level.file.count.L0），无需解析 tags
  MAX(CASE WHEN metric_name = 'paimon.level.file.count.L0' THEN metric_value ELSE NULL END) AS level0_file_count,
  MAX(CASE WHEN metric_name = 'paimon.level.file.count.L1' THEN metric_value ELSE NULL END) AS level1_file_count,
  MAX(CASE WHEN metric_name = 'paimon.level.file.count.L2' THEN metric_value ELSE NULL END) AS level2_file_count,
  -- Level-0 数据量（字节），辅助判断 L0 堆积的数据规模
  MAX(CASE WHEN metric_name = 'paimon.level.size.bytes.L0' THEN metric_value ELSE NULL END) AS level0_size_bytes
FROM metrics_view
WHERE source IN ('YARN', 'HDFS', 'PAIMON_METADATA')
  AND metric_name IN (
    'yarn.allocated.vcores', 'yarn.available.vcores', 'yarn.allocated.memory.mb',
    'hdfs.capacity.used.bytes', 'hdfs.capacity.total.bytes',
    'paimon.file.count',
    'paimon.level.file.count.L0', 'paimon.level.file.count.L1', 'paimon.level.file.count.L2',
    'paimon.level.size.bytes.L0'
  )
GROUP BY time_bucket_minute
ORDER BY time_bucket_minute;

-- 说明：
-- 1. 四类指标按时段聚合，每个视图输出一行/时段，便于后续 JOIN 做综合分析。
-- 2. 若 Flink metrics 未上报，类别1 需编排脚本补采；类别3 本测试无读作业，占位。
-- 3. 类别2/4 依赖已实现的元数据/资源采集器，数据链路已通。

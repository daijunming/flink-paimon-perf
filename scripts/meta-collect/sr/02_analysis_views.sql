-- 02_analysis_views.sql —— 元数据分析层视图(查询时计算,不落存储)
--
-- 依赖:01_ods_tables.sql。
-- 定位:ODS 表(rdw_ods_paimon_meta_*)只保存可复核事实;本文件的视图是分析层产物,
-- 把事实解释成可直接观测的口径。Bucket/分区汇总已是 ODS 物理表
-- (rdw_ods_paimon_meta_buckets / _partitions,由 $files 按 Snapshot 聚合采集),
-- 此处不再重复建视图。
--
-- 用法:按 source_snapshot_id 对齐做相邻两轮差分,即可回答
-- "这两轮之间 L0 如何变化 / 文件大小分布如何变化"。

USE RDW_DATA;

-- 1) Level 汇总:每个 Snapshot 各 LSM Level 的文件数/体积/记录数
CREATE OR REPLACE VIEW v_paimon_meta_level_stats AS
SELECT
  catalog_name,
  database_name,
  table_name,
  source_snapshot_id,
  level,
  COUNT(*)                    AS file_count,
  SUM(file_size_in_bytes)     AS file_size_in_bytes,
  SUM(record_count)           AS record_count
FROM rdw_ods_paimon_meta_files
GROUP BY catalog_name, database_name, table_name, source_snapshot_id, level;

-- 2) 文件大小分布:每个 Snapshot 的有效文件大小统计(小文件/文件膨胀分析)
CREATE OR REPLACE VIEW v_paimon_meta_file_size_stats AS
SELECT
  catalog_name,
  database_name,
  table_name,
  source_snapshot_id,
  COUNT(*)                                   AS file_count,
  SUM(file_size_in_bytes)                    AS total_bytes,
  AVG(file_size_in_bytes)                    AS avg_bytes,
  MIN(file_size_in_bytes)                    AS min_bytes,
  MAX(file_size_in_bytes)                    AS max_bytes,
  PERCENTILE_APPROX(file_size_in_bytes, 0.5) AS p50_bytes,
  PERCENTILE_APPROX(file_size_in_bytes, 0.9) AS p90_bytes
FROM rdw_ods_paimon_meta_files
GROUP BY catalog_name, database_name, table_name, source_snapshot_id;

-- 07_olap_scan.sql —— OLAP 全表扫描作业（模拟 BI 报表，Requirements 7.3）
-- 周期性全表聚合统计（如每 5 分钟），观测：扫描延迟、并发读写下的查询吞吐、Compaction 对读放大的影响。
-- 
-- 说明：本脚本为**单次批量扫描**（读取最新snapshot静态视图），用于：
-- 1. 验证 Paimon 批读性能（OLAP场景）
-- 2. 与流式入湖作业并发运行，观测读写冲突（吞吐下降/延迟上升）
-- 3. 外层用 cron 周期调度（如每5分钟执行一次）模拟周期性报表
--
-- 注意：batch模式是刻意为之（OLAP场景需要一致性快照），不是流式查询。
-- 若要流式实时聚合，应改用 Tumble 窗口 + Paimon changelog 流读。

-- 批读模式：Paimon 表作为批 source（snapshot 读取）
SET 'execution.runtime-mode' = 'batch';

-- 聚合查询：统计当前 snapshot 的记录数、avg/sum/max 等指标（三级命名）
CREATE TEMPORARY TABLE default_catalog.default_database.olap_scan_results (
  snapshot_id BIGINT,
  total_records BIGINT,
  avg_c21_decimal DECIMAL(38,4),
  sum_c21_decimal DECIMAL(38,4),
  max_event_time BIGINT,
  scan_timestamp BIGINT
) WITH (
  'connector' = 'print'
);

-- OLAP 扫描：全表聚合（每次执行读取最新 snapshot）
INSERT INTO default_catalog.default_database.olap_scan_results
SELECT
  0 AS snapshot_id,  -- 实际需从 Paimon API 获取当前 snapshot id（本 SQL 简化为 0）
  COUNT(*) AS total_records,
  AVG(c21_decimal) AS avg_c21_decimal,
  SUM(c21_decimal) AS sum_c21_decimal,
  MAX(event_time) AS max_event_time,
  UNIX_TIMESTAMP() * 1000 AS scan_timestamp
FROM paimon_obs.paimon_database.wide_table;

-- 说明：
-- 1. execution.runtime-mode=batch：Paimon 表作为批 source，读取最新 snapshot（非流式读取 changelog）。
-- 2. 周期执行：本 SQL 为单次全表扫描；实际周期运行需外层编排脚本（如 cron 每 5 分钟调用一次）。
-- 3. 性能观测：
--    - scan_timestamp - max_event_time：粗略估算数据新鲜度（实际需结合延迟探针）
--    - 扫描耗时：通过 Flink Web UI 或 metrics 查看作业运行时长
--    - 并发冲突：若与 05_ingest_insert.sql 同时运行，观测扫描延迟是否因写入压力上升
-- 4. 阶段2 验证读写冲突：启动本作业后，入湖作业的写入吞吐、本作业的扫描延迟是否相互影响。
-- 5. 若要流式周期聚合，可改用 Flink SQL 的 Tumble 窗口 + Paimon 流读（changelog），但开销更大。

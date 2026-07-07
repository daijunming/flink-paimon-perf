-- init_phase2.sql —— 阶段2（生产模拟 SLA 验证）初始化脚本
-- 目标：贴合生产负载验证 SLA（吞吐 ≥ 20000 条/秒、端到端延迟 ≤ 3 分钟），支持 5-7 天连跑。
-- 限速在生成器侧开启到目标 rps；本作业 checkpoint 兼顾延迟。用 -i 在主脚本前执行。

-- 并行度贴合目标吞吐（非探上限），与生成器目标 rps 匹配
SET 'parallelism.default' = '8';

-- checkpoint 间隔兼顾端到端延迟 ≤ 3min：60s 间隔，配合 deduplicate 主键表使可见延迟可控
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.min-pause' = '30s';
SET 'execution.checkpointing.timeout' = '10min';

-- 连跑稳定性：精确一次语义
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

SET 'sql-client.execution.result-mode' = 'tableau';

-- DDL 变量注入：sink 表 bucket 数（阶段2 贴合目标负载，不必过大）
-- 修改为 15（3 的倍数），避免与 Kafka 3 分区不成比例导致倾斜
SET 'BUCKET_NUM' = '15';

-- Kafka source 起始位移：生产模拟从最新开始，模拟实时流入
SET 'SCAN_STARTUP_MODE' = 'latest-offset';

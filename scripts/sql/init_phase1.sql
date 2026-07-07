-- init_phase1.sql —— 阶段1（单表极限压测）初始化脚本
-- 目标：探写入吞吐与并发上限、建立性能基线。高并发、大 bucket、不限速（限速在生成器侧关）。
-- 用 -i 在主脚本前执行，注入运行参数与 DDL 变量。

-- 高并行度探上限
SET 'parallelism.default' = '32';

-- checkpoint 间隔较短：压测阶段快速落 snapshot 便于观测，但不必兼顾 SLA 延迟
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.min-pause' = '10s';
SET 'execution.checkpointing.timeout' = '5min';

-- 结果展示模式（sql-client 交互/提交通用）
SET 'sql-client.execution.result-mode' = 'tableau';

-- DDL 变量注入：sink 表 bucket 数（阶段1 取大以探并发上限）
-- 修改为 63（3 的倍数），避免与 Kafka 3 分区不成比例导致 0 号分区热点
SET 'BUCKET_NUM' = '63';

-- Kafka source 起始位移：压测从最早开始，吃满堆积
SET 'SCAN_STARTUP_MODE' = 'earliest-offset';

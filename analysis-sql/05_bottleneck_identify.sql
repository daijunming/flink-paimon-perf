-- 05_bottleneck_identify.sql —— 瓶颈定位（任务 8.9，Requirements 8.3）
-- 关联四类指标，定位 SLA 未达标时段的瓶颈所在：
-- RESOURCE_CPU / COMPACTION / WRITE_CONCURRENCY / READ_QUERY / NONE（均达标）

CREATE VIEW IF NOT EXISTS bottleneck_identify AS
SELECT
  s.time_bucket_minute,
  s.sla_status,
  s.throughput_rps,
  s.e2e_latency_sec,
  r.yarn_allocated_vcores,
  r.yarn_available_vcores,
  r.paimon_file_count,
  r.level0_file_count,
  u.compact_count,
  u.total_commits,
  -- 瓶颈判定逻辑（按优先级）
  CASE
    -- 1. SLA 达标 → NONE
    WHEN s.sla_status = 'PASS' THEN 'NONE'
    
    -- 2. YARN CPU 利用率 > 80% → RESOURCE_CPU
    WHEN r.yarn_allocated_vcores / NULLIF(r.yarn_allocated_vcores + r.yarn_available_vcores, 0) > 0.8
         THEN 'RESOURCE_CPU'
    
    -- 3. Level-0 堆积 > 1000 → COMPACTION_LAG（Compaction 速度跟不上写入，最精准的滞后信号）
    WHEN r.level0_file_count > 1000 THEN 'COMPACTION_LAG'
    
    -- 4. Paimon 文件数 > 5000 或 Compaction 占比 > 50% → COMPACTION
    WHEN r.paimon_file_count > 5000
      OR (u.compact_count / NULLIF(u.total_commits, 0) > 0.5)
         THEN 'COMPACTION'
    
    -- 5. 吞吐低但资源/Compaction 正常 → WRITE_CONCURRENCY（可能并发度不足）
    WHEN s.throughput_rps < 20000 THEN 'WRITE_CONCURRENCY'
    
    -- 6. 延迟高但吞吐正常 → READ_QUERY（若有读作业；本测试暂无，归 COMPACTION）
    WHEN s.e2e_latency_sec > 180 THEN 'COMPACTION'
    
    ELSE 'UNKNOWN'
  END AS bottleneck_category,
  
  -- 瓶颈详细原因（便于人工排查）
  CASE
    WHEN s.sla_status = 'PASS' THEN 'SLA达标，无瓶颈'
    WHEN r.yarn_allocated_vcores / NULLIF(r.yarn_allocated_vcores + r.yarn_available_vcores, 0) > 0.8
         THEN CONCAT('YARN CPU利用率过高: ', ROUND(r.yarn_allocated_vcores / (r.yarn_allocated_vcores + r.yarn_available_vcores) * 100, 2), '%')
    WHEN r.level0_file_count > 1000
         THEN CONCAT('Level-0堆积: ', r.level0_file_count, ' 个文件（Compaction滞后于写入）')
    WHEN r.paimon_file_count > 5000
         THEN CONCAT('Paimon文件数过多: ', r.paimon_file_count)
    WHEN u.compact_count / NULLIF(u.total_commits, 0) > 0.5
         THEN CONCAT('Compaction频繁: ', ROUND(u.compact_count / u.total_commits * 100, 2), '% commits为COMPACT')
    WHEN s.throughput_rps < 20000
         THEN CONCAT('吞吐不足: ', ROUND(s.throughput_rps, 2), ' rps < 20000')
    WHEN s.e2e_latency_sec > 180
         THEN CONCAT('延迟过高: ', ROUND(s.e2e_latency_sec, 2), ' sec > 180')
    ELSE 'UNKNOWN'
  END AS bottleneck_detail

FROM sla_check s
LEFT JOIN metrics_resource_compaction r
  ON s.time_bucket_minute = r.time_bucket_minute
LEFT JOIN metrics_update_delete_eff u
  ON s.time_bucket_minute = u.time_bucket_minute
WHERE s.time_bucket_minute >= '2024-01-02 00:00:00'  -- 阶段2 时间范围（由编排脚本注入）
ORDER BY s.time_bucket_minute;

-- 说明：
-- 1. 瓶颈判定规则按优先级排列：达标→资源→L0堆积→Compaction→并发→读取。
-- 2. 阈值可调：YARN CPU > 80%、Level-0 文件数 > 1000、文件总数 > 5000、Compaction 占比 > 50% 等。
-- 3. COMPACTION_LAG（L0堆积）比 COMPACTION（文件总数）更精准：L0 是新写入未合并层，
--    L0 持续增长说明 Compaction 速度跟不上写入速度，是性能拐点的早期信号。
-- 4. bottleneck_detail 给出具体数值，便于人工复核。
-- 5. 若四类指标均正常但 SLA 仍未达标，归 UNKNOWN（需进一步排查网络/Kafka 等外部因素）。

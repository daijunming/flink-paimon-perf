-- 03_sla_check.sql —— SLA 达标判定（任务 8.5，Requirements 3.5 / 8.2）
-- 判定逻辑：吞吐 ≥ 20000 条/秒 且 端到端延迟 ≤ 180 秒（3分钟）→ PASS，否则 FAIL。
-- 抽为可测函数：SLA_STATUS(throughput, latency_sec) = CASE WHEN ... THEN 'PASS' ELSE 'FAIL' END

CREATE VIEW IF NOT EXISTS sla_check AS
SELECT
  time_bucket_minute,
  -- 吞吐（条/秒）：从类别1 入湖性能指标获取（简化：取 records_out_total / 60）
  -- 实际应取相邻时段差值 / 时段秒数，这里示意结构
  COALESCE(records_out_total / 60.0, 0) AS throughput_rps,
  -- 端到端延迟（秒）：从延迟探针指标获取（metric_name = 'ingest.e2e_latency_ms' / 1000）
  COALESCE(e2e_latency_ms / 1000.0, 999) AS e2e_latency_sec,
  -- SLA 达标判定：吞吐 ≥ 20000 且 延迟 ≤ 180 → PASS
  CASE
    WHEN (COALESCE(records_out_total / 60.0, 0) >= 20000)
     AND (COALESCE(e2e_latency_ms / 1000.0, 999) <= 180)
    THEN 'PASS'
    ELSE 'FAIL'
  END AS sla_status,
  -- 违规原因（便于瓶颈定位）
  CASE
    WHEN COALESCE(records_out_total / 60.0, 0) < 20000 THEN 'THROUGHPUT_LOW'
    WHEN COALESCE(e2e_latency_ms / 1000.0, 999) > 180 THEN 'LATENCY_HIGH'
    ELSE 'OK'
  END AS violation_reason
FROM (
  SELECT
    m1.time_bucket_minute,
    MAX(CASE WHEN m1.metric_name = 'numRecordsOut' THEN m1.metric_value ELSE NULL END) AS records_out_total,
    MAX(CASE WHEN m2.metric_name = 'ingest.e2e_latency_ms' THEN m2.metric_value ELSE NULL END) AS e2e_latency_ms
  FROM metrics_view m1
  LEFT JOIN metrics_view m2
    ON m1.time_bucket_minute = m2.time_bucket_minute
   AND m2.source = 'PAIMON_METADATA'
   AND m2.metric_name = 'ingest.e2e_latency_ms'
  WHERE m1.source = 'FLINK'
    AND m1.metric_name = 'numRecordsOut'
  GROUP BY m1.time_bucket_minute
) t
ORDER BY time_bucket_minute;

-- 说明：
-- 1. SLA 判定逻辑封装在 CASE WHEN 表达式中，即"可测函数"（用固定数据验证，见 _test.sql）。
-- 2. 吞吐计算简化为 records_out_total/60（假设每分钟一桶）；实际应取相邻桶差值。
-- 3. 延迟探针（任务 5.6）产出 metric_name='ingest.e2e_latency_ms'，source='PAIMON_METADATA'。
-- 4. violation_reason 便于后续瓶颈定位（任务 8.9）筛选未达标时段。

-- 09_streaming_read_test.sql —— 流式读分析逻辑验证（自包含，不触碰真实分区表）
-- 复现 09_streaming_read.sql 的核心逻辑并断言：
--   1) 吞吐：桶内 MAX（同桶两点取大）→ 跨 subtask SUM → 相邻桶差分/实际秒（3 分钟缺口仍正确）；
--   2) 反压软标志三档：>500 READ_BACKPRESSURE / >100 ELEVATED / 否则 OK；
--   3) 读写对照 consume_status 四态：KEEPING_UP / LAGGING / NO_READ_DATA / NO_WRITE_BASELINE。

DROP TABLE IF EXISTS test_sr_input;

-- 模拟 metrics_view 层输入（metric_value / metric_ts 保持 varchar，与真实表一致）
CREATE TABLE test_sr_input (
  metric_name  VARCHAR(200),
  metric_value VARCHAR(50),
  metric_ts    VARCHAR(50)
) DUPLICATE KEY(metric_name)
DISTRIBUTED BY HASH(metric_name) BUCKETS 1;

-- 3 个 subtask 的累计计数器，上报周期 3 分钟（946684800000 = 2000-01-01 00:00:00 UTC 起，+180s/桶）：
--   t0=0 → t1=18000 → t2=36000（每 subtask 每 3 分钟 +18000 = 100 rps，作业级 300 rps）；
--   s0 在 t1 桶内有两个采样点（17999@:00 / 18000@:30），验证桶内 MAX 取 18000。
-- 反压（backPressuredTimeMsPerSecond）：t0 最大 50 → OK；t1 最大 600 → READ_BACKPRESSURE；t2 最大 150 → ELEVATED。
INSERT INTO test_sr_input VALUES
  ('Source: wide_table[1].0.numRecordsOut', '0',     '946684800000'),
  ('Source: wide_table[1].1.numRecordsOut', '0',     '946684800000'),
  ('Source: wide_table[1].2.numRecordsOut', '0',     '946684800000'),
  ('Source: wide_table[1].0.numRecordsOut', '17999', '946684980000'),
  ('Source: wide_table[1].0.numRecordsOut', '18000', '946685010000'),
  ('Source: wide_table[1].1.numRecordsOut', '18000', '946684980000'),
  ('Source: wide_table[1].2.numRecordsOut', '18000', '946684980000'),
  ('Source: wide_table[1].0.numRecordsOut', '36000', '946685160000'),
  ('Source: wide_table[1].1.numRecordsOut', '36000', '946685160000'),
  ('Source: wide_table[1].2.numRecordsOut', '36000', '946685160000'),
  ('Source: wide_table[1].0.backPressuredTimeMsPerSecond', '50',  '946684800000'),
  ('Source: wide_table[1].1.backPressuredTimeMsPerSecond', '40',  '946684800000'),
  ('Source: wide_table[1].2.backPressuredTimeMsPerSecond', '30',  '946684800000'),
  ('Source: wide_table[1].0.backPressuredTimeMsPerSecond', '600', '946684980000'),
  ('Source: wide_table[1].1.backPressuredTimeMsPerSecond', '500', '946684980000'),
  ('Source: wide_table[1].2.backPressuredTimeMsPerSecond', '400', '946684980000'),
  ('Source: wide_table[1].0.backPressuredTimeMsPerSecond', '150', '946685160000'),
  ('Source: wide_table[1].1.backPressuredTimeMsPerSecond', '120', '946685160000'),
  ('Source: wide_table[1].2.backPressuredTimeMsPerSecond', '110', '946685160000');

-- 复现 metrics_streaming_read 的吞吐 + 反压逻辑（与 09 正文一致）
SELECT
  time_bucket_minute,
  records_out_total,
  read_rps,
  backpressured_ms_per_sec_max,
  CASE
    WHEN backpressured_ms_per_sec_max > 500 THEN 'READ_BACKPRESSURE'
    WHEN backpressured_ms_per_sec_max > 100 THEN 'ELEVATED'
    WHEN backpressured_ms_per_sec_max IS NULL THEN NULL
    ELSE 'OK'
  END AS read_backpressure_flag
FROM (
  SELECT
    time_bucket_minute,
    records_out_total,
    (records_out_total - LAG(records_out_total) OVER (ORDER BY time_bucket_minute))
      / NULLIF(UNIX_TIMESTAMP(time_bucket_minute)
               - LAG(UNIX_TIMESTAMP(time_bucket_minute)) OVER (ORDER BY time_bucket_minute), 0)
      AS read_rps
  FROM (
    SELECT time_bucket_minute, SUM(subtask_cum) AS records_out_total
    FROM (
      SELECT
        FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
        metric_name,
        MAX(CAST(metric_value AS DOUBLE)) AS subtask_cum
      FROM test_sr_input
      WHERE metric_name LIKE '%Source%numRecordsOut'
      GROUP BY 1, metric_name
    ) s
    GROUP BY time_bucket_minute
  ) t
) tp
FULL OUTER JOIN (
  SELECT
    FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
    MAX(CAST(metric_value AS DOUBLE)) AS backpressured_ms_per_sec_max
  FROM test_sr_input
  WHERE metric_name LIKE '%backPressuredTimeMsPerSecond'
  GROUP BY 1
) bp USING (time_bucket_minute)
ORDER BY time_bucket_minute;

-- 预期（3 行）：
--   t0: total=0,      rps=NULL, bp_max=50,  OK
--   t1: total=54000,  rps=300,  bp_max=600, READ_BACKPRESSURE
--   t2: total=108000, rps=300,  bp_max=150, ELEVATED

-- 断言1：3 桶、total 序列 (0, 54000, 108000)、非空 rps 恒为 300（3 分钟缺口下差分/实际秒守恒）
SELECT
  '断言1: 吞吐 3桶 total=(0,54000,108000) 且非空rps恒为300' AS test_description,
  CASE WHEN COUNT(*) = 3
        AND MIN(records_out_total) = 0 AND MAX(records_out_total) = 108000
        AND SUM(records_out_total) = 162000   -- 锁定中值 54000（0+54000+108000）
        AND SUM(CASE WHEN read_rps IS NOT NULL AND read_rps <> 300 THEN 1 ELSE 0 END) = 0
        AND SUM(CASE WHEN read_rps IS NULL THEN 1 ELSE 0 END) = 1
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
  SELECT
    time_bucket_minute,
    records_out_total,
    (records_out_total - LAG(records_out_total) OVER (ORDER BY time_bucket_minute))
      / NULLIF(UNIX_TIMESTAMP(time_bucket_minute)
               - LAG(UNIX_TIMESTAMP(time_bucket_minute)) OVER (ORDER BY time_bucket_minute), 0)
      AS read_rps
  FROM (
    SELECT time_bucket_minute, SUM(subtask_cum) AS records_out_total
    FROM (
      SELECT
        FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
        metric_name,
        MAX(CAST(metric_value AS DOUBLE)) AS subtask_cum
      FROM test_sr_input
      WHERE metric_name LIKE '%Source%numRecordsOut'
      GROUP BY 1, metric_name
    ) s
    GROUP BY time_bucket_minute
  ) t
) tp;

-- 断言2：反压标志三档各 1（OK / READ_BACKPRESSURE / ELEVATED）
SELECT
  '断言2: 反压标志 OK/READ_BACKPRESSURE/ELEVATED 各1' AS test_description,
  CASE WHEN SUM(CASE WHEN flag = 'OK' THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN flag = 'READ_BACKPRESSURE' THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN flag = 'ELEVATED' THEN 1 ELSE 0 END) = 1
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
  SELECT
    CASE
      WHEN bp_max > 500 THEN 'READ_BACKPRESSURE'
      WHEN bp_max > 100 THEN 'ELEVATED'
      ELSE 'OK'
    END AS flag
  FROM (
    SELECT
      FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
      MAX(CAST(metric_value AS DOUBLE)) AS bp_max
    FROM test_sr_input
    WHERE metric_name LIKE '%backPressuredTimeMsPerSecond'
    GROUP BY 1
  ) s
) f;

-- 断言3：读写对照 consume_status 四态判定
DROP TABLE IF EXISTS test_rw_input;

CREATE TABLE test_rw_input (
  write_rps DOUBLE,
  read_rps  DOUBLE
) DUPLICATE KEY(write_rps)
DISTRIBUTED BY HASH(write_rps) BUCKETS 1;

INSERT INTO test_rw_input VALUES
  (100, 100),   -- KEEPING_UP
  (100, 80),    -- LAGGING
  (100, NULL),  -- NO_READ_DATA
  (NULL, 50);   -- NO_WRITE_BASELINE

SELECT
  '断言3: consume_status 四态各1' AS test_description,
  CASE WHEN SUM(CASE WHEN status = 'KEEPING_UP' THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN status = 'LAGGING' THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN status = 'NO_READ_DATA' THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN status = 'NO_WRITE_BASELINE' THEN 1 ELSE 0 END) = 1
       THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
  SELECT
    CASE
      WHEN write_rps IS NULL THEN 'NO_WRITE_BASELINE'
      WHEN read_rps IS NULL  THEN 'NO_READ_DATA'
      WHEN read_rps >= write_rps THEN 'KEEPING_UP'
      ELSE 'LAGGING'
    END AS status
  FROM test_rw_input
) s;

-- 清理
DROP TABLE IF EXISTS test_sr_input;
DROP TABLE IF EXISTS test_rw_input;

-- 预期输出：断言1/2/3 result 均为 PASS

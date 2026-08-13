-- 09_streaming_read.sql —— 场景3：流式读取性能（Flink 流读 Paimon changelog）
-- 口径来源：docs/观测指标地图.md 场景3；作业脚本 scripts/sql/07_streaming_read.sql
-- （blackhole sink，只测流读本身、不引入 sink 开销），job_name = 'streaming_read_job'
-- （若实际提交的作业名不同，改 01_metrics_view.sql 白名单即可，本文件不用动）。
--
-- 指标形态与写入作业相同：任务级 '<算子名>.<subtask下标>.<指标短名>'，按 subtask 分行，
-- 经既有 Flink 链路上报（每 3 分钟一批）。因此吞吐同样用
-- 「桶内 MAX → 单算子内跨 subtask SUM → 跨算子 MAX 去重 → 相邻桶差分/实际间隔秒」
-- （不写死 60，兼容 3 分钟上报与采样缺口）。跨算子去重防任务级链名/算子级名双粒度重复
-- 上报（写入作业已现场核实存在，见 02 类别1a；读作业是否存在同形态见下方"确认上报"，
-- 单粒度下 MAX 去重结果不变，属安全兜底）。
--
-- 视图：
--   1) metrics_streaming_read —— 流读吞吐 + Source 反压/繁忙 + 软标志（阈值来自指标地图场景3）
--   2) metrics_read_vs_write  —— 读写对照：消费是否跟得上写入（场景5 核心读数）
--
-- 不在本文件的内容：
--   * 数据可见延迟 / 快照停滞：复用 08_checkpoint_health 的 commit_interval_sec / STALL。
--   * currentFetchEventTimeLag、lastCheckpointDuration：未确认上报（Paimon source 可能不暴露
--     event-time lag；lastCheckpointDuration 是作业级指标，既有链路可能只采任务级），
--     不放占位列；用下方"确认上报"查询核实后，按短名后缀加列即可。
--
-- 确认上报（部署后先跑一次）：
--   SELECT DISTINCT metric_name FROM RDW_DATA.RDW_ODS_FLINK_METRICS
--   WHERE job_name = 'streaming_read_job' AND etl_dt = '<最新分区日期>';
--   预期命中 'Source: ...%numRecordsOut' 与 '%backPressuredTimeMsPerSecond'。
--   若 '%Source%numRecordsOut' 剥掉 '<subtask>.numRecordsOut' 后缀后剩多种算子名
--   = 任务级/算子级双粒度重复上报（写入作业已核实存在该形态，见 02 类别1a）：
--   视图已按算子名收敛取 MAX 去重，不受影响。

-- ==================== 视图1：流读吞吐 + Source 反压 ====================
CREATE OR REPLACE VIEW RDW_DATA.metrics_streaming_read AS
WITH buckets AS (
  SELECT DISTINCT time_bucket_minute
  FROM RDW_DATA.metrics_view
  WHERE job_name = 'streaming_read_job'
),
tp AS (
  -- 吞吐：Source 算子 numRecordsOut 是累计计数器，三层聚合（①桶内MAX→②单算子内跨subtask SUM→③跨算子MAX去重）
  SELECT
    time_bucket_minute,
    records_out_total,
    -- 流读吞吐(rps)：相邻桶累计差 / 实际间隔秒；首桶为 NULL
    (records_out_total - LAG(records_out_total) OVER (ORDER BY time_bucket_minute))
      / NULLIF(UNIX_TIMESTAMP(time_bucket_minute)
               - LAG(UNIX_TIMESTAMP(time_bucket_minute)) OVER (ORDER BY time_bucket_minute), 0)
      AS read_rps
  FROM (
    SELECT
      time_bucket_minute,
      MAX(op_total) AS records_out_total           -- ③ 跨算子 MAX 去重（双粒度计数相同，不可 SUM）
    FROM (
      SELECT
        time_bucket_minute,
        operator_name,
        SUM(subtask_cum) AS op_total               -- ② 单算子内：各 subtask 累计求和
      FROM (
        SELECT
          time_bucket_minute,
          -- 剥掉末尾 '<subtask>.numRecordsOut' 得到算子名（两种上报粒度在此分开）
          REGEXP_REPLACE(metric_name, '\\.[0-9]+\\.numRecordsOut$', '') AS operator_name,
          subtask_cum
        FROM (
          SELECT
            time_bucket_minute,
            metric_name,
            MAX(metric_value) AS subtask_cum       -- ① 累计计数器：桶内取最大≈桶末值
          FROM RDW_DATA.metrics_view
          WHERE job_name = 'streaming_read_job'
            AND metric_name LIKE '%Source%numRecordsOut'  -- 结尾非 PerSecond，天然排除速率指标
          GROUP BY time_bucket_minute, metric_name
        ) m
      ) s
      GROUP BY time_bucket_minute, operator_name
    ) o
    GROUP BY time_bucket_minute
  ) t
),
bp AS (
  -- 反压/繁忙：task 级指标，桶内取最大（最差 task）与平均；busy 是 backPressured 未上报时的替代
  SELECT
    time_bucket_minute,
    MAX(CASE WHEN metric_name LIKE '%backPressuredTimeMsPerSecond' THEN metric_value END) AS backpressured_ms_per_sec_max,
    AVG(CASE WHEN metric_name LIKE '%backPressuredTimeMsPerSecond' THEN metric_value END) AS backpressured_ms_per_sec_avg,
    MAX(CASE WHEN metric_name LIKE '%busyTimeMsPerSecond'          THEN metric_value END) AS busy_ms_per_sec_max,
    AVG(CASE WHEN metric_name LIKE '%busyTimeMsPerSecond'          THEN metric_value END) AS busy_ms_per_sec_avg
  FROM RDW_DATA.metrics_view
  WHERE job_name = 'streaming_read_job'
    AND (metric_name LIKE '%backPressuredTimeMsPerSecond' OR metric_name LIKE '%busyTimeMsPerSecond')
  GROUP BY time_bucket_minute
)
SELECT
  b.time_bucket_minute,
  tp.records_out_total           AS read_records_out_total,  -- 作业级累计读取条数
  tp.read_rps,                                                 -- 流读吞吐（实际窗口均值）
  bp.backpressured_ms_per_sec_max,                           -- 反压最严重 task（ms/s，1000=全程反压）
  bp.backpressured_ms_per_sec_avg,
  bp.busy_ms_per_sec_max,                                    -- 繁忙度（接近 1000 = 满负荷）
  bp.busy_ms_per_sec_avg,
  -- 软标志（阈值来自指标地图场景3：<100 良好；>500 告警；可调）
  CASE
    WHEN bp.backpressured_ms_per_sec_max > 500 THEN 'READ_BACKPRESSURE'
    WHEN bp.backpressured_ms_per_sec_max > 100 THEN 'ELEVATED'
    WHEN bp.backpressured_ms_per_sec_max IS NULL THEN NULL   -- 该桶无反压数据（未上报）
    ELSE 'OK'
  END AS read_backpressure_flag
FROM buckets b
LEFT JOIN tp ON b.time_bucket_minute = tp.time_bucket_minute
LEFT JOIN bp ON b.time_bucket_minute = bp.time_bucket_minute;

-- ==================== 视图2：读写对照（消费是否跟得上写入） ====================
-- 以写入作业吞吐为基准 LEFT JOIN 流读：读侧 NULL = 该分钟无读数据（作业未启动/未上报）。
-- unconsumed_records = 写累计 − 读累计，趋势持续扩大 = 消费滞后在累积。
-- 注意：读作业 scan.mode 默认（当前快照全量 + 增量），启动初期 read_rps 因追全量会冲高，
-- 追平后稳态应与 write_rps 持平；判读 consume_status 时排除追数据阶段。
CREATE OR REPLACE VIEW RDW_DATA.metrics_read_vs_write AS
SELECT
  w.time_bucket_minute,
  w.throughput_rps                     AS write_rps,
  r.read_rps,
  w.records_out_total                  AS write_records_total,
  r.read_records_out_total             AS read_records_total,
  w.records_out_total - r.read_records_out_total AS unconsumed_records,
  CASE
    WHEN w.throughput_rps IS NULL THEN 'NO_WRITE_BASELINE'  -- 写入侧首桶/缺口，无基准
    WHEN r.read_rps IS NULL       THEN 'NO_READ_DATA'       -- 读作业未启动/未上报
    WHEN r.read_rps >= w.throughput_rps THEN 'KEEPING_UP'
    ELSE 'LAGGING'
  END AS consume_status
FROM RDW_DATA.metrics_ingest_perf w
LEFT JOIN RDW_DATA.metrics_streaming_read r
  ON w.time_bucket_minute = r.time_bucket_minute;

-- 说明：
-- 1. 与写入侧一致的「单算子内 subtask 求和 + 跨算子 MAX 去重 + 实际秒差分」是本任务级指标的正确聚合方式；3 分钟上报下
--    本视图每 3 分钟一行有效数据，read_rps 是该窗口的平均速率（数值正确，时间分辨率=上报周期）。
-- 2. LAGGING 持续出现 且 unconsumed_records 持续扩大 = 消费跟不上写入；结合
--    read_backpressure_flag（读侧反压）与 08 的 STALL（快照停滞）定位"读不动"还是"没新数据"。
-- 3. 查询建议加时间范围：SELECT * FROM RDW_DATA.metrics_read_vs_write
--    WHERE time_bucket_minute BETWEEN '<起>' AND '<止>' ORDER BY time_bucket_minute;

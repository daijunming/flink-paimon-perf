# Paimon 性能测试 · 指标地图（按测试场景分类）

> **测试目标**：评估 Flink + Paimon 主键表在**流式计算场景**（先全量后增量）的功能完整性与性能表现，覆盖：
> - 流式写入（高频 I/U/D）
> - 流式合并（独立 Compaction 作业，流模式持续合并）
> - 流式读取（全量+增量消费 changelog）
> - 流式聚合（持续 running 聚合）
> - 维度表关联（Paimon 作为维度表，仅功能验证）
>
> **数据新鲜度目标**：≤5 分钟（由 checkpoint 周期控制，当前配置 3 分钟）
>
> **测试阶段**：
> - 阶段 1：单表极限压测（不限速，探上限，建立吞吐基线）
> - 阶段 2：生产模拟负载（限速 20000 rps，连续运行 5-7 天，验证 SLA）

---

## 📋 测试场景 → 指标映射速查表

| 测试场景 | 关注指标 | 数据来源（job_name） | 分析视图 |
|---------|---------|-------------------|---------|
| **场景 1：单写入性能** | 写入吞吐、反压、checkpoint 成功率 | `DataStreamperf_paimon` | `metrics_ingest_perf`<br>`metrics_write_health` |
| **场景 2：单合并性能** | Compaction 繁忙度/耗时、L0 堆积、资源占用 | `compaction_job`<br>`wide_table`<br>`cluster` | `metrics_compaction_job`<br>`metrics_resource_compaction`<br>`health_flags` |
| **场景 3：单查询性能（流式读）** | 流读吞吐、Source 反压、数据可见延迟 | `streaming_read_job`<br>（`scripts/sql/07_streaming_read.sql`，待运行）<br>`wide_table`（快照时间） | `metrics_streaming_read`<br>`metrics_read_vs_write`（已建）<br>`checkpoint_health`（已建） |
| **场景 4：单聚合性能** | 聚合吞吐、状态大小、算子繁忙度 | `streaming_agg_job`<br>（`scripts/sql/08_streaming_agg.sql`，待运行） | 待建视图 |
| **场景 5：并发读写** | 写入吞吐波动、Compaction 相互影响、资源争抢 | 多作业组合观测 | 综合使用上述视图 |

---

## 🎯 场景 1：单写入性能测试

### 测试目的
验证 Paimon 主键表在**流式写入场景**（先全量后增量，高频 I/U/D）的吞吐能力、稳定性和 checkpoint 健康度。

### 前置验证（运行测试前确认）
- [ ] 确认写入作业已提交（job_name = `DataStreamperf_paimon`）
- [ ] 确认 `RDW_DATA.metrics_ingest_perf` 视图有数据（运行 `analysis-sql/01_metrics_view_test.sql` 验证逻辑）
- [ ] 确认 Flink metrics reporter 上报了 `numRecordsOut`（查询 `RDW_ODS_FLINK_METRICS` 原始表）
- [ ] 确认 metadata-collector 正常采集快照数据（`paimon.snapshot.*` 指标存在）

### 关注指标（优先级排序）

#### ⭐ 核心指标

| 指标名称 | metric_name（LIKE 匹配） | 观测目的 | 健康标准 |
|---------|------------------------|---------|---------|
| **写入吞吐（records/s）** | `%ConstraintEnforcer%numRecordsOut` | 衡量写入能力是否达标，建立吞吐基线 | **阶段 1**：探上限，无固定标准<br>**阶段 2**：稳定 ≥ 20000 rps |
| **反压指标** | `%checkpointStartDelayNanos` | 判断写入是否到上限（被 compaction/资源拖慢） | **良好**：< 10 秒<br>**注意**：10-30 秒<br>**告警**：持续 > 30 秒（触发 `health_flags.backpressure_flag = 'BACKPRESSURE'`） |
| **Checkpoint 成功率** | `%numberOfCompletedCheckpoints`<br>`%numberOfFailedCheckpoints` | 写入稳定性的核心指标 | **目标**：> 99%<br>⚠️ **若此指标未上报**：观察快照推进是否停滞（替代判断方式） |
| **快照推进速度** | `paimon.snapshot.id`<br>`paimon.snapshot.time.millis` | 数据提交是否正常、可见延迟 | **良好**：间隔 ~3 分钟<br>**注意**：间隔 3-5 分钟<br>**告警**：间隔 > 5 分钟（触发 `checkpoint_stall_alert.health_status = 'STALE'`） |

#### 📊 辅助指标

| 指标名称 | metric_name | 观测目的 | 缺失时的替代方案 |
|---------|-------------|---------|---------------|
| Checkpoint 耗时 | `%lastCheckpointDuration` | 判断 checkpoint 是否成为瓶颈 | ⚠️ 若未上报：观察反压指标推断 |
| Buffer 使用率 | `%buffers%usage` | 判断是否有内存压力 | ⚠️ 若未上报：观察 YARN 内存指标 |
| 提交类型分布 | `paimon.last.commit.kind`（数值编码） | 区分 COMPACT(1.0) / APPEND(0.0) / 其他(0.5) | — |

### 数据来源
- `DataStreamperf_paimon`：Flink 写入作业的原生指标
- `wide_table`：Paimon 元数据采集器（轮询 $snapshots 系统表）

### 分析视图
```sql
-- 写入吞吐（records_out_total / throughput_rps）
SELECT * FROM RDW_DATA.metrics_ingest_perf
WHERE time_bucket_minute BETWEEN '<起>' AND '<止>'
ORDER BY time_bucket_minute;

-- 写入健康度（反压信号 max_checkpoint_start_delay_ms）
SELECT * FROM RDW_DATA.metrics_write_health
WHERE time_bucket_minute BETWEEN '<起>' AND '<止>'
ORDER BY time_bucket_minute;

-- 快照推进 / 停滞（snapshot_id 增量、提交间隔）
SELECT * FROM RDW_DATA.checkpoint_health
WHERE time_bucket_minute BETWEEN '<起>' AND '<止>'
ORDER BY time_bucket_minute;
```

---

## ⚙️ 场景 2：Compaction 性能测试（独立流式合并作业）

### 测试目的
评估**独立 Compaction 作业**（`paimon-flink-action compact`，流模式持续合并，见 `scripts/sql/06_compaction_job.sh`）在高频 I/U/D 场景下的合并能力、资源开销和文件管理效率。

### 前置验证（运行测试前确认）
- [ ] 确认 Compaction 作业已启动（job_name = `compaction_job`）
- [ ] 确认 Paimon 桥接指标已上报（`compactionThreadBusy`、`avgCompactionTime`）
- [ ] 确认 metadata-collector 正常采集文件数据（`paimon.level.file.count.*` 指标存在）
- [ ] 确认 resource-collector 正常采集集群资源（`yarn.*`、`hdfs.*` 指标存在）

### 关注指标（优先级排序）

#### ⭐ 核心指标

| 指标名称 | metric_name（LIKE 匹配） | 观测目的 | 健康标准 |
|---------|------------------------|---------|---------|
| **Compaction 繁忙度（%）** | `%compactionThreadBusy` | 合并线程是否满负荷 | **良好**：< 70%<br>**注意**：70-90%<br>**告警**：≥ 90%（触发 `health_flags.compaction_flag = 'COMPACTION_SATURATED'`，需加资源） |
| **L0 文件堆积** | `paimon.level.file.count.L0` | 最直接的合并滞后信号 | **良好**：稳定或缓慢增长<br>**告警**：持续快速增长（触发 `health_flags.l0_flag = 'L0_PILEUP'`） |
| **Compaction 平均耗时（ms）** | `%avgCompactionTime` | 单次合并的时间代价 | **良好**：< 30 秒<br>**注意**：30-60 秒<br>**告警**：> 60 秒 |
| **Compaction 提交占比** | `paimon.last.commit.kind`<br>（编码值：COMPACT=1.0 / APPEND=0.0 / 其他=0.5） | 合并触发频率（U/D 越多占比越高） | 根据 U/D 比例（约 50%）合理即可，无固定标准 |

#### 📊 辅助指标

| 指标名称 | metric_name | 观测目的 | 说明 |
|---------|-------------|---------|------|
| 总文件数 | `paimon.file.count` | 文件规模 = Compaction 压力总量 | — |
| L1-L5 文件分布 | `paimon.level.file.count.L1/L2/.../L5` | LSM Tree 层级健康度 | — |
| YARN CPU 占用 | `yarn.allocated.vcores` | 集群级 CPU 占用 | ⚠️ **集群级指标**，无法拆分到单作业 |
| YARN 内存占用 | `yarn.allocated.memory.mb` | 集群级内存占用 | ⚠️ **集群级指标**，无法拆分到单作业 |
| HDFS 存储增长 | `hdfs.capacity.used.bytes` | 存储水位与增长速率 | ⚠️ **集群级指标**，包含所有表 |

### 数据来源
- `compaction_job`：独立 Compaction 作业的 Paimon 桥接指标
- `wide_table`：Paimon 元数据采集器（轮询 $files/$snapshots）
- `cluster`：YARN/HDFS REST API 采集器

### 分析视图
```sql
-- Compaction 作业性能（繁忙度 / 平均耗时）
SELECT * FROM RDW_DATA.metrics_compaction_job
WHERE time_bucket_minute BETWEEN '<起>' AND '<止>'
ORDER BY time_bucket_minute;

-- 文件堆积与资源占用（文件数 / L0-L5 / YARN / HDFS）
SELECT * FROM RDW_DATA.metrics_resource_compaction
WHERE time_bucket_minute BETWEEN '<起>' AND '<止>'
ORDER BY time_bucket_minute;

-- Compaction 活跃度（COMPACT 提交占比）
SELECT * FROM RDW_DATA.metrics_update_delete_eff
WHERE time_bucket_minute BETWEEN '<起>' AND '<止>'
ORDER BY time_bucket_minute;

-- 健康软标志（L0 堆积 / Compaction 饱和 / 反压）
SELECT * FROM RDW_DATA.health_flags
WHERE l0_flag <> 'OK' OR compaction_flag <> 'OK' OR backpressure_flag <> 'OK'
ORDER BY time_bucket_minute;

-- 快照停滞 / 新鲜度超标告警
SELECT * FROM RDW_DATA.checkpoint_stall_alert
ORDER BY time_bucket_minute;
```

---

## 📖 场景 3：流式读取性能测试

### 测试目的
验证 Paimon **作为流式数据源**，被下游 Flink 流作业**全量+增量消费 changelog** 的性能表现（scan.mode = 当前快照全量 + 持续增量变更）。

**作业脚本**：`scripts/sql/07_streaming_read.sql`（blackhole sink，只测流读本身、不引入 sink 开销；**建议 job_name=`streaming_read_job`**）。

**前提条件**：被读表 `changelog-producer=input`（当前 `wide_table` 已满足）。

### 接入状态与步骤

⚠️ **当前状态**：分析视图已建（`analysis-sql/09_streaming_read.sql`，含 `metrics_streaming_read` / `metrics_read_vs_write`），`01_metrics_view.sql` 白名单已含 `streaming_read_job`。待办仅剩：提交作业 + 确认指标上报。

**接入步骤**（需开发协助）：
1. **提交流式读作业**：
   ```bash
   # 使用平台提交 scripts/sql/07_streaming_read.sql
   # 作业名设置为：streaming_read_job
   # 运行参数参考写入作业的 job-run-params.json（parallelism=3 等）
   ```

2. **确认指标已上报**：
   ```sql
   -- 查询原始表，确认 Source 算子指标已采集
   SELECT DISTINCT metric_name
   FROM RDW_ODS_FLINK_METRICS
   WHERE job_name = 'streaming_read_job'
     AND metric_name LIKE '%Source%'
     AND etl_dt = '<最新分区日期>';
   ```

3. **修改分析视图白名单**（✅ 已完成）：`01_metrics_view.sql` 的 `job_name IN (...)` 已含 `'streaming_read_job'`；若实际作业名不同，改该白名单即可。

4. **新建流式读分析视图**（✅ 已完成）：见 `analysis-sql/09_streaming_read.sql`，产出 `RDW_DATA.metrics_streaming_read`（流读吞吐 + Source 反压 + 软标志）与 `RDW_DATA.metrics_read_vs_write`（读写对照）。

### 临时查询方案（视图未建之前）

```sql
-- 直接查原始表（需手工做分钟分桶 + subtask 求和差分）
SELECT
    job_name,
    metric_name,
    CAST(metric_value AS DOUBLE) AS value,
    CAST(metric_ts AS BIGINT) AS ts,
    FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000) AS ts_readable
FROM RDW_ODS_FLINK_METRICS
WHERE job_name = 'streaming_read_job'
  AND metric_name LIKE '%Source%numRecordsOut%'
  AND etl_dt = '<分区日期>'
ORDER BY ts;
```

### 关注指标（优先级排序）

#### ⭐ 核心指标

| 指标名称 | metric_name（LIKE 匹配） | 观测目的 | 健康标准 |
|---------|------------------------|---------|---------|
| **流读吞吐（records/s）** | `%Source%numRecordsOut`<br>（累计值，复用 01 的分钟分桶 + subtask 求和差分） | 消费 changelog 的速率 | **良好**：稳定 ≥ 写入速率（不积压） |
| **Source 反压** | `%Source%backPressuredTimeMsPerSecond` | 读取侧是否成为瓶颈 | **良好**：< 100 ms/s（< 10%）<br>**告警**：持续 > 500 ms/s（> 50%）<br>⚠️ **若未上报**：观察 `busyTimeMsPerSecond`（接近 1000 = 满负荷） |
| **数据可见延迟** | `paimon.snapshot.id` / `paimon.snapshot.time.millis`<br>→ `checkpoint_health.commit_interval_sec`（视图已建） | 数据从写入到可被流读发现的间隔 | **良好**：≤ 3 分钟<br>**注意**：3-5 分钟<br>**告警**：> 5 分钟 |
| **快照停滞** | `checkpoint_health.snapshot_id_delta`（视图已建） | 流读"无新数据可消费"时的根因排查 | **正常**：不为 0<br>**异常**：为 0（即 `STALL`） |

#### 📊 辅助指标

| 指标名称 | metric_name | 观测目的 | 缺失时的替代方案 |
|---------|-------------|---------|---------------|
| Event-time 滞后 | `%currentFetchEventTimeLag` / `%currentEmitEventTimeLag` | 数据产生 → 被读取的事件时间差 | ⚠️ Paimon source 可能不暴露，若无可忽略 |
| Checkpoint 耗时 | `%lastCheckpointDuration` | 读作业自身 checkpoint 开销 | — |
| 累计读取量 | `%Source%numRecordsOut`（累计原值） | 与写入量对齐，核对消费进度 | — |

### 数据来源
- `streaming_read_job`（待接入）：`scripts/sql/07_streaming_read.sql` 提交后，指标经既有 Flink 链路上报（与写入作业同一管道，无需新采集器）
- `wide_table`：Paimon 元数据采集器（快照推进 → 可见延迟）

### 分析视图
- **已建**：`RDW_DATA.checkpoint_health` / `checkpoint_stall_alert`（可见延迟与停滞，见 `analysis-sql/08_checkpoint_health.sql`）
- **已建**：`RDW_DATA.metrics_streaming_read` / `metrics_read_vs_write`（流读吞吐 + 反压 + 读写对照，见 `analysis-sql/09_streaming_read.sql`）

---

## 📊 场景 4：流式聚合性能测试

### 测试目的
评估 Paimon **作为聚合计算数据源**，支撑流式持续聚合（running aggregate）的更新吞吐与状态开销。

**作业脚本**：`scripts/sql/08_streaming_agg.sql`（全局 COUNT/AVG/SUM/MAX，无 GROUP BY，print sink；**建议 job_name=`streaming_agg_job`**）。全局聚合维护单行 running 结果，随上游 changelog（+I/-U/+U/-D 回撤流）持续更新。

### 接入状态与步骤

⚠️ **当前状态**：流式聚合作业脚本已就绪，但配套分析视图尚未建立。

**接入步骤**（与场景 3 类似）：
1. **提交流式聚合作业**（job_name=`streaming_agg_job`）
2. **确认指标已上报**（查询原始表，确认 `GroupAggregate` 或 `GlobalGroupAggregate` 算子指标）
3. **修改分析视图白名单**（在 `01_metrics_view.sql` 加入 `streaming_agg_job`）
4. **新建流式聚合分析视图**（`analysis-sql/10_streaming_agg.sql`）

⚠️ **聚合算子名由 Flink SQL planner 生成**（形如 `GroupAggregate(...)` / `GlobalGroupAggregate(...)`），LIKE 匹配串**以运行作业 Web UI 的实际算子名为准**。

### 临时查询方案（视图未建之前）

```sql
-- 查询聚合算子指标
SELECT
    job_name,
    metric_name,
    CAST(metric_value AS DOUBLE) AS value,
    FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000) AS ts_readable
FROM RDW_ODS_FLINK_METRICS
WHERE job_name = 'streaming_agg_job'
  AND (metric_name LIKE '%Aggregate%numRecordsIn%' 
       OR metric_name LIKE '%Aggregate%numRecordsOut%'
       OR metric_name LIKE '%busyTimeMsPerSecond%'
       OR metric_name LIKE '%lastCheckpointFullSize%')
  AND etl_dt = '<分区日期>'
ORDER BY metric_ts;
```

### 关注指标（优先级排序）

#### ⭐ 核心指标

| 指标名称 | metric_name（LIKE 匹配） | 观测目的 | 健康标准 |
|---------|------------------------|---------|---------|
| **聚合输入吞吐（records/s）** | `%GroupAggregate%numRecordsIn`<br>`%GlobalGroupAggregate%numRecordsIn`<br>（累计值，分钟分桶 + subtask 求和差分） | 聚合消费 changelog 的速率 | **良好**：稳定 ≥ 写入速率（不积压） |
| **聚合输出（更新）频率** | `%GroupAggregate%numRecordsOut`<br>`%GlobalGroupAggregate%numRecordsOut` | 回撤/更新流的放大情况 | **预期**：与输入同量级（全局聚合逐条触发更新输出） |
| **算子繁忙度** | `%GroupAggregate%busyTimeMsPerSecond`<br>`%GlobalGroupAggregate%busyTimeMsPerSecond` | 聚合计算是否成为瓶颈 | **良好**：< 500 ms/s（< 50%）<br>**注意**：500-800 ms/s<br>**告警**：> 800 ms/s（> 80%）<br>⚠️ **若未上报**：观察 Source 反压推断 |
| **状态大小** | `%lastCheckpointFullSize`<br>（作业级 checkpoint 全量大小） | 聚合状态开销 | **预期**：全局聚合应为单行量级（KB 级别）；异常增长（MB 级别）= 状态退化 |

#### 📊 辅助指标

| 指标名称 | metric_name | 观测目的 | 缺失时的替代方案 |
|---------|-------------|---------|---------------|
| Checkpoint 耗时 | `%lastCheckpointDuration` | State Backend 快照开销 | — |
| Source 侧反压 | `%Source%backPressuredTimeMsPerSecond` | 区分瓶颈在读取侧还是聚合算子 | ⚠️ 若未上报：对比聚合算子繁忙度 |
| 数据可见延迟 | `checkpoint_health.commit_interval_sec`（视图已建） | 同场景 3 | — |

### 数据来源
- `streaming_agg_job`（待接入）：`scripts/sql/08_streaming_agg.sql` 提交后，指标经既有 Flink 链路上报

### 分析视图
- **待建**：`RDW_DATA.metrics_streaming_agg`（吞吐 + 繁忙度 + checkpoint），同样需先把 `streaming_agg_job` 加入 `01_metrics_view.sql` 白名单

---

## 🔗 场景 5：并发读写测试

### 测试目的
评估**写入 + Compaction + 流式读取**同时运行时的相互影响和资源争抢情况。

### 测试执行流程

**启动顺序**（避免相互干扰）：
1. **启动 Compaction 作业**：`bash scripts/sql/06_compaction_job.sh`，等待 1-2 分钟确认正常运行
2. **启动写入作业**：提交 `DataStreamperf_paimon`（03+05 SQL），等待 3-5 分钟让数据稳定流入
3. **启动流式读作业**（可选）：提交 `07_streaming_read.sql`（job_name=`streaming_read_job`）
4. **启动流式聚合作业**（可选）：提交 `08_streaming_agg.sql`（job_name=`streaming_agg_job`）

**观测周期**：
- **预热期**：前 10 分钟（写入从全量切换到增量，Compaction 积累触发）
- **稳定期**：10-60 分钟（核心观测窗口，各作业进入稳态）
- **长期验证**：阶段 2 需连续运行 5-7 天（验证 SLA 稳定性）

**判定标准**：
- ✅ **通过**：`health_flags` 全程 OK，或仅偶发（< 5% 时间）触发软标志
- ⚠️ **注意**：某维度持续告警（如 L0 堆积 > 30 分钟），但未影响核心指标
- ❌ **失败**：作业失败、checkpoint 失败、快照停滞 > 10 分钟、吞吐低于目标（阶段 2 < 20000 rps）

### 关注指标

#### 综合观测维度

| 维度 | 关注点 | 使用指标 |
|-----|-------|---------|
| **写入侧影响** | 写入吞吐是否下降、反压是否增加 | 场景 1 的核心指标 |
| **合并侧影响** | L0 堆积是否加剧、繁忙度是否接近 100% | 场景 2 的核心指标 |
| **读取侧影响** | 流读滞后是否增加、可见延迟是否超标 | 场景 3 的核心指标 |
| **资源争抢** | YARN CPU/内存是否饱和、HDFS I/O 是否瓶颈 | `cluster` 的所有资源指标 |

### 分析方法
```sql
-- 现成方案：health_flags 已按分钟对齐 写入/合并/资源/反压 四个维度
SELECT * FROM RDW_DATA.health_flags
WHERE time_bucket_minute BETWEEN '<起>' AND '<止>'
ORDER BY time_bucket_minute;

-- 需要自定义组合时，按 time_bucket_minute 对齐各视图：
SELECT
    w.time_bucket_minute,
    w.throughput_rps,                 -- 写入吞吐（场景 1）
    h.max_checkpoint_start_delay_ms,  -- 写入反压（场景 1）
    c.compaction_thread_busy_max,     -- 合并繁忙度（场景 2）
    c.avg_compaction_time_ms,         -- 合并耗时（场景 2）
    r.level0_file_count,              -- L0 堆积（场景 2）
    r.yarn_allocated_vcores,          -- 集群 CPU 占用
    r.yarn_allocated_memory_mb        -- 集群内存占用
FROM RDW_DATA.metrics_ingest_perf w
JOIN RDW_DATA.metrics_write_health h        ON h.time_bucket_minute = w.time_bucket_minute
JOIN RDW_DATA.metrics_compaction_job c      ON c.time_bucket_minute = w.time_bucket_minute
JOIN RDW_DATA.metrics_resource_compaction r ON r.time_bucket_minute = w.time_bucket_minute
ORDER BY w.time_bucket_minute;
-- 流读视图 metrics_streaming_read 待建后按同法 JOIN
```

---

## 📌 指标来源汇总

### 数据流向
```
metadata-collector（轮询 $files/$snapshots）──┐
resource-collector（YARN/HDFS REST）────────┤→ Kafka metrics topic
Flink 作业原生指标（metrics reporter）──────┘   → RDW_ODS_FLINK_METRICS(12列)
                                                  → RDW_DATA.* 分析视图
```

### job_name 说明

| job_name | 类别 | 产出的指标 |
|----------|------|----------|
| `DataStreamperf_paimon` | 写入作业（Flink Streaming，write-only） | 吞吐、反压、checkpoint |
| `compaction_job` | 合并作业（Paimon Action compact，流模式持续合并） | Compaction 繁忙度/耗时 |
| `wide_table` | Paimon 元数据采集器 | 文件数、Level 分布、快照、commit kind |
| `cluster` | YARN/HDFS 资源采集器 | CPU、内存、存储（集群级） |
| `streaming_read_job` / `streaming_agg_job`（待运行） | 下游消费作业（Flink Streaming） | 流读吞吐/反压、聚合开销 |

> ⚠️ **不要用 `app_id` 过滤**——它固定为 `paimon_table_mornit`，与业务无关，必须用 `job_name` 区分。

---

## ❌ 测试范围外说明

### 不测什么

1. **批式查询性能**（Flink Batch SELECT）：本测试专注流式场景
2. **秒级数据新鲜度**：Paimon 绑定 checkpoint（3min），不支持秒级实时
3. **维度表 Lookup Join**：当前未实现独立测试，仅作为功能验证项

### 数据可见新鲜度说明

- **采用快照提交间隔**近似 e2e 延迟（见 `08_checkpoint_health.sql`）
- Paimon 数据可见性绑定写作业 checkpoint（当前 3 分钟）
- **目标：≤ 5 分钟**（当前 ~3 分钟符合预期，无需优化）
- ⚠️ `LatencyProbe` 相关代码为占位，`ingest.e2e_latency_ms` 从不产出，可忽略

### 待接入项

- **流式读取**：分析侧已就绪（`01_metrics_view.sql` 白名单已含 `streaming_read_job`，视图见 `analysis-sql/09_streaming_read.sql`）；仅剩提交作业（`scripts/sql/07_streaming_read.sql`，job_name=`streaming_read_job`）并确认指标上报（口径见场景 3）。
- **流式聚合**：作业脚本已就绪（`scripts/sql/08_streaming_agg.sql`，建议 job_name=`streaming_agg_job`）。接入步骤同上，新建 `metrics_streaming_agg` 视图（口径见场景 4）。

---

## 📚 附录：指标采集技术细节

### Paimon 指标映射
采集逻辑见：`metadata-collector/src/main/java/com/paimonperf/metadata/MetadataMetricMapper.java`

### 数据类型转换
- 原始表 `RDW_ODS_FLINK_METRICS` 中 `metric_value` / `metric_ts` 为 `varchar`
- 分析视图中统一 `CAST` 为 `DOUBLE` / `BIGINT`

### 环境相关性
上述 `job_name`、Kafka topic、表名为当前测试环境配置，**换环境需同步调整分析视图的过滤白名单**。

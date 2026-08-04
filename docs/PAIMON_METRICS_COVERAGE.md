# Paimon 性能测试 · 指标地图（按测试场景分类）

> **测试目标**：评估 Flink + Paimon 主键表在**流式计算场景**（先全量后增量）的功能完整性与性能表现，覆盖：
> - 流式写入（高频 I/U/D）
> - 批式合并（Flink Batch Compaction）
> - 流式读取（全表 changelog 消费）
> - 流式聚合（维度聚合计算）
> - 维度表关联（Paimon 作为维度表）
>
> **数据新鲜度目标**：≤5 分钟（由 checkpoint 周期控制，当前配置 3 分钟）

---

## 📋 测试场景 → 指标映射速查表

| 测试场景 | 关注指标 | 数据来源（job_name） | 分析视图 |
|---------|---------|-------------------|---------|
| **场景 1：单写入性能** | 写入吞吐、反压、checkpoint 成功率 | `DataStreamperf_paimon` | `metrics_ingest_perf`<br>`metrics_write_health` |
| **场景 2：单合并性能** | Compaction 繁忙度/耗时、L0 堆积、资源占用 | `compaction_job`<br>`wide_table`<br>`cluster` | `metrics_compaction_job`<br>`metrics_resource_compaction`<br>`health_flags` |
| **场景 3：单查询性能（流式读）** | 流读吞吐、滞后时间、数据可见延迟 | 流式读作业（待接入）<br>`wide_table`（快照时间） | 待建视图<br>`checkpoint_health` |
| **场景 4：单聚合性能** | 聚合吞吐、状态大小、计算延迟 | 流式聚合作业（待接入） | 待建视图 |
| **场景 5：并发读写** | 写入吞吐波动、Compaction 相互影响、资源争抢 | 多作业组合观测 | 综合使用上述视图 |

---

## 🎯 场景 1：单写入性能测试

### 测试目的
验证 Paimon 主键表在**流式写入场景**（先全量后增量，高频 I/U/D）的吞吐能力、稳定性和 checkpoint 健康度。

### 关注指标（优先级排序）

#### ⭐ 核心指标

| 指标名称 | metric_name（LIKE 匹配） | 观测目的 | 健康标准 |
|---------|------------------------|---------|---------|
| **写入吞吐（records/min）** | `%ConstraintEnforcer%numRecordsOut` | 衡量写入能力是否达标，建立吞吐基线 | 达到设计目标（如 60K/min） |
| **反压指标** | `%checkpointStartDelayNanos` | 判断写入是否到上限（被 compaction/资源拖慢） | < 1 秒（低反压） |
| **Checkpoint 成功率** | `%numberOfCompletedCheckpoints`<br>`%numberOfFailedCheckpoints` | 写入稳定性的核心指标 | 成功率 > 99% |
| **快照推进速度** | `paimon.snapshot.id`<br>`paimon.snapshot.time.millis` | 数据提交是否正常、可见延迟 | 间隔 ≤ checkpoint 周期（3min） |

#### 📊 辅助指标

| 指标名称 | metric_name | 观测目的 |
|---------|-------------|---------|
| Checkpoint 耗时 | `%lastCheckpointDuration` | 判断 checkpoint 是否成为瓶颈 |
| Buffer 使用率 | `%buffers%usage` | 判断是否有内存压力 |
| 提交类型分布 | `paimon.last.commit.kind` | 区分 APPEND / COMPACT / OVERWRITE |

### 数据来源
- `DataStreamperf_paimon`：Flink 写入作业的原生指标
- `wide_table`：Paimon 元数据采集器（轮询 $snapshots 系统表）

### 分析视图
```sql
-- 写入吞吐与反压
SELECT * FROM RDW_DATA.metrics_ingest_perf 
WHERE metric_ts >= DATEADD(hour, -1, GETDATE());

-- 写入健康度（checkpoint 成功率、快照推进）
SELECT * FROM RDW_DATA.metrics_write_health
WHERE metric_ts >= DATEADD(hour, -1, GETDATE());
```

---

## ⚙️ 场景 2：单合并性能测试

### 测试目的
评估**独立 Compaction 作业**（Flink Batch 模式）在高频 I/U/D 场景下的合并能力、资源开销和文件管理效率。

### 关注指标（优先级排序）

#### ⭐ 核心指标

| 指标名称 | metric_name（LIKE 匹配） | 观测目的 | 健康标准 |
|---------|------------------------|---------|---------|
| **Compaction 繁忙度（%）** | `%compactionThreadBusy` | 合并线程是否满负荷 | < 80%（有余量）<br>≥ 95%（合不动，需加资源） |
| **L0 文件堆积** | `paimon.level.file.count.L0` | 最直接的合并滞后信号 | 稳定或缓慢增长<br>快速增长 = 合并跟不上 |
| **Compaction 平均耗时（ms）** | `%avgCompactionTime` | 单次合并的时间代价 | < 30 秒（参考值） |
| **Compaction 提交占比** | `paimon.last.commit.kind = 'COMPACT'` | 合并触发频率（U/D 越多占比越高） | 根据 U/D 比例合理即可 |

#### 📊 辅助指标

| 指标名称 | metric_name | 观测目的 |
|---------|-------------|---------|
| 总文件数 | `paimon.file.count` | 文件规模 = Compaction 压力总量 |
| L1-L5 文件分布 | `paimon.level.file.count.L1/L2/.../L5` | LSM Tree 层级健康度 |
| YARN CPU 占用 | `yarn.allocated.vcores` | Compaction 作业的计算资源消耗 |
| YARN 内存占用 | `yarn.allocated.memory.mb` | Compaction 作业的内存消耗 |
| HDFS 存储增长 | `hdfs.capacity.used.bytes` | 存储水位与增长速率 |

### 数据来源
- `compaction_job`：独立 Compaction 作业的 Paimon 桥接指标
- `wide_table`：Paimon 元数据采集器（轮询 $files/$snapshots）
- `cluster`：YARN/HDFS REST API 采集器

### 分析视图
```sql
-- Compaction 作业性能
SELECT * FROM RDW_DATA.metrics_compaction_job
WHERE metric_ts >= DATEADD(hour, -1, GETDATE());

-- 文件堆积与资源占用
SELECT * FROM RDW_DATA.metrics_resource_compaction
WHERE metric_ts >= DATEADD(hour, -1, GETDATE());

-- 健康告警（L0 堆积、快照停滞）
SELECT * FROM RDW_DATA.health_flags
WHERE flag_type IN ('l0_accumulation', 'snapshot_stall');
```

---

## 📖 场景 3：单查询性能测试（流式读取）

### 测试目的
验证 Paimon **作为流式数据源**，被下游 Flink 流作业消费 changelog 的性能表现（全表扫描 + 增量变更）。

### 关注指标（优先级排序）

#### ⭐ 核心指标

| 指标名称 | metric_name（LIKE 匹配） | 观测目的 | 健康标准 |
|---------|------------------------|---------|---------|
| **流读吞吐（records/s）** | `%Source%numRecordsOutPerSecond` | 读取 changelog 的速率 | 应 ≥ 写入速率 |
| **流读滞后时间（秒）** | `%Source%currentFetchEventTimeLag` | 读取延迟（数据产生 → 被读取） | < 5 分钟（目标） |
| **数据可见延迟** | `paimon.snapshot.time.millis`<br>（与当前时间差） | 最新快照距当前的时间差 | ≤ checkpoint 周期（3min） |
| **Source 算子反压** | `%Source%busyTimeMsPerSecond` | 判断读取侧是否成为瓶颈 | < 800ms（80% 以下） |

#### 📊 辅助指标

| 指标名称 | metric_name | 观测目的 |
|---------|-------------|---------|
| 读取记录数 | `%Source%numRecordsOut` | 累计读取量 |
| Checkpoint 对齐时间 | `%checkpointAlignmentTime` | 判断 barrier 对齐开销 |

### 数据来源
- 流式读作业（`07_streaming_read.sql`）：待接入 metrics reporter
- `wide_table`：快照时间戳（用于计算可见延迟）

### 分析视图
```sql
-- 待建视图（当前仅有 SQL 脚本）
-- 参考：analysis-sql/07_streaming_read.sql
```

---

## 📊 场景 4：单聚合性能测试（流式聚合）

### 测试目的
评估 Paimon **作为聚合计算数据源**，支持维度聚合（GROUP BY）的性能和状态管理能力。

### 关注指标（优先级排序）

#### ⭐ 核心指标

| 指标名称 | metric_name（LIKE 匹配） | 观测目的 | 健康标准 |
|---------|------------------------|---------|---------|
| **聚合吞吐（records/s）** | `%Aggregation%numRecordsOutPerSecond` | 聚合算子的输出速率 | 稳定且无积压 |
| **状态大小（MB）** | `%Aggregation%usedMemory` | 聚合状态的内存占用 | < 作业可用内存的 70% |
| **状态后端延迟** | `%checkpointDuration` | State Backend 写入开销 | < 30 秒 |
| **聚合反压** | `%Aggregation%busyTimeMsPerSecond` | 判断聚合计算是否成为瓶颈 | < 800ms（80% 以下） |

#### 📊 辅助指标

| 指标名称 | metric_name | 观测目的 |
|---------|-------------|---------|
| 输入记录数 | `%Aggregation%numRecordsIn` | 聚合算子的输入量 |
| Late Event 数量 | `%numLateRecordsDropped` | 迟到数据丢弃情况 |

### 数据来源
- 流式聚合作业（`08_streaming_agg.sql`）：待接入 metrics reporter

### 分析视图
```sql
-- 待建视图（当前仅有 SQL 脚本）
-- 参考：analysis-sql/08_streaming_agg.sql
```

---

## 🔗 场景 5：并发读写测试

### 测试目的
评估**写入 + Compaction + 流式读取**同时运行时的相互影响和资源争抢情况。

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
-- 时间对齐查询：同时观察三个作业的核心指标
SELECT 
    w.metric_ts,
    w.write_throughput,        -- 写入吞吐
    w.backpressure_delay,      -- 写入反压
    c.compaction_busy_pct,     -- 合并繁忙度
    c.l0_file_count,           -- L0 堆积
    r.read_lag_seconds,        -- 读取滞后
    y.cpu_usage_pct,           -- YARN CPU 使用率
    y.memory_usage_pct         -- YARN 内存使用率
FROM RDW_DATA.metrics_ingest_perf w
JOIN RDW_DATA.metrics_compaction_job c ON c.metric_ts = w.metric_ts
JOIN RDW_DATA.metrics_streaming_read r ON r.metric_ts = w.metric_ts  -- 待建
JOIN RDW_DATA.metrics_cluster_resource y ON y.metric_ts = w.metric_ts
WHERE w.metric_ts >= DATEADD(hour, -1, GETDATE())
ORDER BY w.metric_ts;
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
| `DataStreamperf_paimon` | 写入作业（Flink Streaming） | 吞吐、反压、checkpoint |
| `compaction_job` | 合并作业（Flink Batch） | Compaction 繁忙度/耗时 |
| `wide_table` | Paimon 元数据采集器 | 文件数、Level 分布、快照、commit kind |
| `cluster` | YARN/HDFS 资源采集器 | CPU、内存、存储 |
| 流式读/聚合作业 | 下游消费作业（Flink Streaming） | 流读吞吐/滞后、聚合开销 |

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

- **流式读取分析视图**：`07_streaming_read.sql` 作业运行后需建对应视图
- **流式聚合分析视图**：`08_streaming_agg.sql` 作业运行后需建对应视图

---

## 📚 附录：指标采集技术细节

### Paimon 指标映射
采集逻辑见：`metadata-collector/src/main/java/com/paimonperf/metadata/MetadataMetricMapper.java`

### 数据类型转换
- 原始表 `RDW_ODS_FLINK_METRICS` 中 `metric_value` / `metric_ts` 为 `varchar`
- 分析视图中统一 `CAST` 为 `DOUBLE` / `BIGINT`

### 环境相关性
上述 `job_name`、Kafka topic、表名为当前测试环境配置，**换环境需同步调整分析视图的过滤白名单**。

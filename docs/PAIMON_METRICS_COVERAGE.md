# Paimon 性能测试 · 指标地图

> 测的是 **Flink 读写 Paimon 主键宽表（高频 I/U/D）** 的性能与健康——把 Paimon 放进实时数据链路，作为**分钟级流式计算引擎**评估其表现。读写两侧并重：
> - **写入侧**：高频 I/U/D 入湖（write-only）+ 独立 compaction，看"写得快 vs 合得动"；
> - **读取侧**：下游流式作业消费 Paimon changelog 做流式计算——changelog 按 **checkpoint 周期（分钟级）**对下游可见（秒级变更传达成本过高），故 Paimon 支持的是分钟级、非秒级流式计算；看并发写入下的读性能与数据可见新鲜度（目标 ≤5min）；
> - 以及**并发读写的相互影响**。

本文档声明三件事：**需要哪几类指标 → 每类指标的来源 → 每个指标项的观测目的**。

## 来源类别

四个来源用 **`job_name`** 区分（**不要用 `app_id` 过滤**——它固定 `paimon_table_mornit`，与业务无关）：

```
metadata-collector（轮询 Paimon $files/$snapshots）─┐
resource-collector（YARN/HDFS REST）───────────────┤→ Kafka metrics topic → 既有链路
Flink 作业原生 / Paimon 桥接指标（metrics reporter）─┘   → RDW_ODS_FLINK_METRICS(12列) → RDW_DATA.* 分析视图
```

| 来源（job_name） | 类别 | 产出的指标 |
|---|---|---|
| `DataStreamperf_paimon` | 写入作业（write-only）Flink 原生指标 | 吞吐、反压 |
| `compaction_job` | 独立 compaction 作业的 Paimon 桥接指标 | Compaction 繁忙度/耗时 |
| `wide_table` | Paimon 元数据采集器（轮询系统表） | 文件数、Level 分布、快照、commit kind |
| `cluster` | YARN/HDFS 资源采集器（REST） | CPU、内存、存储 |
| 流式读/聚合作业 | 分钟级分析消费侧的 Flink 原生指标 | 流读吞吐/滞后、聚合开销 |

## 需要的四类指标（每项含观测目的）

### 类别1 · 写入性能 — 来源：`DataStreamperf_paimon`
**为什么需要**：探写入"多快、稳不稳"——吞吐是核心指标，反压反映是否已到写入上限。

| 指标项 | metric_name（LIKE 匹配） | 观测目的 |
|---|---|---|
| 写入吞吐 | `%ConstraintEnforcer%numRecordsOut` | 测写入能力、是否达目标 rps、建立吞吐基线（各 subtask 求和后分钟差分） |
| 反压 | `%checkpointStartDelayNanos` | 判断写入是否被下游（合并/资源）拖住、是否到达上限 |

→ 分析视图：`metrics_ingest_perf`、`metrics_write_health`

### 类别2 · 更新/删除效率 — 来源：`wide_table`
**为什么需要**：本测试核心是主键表高频 I/U/D，要看 update/delete 触发合并的程度、以及合并是否跟得上。

| 指标项 | metric_name | 观测目的 |
|---|---|---|
| Compaction 活跃度 | `paimon.last.commit.kind` | COMPACT 占比 = update/delete 触发合并的频繁程度 |
| L0 堆积 | `paimon.level.file.count.L0`（…L5） | L0 持续涨 = 合并跟不上写入（最直接的滞后信号） |

→ 分析视图：`metrics_update_delete_eff`、`metrics_resource_compaction`、`health_flags`

### 类别3 · 读取性能 — 来源：流式读/聚合作业（分钟级分析消费侧）
**为什么需要**：Paimon 作为**分钟级流式计算引擎**——其 changelog 按 checkpoint 周期（分钟级）对下游可见（秒级变更传达成本过高）。下游流式读/聚合消费该 changelog 即是业务目的；重点看并发写入下的读性能与数据可见新鲜度（≤5min）。

| 指标项 | 来源 | 观测目的 |
|---|---|---|
| 流读吞吐/滞后 | `07_streaming_read.sql` source 算子 | 并发写入下流式读 changelog 的速率与滞后 |
| 流式聚合开销 | `08_streaming_agg.sql`（全局 running 聚合，随 changelog 按分钟级刷新） | 持续聚合的状态规模与更新延迟 |

→ 分析视图：待接（作业运行后再加）

### 类别4 · 资源 & Compaction 开销 — 来源：`compaction_job` + `cluster` + `wide_table`
**为什么需要**：量化"合得动"的代价——合并耗多少资源、瓶颈在哪、存储怎么涨。

| 指标项 | job_name · metric_name | 观测目的 |
|---|---|---|
| Compaction 繁忙度 | `compaction_job` · `%compactionThreadBusy` | 合并线程是否满负荷（0~100，接近 100 = 合不动） |
| Compaction 耗时 | `compaction_job` · `%avgCompactionTime` | 单次合并的时间代价 |
| YARN CPU/内存 | `cluster` · `yarn.allocated.vcores` / `yarn.*.memory.mb` | 两个作业合计的计算资源占用 |
| HDFS 存储 | `cluster` · `hdfs.capacity.used.bytes` / `.total.bytes` | 存储增长与水位 |
| 文件总数 | `wide_table` · `paimon.file.count` | 文件规模 = Compaction 压力总量 |
| 快照推进 / 数据新鲜度 | `wide_table` · `paimon.snapshot.id` / `.time.millis` | 快照单调推进 + 提交间隔 = 数据可见新鲜度（目标 ≤5min；停滞=写不进/checkpoint 失败） |

→ 分析视图：`metrics_compaction_job`、`metrics_resource_compaction`、`checkpoint_health`、`health_flags`

> 采集映射见 `metadata-collector/src/main/java/com/paimonperf/metadata/MetadataMetricMapper.java`（此处不复制代码）。
> `metric_value` / `metric_ts` 在表里是 varchar，分析视图里 CAST 成 DOUBLE / BIGINT。

## 没测什么 / 怎么看新鲜度（诚实说明）

- **数据可见新鲜度**：用**快照提交间隔**近似（见 `08_checkpoint_health`，目标 ≤5min）。Paimon 数据可见性本就绑定写作业 checkpoint（当前 3min），~3min 可见 < 5min 目标，故 checkpoint 不必调、也不追求秒级；无需独立 e2e 延迟探针（`LatencyProbe` 占位、`ingest.e2e_latency_ms` 从不产出，可忽略）。
- **读取分析视图待接入**：读作业（`07`/`08`）运行后即有数据，需照写入侧同口径补上对应观测视图（当前只有脚本、未建分析视图）。
- **环境相关性**：上述 `job_name` / topic / 表名是当前这轮压测的值，换环境需同步调整分析视图的 `job_name` 过滤白名单。

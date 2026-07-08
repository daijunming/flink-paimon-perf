# analysis-sql（组件 f：StarRocks 分析 SQL）

StarRocks 分析 SQL 脚本集,针对既有指标表 `RDW_ODS_FLINK_METRICS`(采集器写入 + 既有 Flink 链路上报的指标统一落表)。非 Maven 模块,作为资源目录随包搬运、在 StarRocks 上执行。所有视图统一建在 **`RDW_DATA`** 库下。

> 若 `RDW_ODS_FLINK_METRICS` 不在 `RDW_DATA` 库,请把 `01_metrics_view.sql` 的 `FROM RDW_DATA.RDW_ODS_FLINK_METRICS` 库名限定改成实际库。

## 真实作业拓扑（2026-07-07 核对）

被测的是"Flink 写入 Paimon"的性能。写入作业为 **write-only**(只写不合并),Compaction 由**独立 paimon action 作业**完成。分析就围绕两件事:**写入作业状况** 与 **Paimon 表状况**,外加 **Compaction 作业开销** 和 **集群资源**。

相关指标来自四个 `job_name`(用 `job_name` 区分来源,不要用 `app_id` 过滤):

| 来源 | job_name | 说明 | metric_name 形态 |
|------|----------|------|------------------|
| 写入作业(write-only) | `DataStreamperf_paimon` | 纯写入,Flink 原生任务级指标,**按 subtask 分行** | `<算子名>.<subtask>.<短名>`,如 `...ConstraintEnforcer[4] -> Map.0.numRecordsOut`、`Writer(write-only) : wide_table.0.checkpointStartDelayNanos` |
| Compaction 作业 | `compaction_job` | 独立作业(普通 Flink 任务 + Paimon 桥接 Compaction Metrics) | Flink 标准指标 + `...compaction.compactionThreadBusy` / `...avgCompactionTime` 等 |
| Paimon 表元数据 | `wide_table` | 元数据采集器 | `paimon.file.count` / `paimon.level.*.L0..L5` / `paimon.snapshot.*` / `paimon.last.commit.kind` |
| 集群资源 | `cluster` | YARN/HDFS 采集器(打 `tags.table='cluster'`) | `yarn.*` / `hdfs.*` |

关键约定(也是旧版分析 SQL 的错误来源):

- **按 subtask 求和**:Flink 指标是任务级 `<算子>.<subtask>.<短名>`,并行度=3(subtask 0/1/2)。作业级总量 = 每个 subtask 桶内取累计最大值,再对各 subtask **求和**(不是直接 MAX)。
- **吞吐 anchor 用"包含"不是"前缀"**:算子链名以 `Source:` 开头,故用 `metric_name LIKE '%ConstraintEnforcer%numRecordsOut'`(旧版 `ConstraintEnforcer%` 前缀匹配不到)。
- **没有读作业**:Flink 流读 / 关联查询 Paimon 目前不存在,故不产出"读取性能"视图(不留空占位)。
- **没有真实端到端延迟**:延迟探针 `ingest.e2e_latency_ms` 从未产出(占位抛异常),故不做延迟 SLA 判定。
- `metric_value` / `metric_ts` 是 **varchar**,视图里 `CAST` 成 DOUBLE / BIGINT。

## 脚本清单

| 文件 | 内容 |
|------|------|
| `01_metrics_view.sql` | 基础视图:字段映射 + 分钟分桶,过滤到四个真实 job_name |
| `01_metrics_view_test.sql` | 分钟分桶逻辑验证(自包含临时表) |
| `02_four_category_metrics.sql` | 观测视图集(见下) |
| `05_health_flags.sql` | 可读健康标志:L0 堆积 / 反压 / 写速率 vs 合速率 / Compaction 活跃度 |
| `05_health_flags_test.sql` | 健康标志判定验证(自包含临时表) |
| `08_checkpoint_health.sql` | 快照推进健康度(snapshot_id 单调 / 停滞 STALL / 过慢 SLOW) |
| `08_checkpoint_health_test.sql` | 快照停滞检测验证(自包含临时表) |

`02_four_category_metrics.sql` 内含的视图:

| 视图 | 类别 | 数据源 |
|------|------|--------|
| `metrics_ingest_perf` | 1 写入性能 | 写入作业源链路 numRecordsOut(subtask 求和)→ `records_out_total` + `throughput_rps`(相邻桶差分/实际秒) |
| `metrics_write_health` | 1 写入健康 | 写入作业 `checkpointStartDelayNanos`(最大,纳秒→毫秒,反压信号) |
| `metrics_update_delete_eff` | 2 更新删除效率 | Paimon `paimon.last.commit.kind`(COMPACT 占比) |
| `metrics_resource_compaction` | 4 资源 + Paimon | `cluster` 的 YARN/HDFS + `wide_table` 的文件数 / Level L0–L5 |
| `metrics_compaction_job` | 4 Compaction 开销 | `compaction_job` 的 Paimon Compaction Metrics:`compactionThreadBusy`(0~100 繁忙度)、`avgCompactionTime`(ms) |

> 已移除:`03_sla_check`(延迟无数据源、无真实吞吐目标 → 砍掉)、`04_baseline_compare`(跨阶段对比,依赖三阶段方案,**先搁置**,待方案定后重建)。均可从 git 历史恢复。

## 执行方式

```sql
SOURCE 01_metrics_view.sql;              -- 基础视图(其余视图的底座)
SOURCE 02_four_category_metrics.sql;     -- 观测视图集
SOURCE 05_health_flags.sql;              -- 依赖 02
SOURCE 08_checkpoint_health.sql;         -- 依赖 01
```

测试 SQL(`_test.sql`)均为**自包含逻辑验证**:建临时表插固定样本、复现判定逻辑、断言、清理,不触碰真实分区表,本地即可执行。

```sql
SOURCE 01_metrics_view_test.sql;      -- 预期:分桶断言 PASS
SOURCE 05_health_flags_test.sql;      -- 预期:7 个用例全 PASS
SOURCE 08_checkpoint_health_test.sql; -- 预期:停滞检测断言 PASS
```

## 健康标志说明（05_health_flags）

只呈现"看得见的事实 + 可调阈值软标志",不替用户做武断的根因归因:

- `write_rps`:写入吞吐(写入作业)。
- `compaction_thread_busy_max`:Compaction 线程繁忙度(0~100),独立 compaction 作业的 Paimon 指标。
- `level0_file_count` / `paimon_file_count`:L0 堆积是"Compaction 是否跟得上"的直接证据。
- `max_checkpoint_start_delay_ms`:反压信号(barrier 迟迟到不了 task)。
- `compact_ratio`:COMPACT commit 占比,反映 Compaction 活跃度。
- 软标志:`l0_flag`(L0>1000 → `L0_PILEUP`)、`backpressure_flag`(>30000ms → `BACKPRESSURE`)、`compaction_flag`(繁忙>90 → `COMPACTION_SATURATED`)。阈值为可调起点,按实测基线调整。

> 设计取舍:判断"合不过来"看两个直接信号——**L0 堆积**(`l0_flag`)与 **Compaction 线程饱和**(`compaction_flag`),而不是"写速率−合速率"的减法(两者不是同一记录总体,单位不可直接相减)。write_rps 与 compaction_thread_busy 并排给出供人对照。快照推进/停滞另见 `08`。

## 依赖前置

- 既有表 `RDW_ODS_FLINK_METRICS`(12 列:etl_dt / metric_id / job_name / app_id / job_id / host_name / container_id / container_rule / metric_name / metric_type / metric_value / metric_ts)。
- 写入作业(`job_name='DataStreamperf_paimon'`)原生 metrics 已由既有链路上报到该表。
- 独立 Compaction 作业(`job_name='compaction_job'`,普通 Flink 任务)已上报 Paimon 桥接的 Compaction Metrics(`compactionThreadBusy` / `avgCompactionTime` 等),按短名后缀匹配。
- Paimon 元数据采集器(`job_name='wide_table'`)已运行。
- YARN/HDFS 资源采集器(`job_name='cluster'`)已运行(见 `resource-collector/DEVELOP.md`;注意其指标 `job_name='cluster'` 已纳入 01 白名单)。

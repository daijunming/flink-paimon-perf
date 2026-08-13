# analysis-sql（组件 f：StarRocks 分析 SQL）

StarRocks 分析 SQL 脚本集,数据源两类：既有指标表 `RDW_ODS_FLINK_METRICS`（Flink 作业指标经既有链路上报统一落表）与 meta-collect 的 Paimon 元数据 ODS 表（`rdw_ods_paimon_meta_*`，行级元数据通路）。非 Maven 模块,作为资源目录随包搬运、在 StarRocks 上执行。所有视图统一建在 **`RDW_DATA`** 库下。

> 若 `RDW_ODS_FLINK_METRICS` 不在 `RDW_DATA` 库,请把 `01_metrics_view.sql` 的 `FROM RDW_DATA.RDW_ODS_FLINK_METRICS` 库名限定改成实际库。

## 真实作业拓扑（2026-07-07 核对；streaming_read_job 后续接入）

被测的是"Flink 写入 Paimon"的性能。写入作业为 **write-only**(只写不合并),Compaction 由**独立 paimon action 作业**完成。分析围绕:**写入作业状况**、**Paimon 表状况**(表侧信号来自 meta-collect ODS)、**Compaction 作业开销**(仅历史),以及 **流式读作业状况**（读 Paimon changelog）。**集群资源**维度随 resource-collector 2026-08 退役,无替代通路。

相关指标来自五个 `job_name`(用 `job_name` 区分来源,不要用 `app_id` 过滤):

| 来源 | job_name | 说明 | metric_name 形态 |
|------|----------|------|------------------|
| 写入作业(write-only) | `DataStreamperf_paimon` | 纯写入,Flink 原生任务级指标,**按 subtask 分行** | `<算子名>.<subtask>.<短名>`,如 `...ConstraintEnforcer[4] -> Map.0.numRecordsOut`、`Writer(write-only) : wide_table.0.checkpointStartDelayNanos` |
| Compaction 作业 | `compaction_job` | 旧流式常驻形态(现行合并为 crontab 批任务 `paimon-compact`,几十秒退出、**无指标上报**) | Flink 标准指标 + `...compaction.compactionThreadBusy` / `...avgCompactionTime` 等(仅流式形态有数据) |
| Paimon 表元数据 | `wide_table` | 元数据采集器，**2026-08 现场退役、仅历史数据可查**（表侧信号改由 meta-collect ODS `rdw_ods_paimon_meta_*` 承载） | `paimon.file.count` / `paimon.level.*.L0..L5` / `paimon.snapshot.*` / `paimon.last.commit.kind` |
| 集群资源 | `cluster` | YARN/HDFS 采集器(打 `tags.table='cluster'`)，**2026-08 现场退役、仅历史数据可查，无替代通路** | `yarn.*` / `hdfs.*` |
| 流式读作业 | `streaming_read_job` | 流读 wide_table changelog（blackhole sink，只测流读） | 同为任务级按 subtask 分行：`Source: ...%numRecordsOut`、`%backPressuredTimeMsPerSecond` 等 |

关键约定(也是旧版分析 SQL 的错误来源):

- **按 subtask 求和 + 跨算子 MAX 去重**:Flink 指标是任务级 `<算子>.<subtask>.<短名>`,并行度=3(subtask 0/1/2)。同一指标会被任务级链名(如 `Source: kafka_source[3] -> ConstraintEnforcer[4] -> Map`)与算子级名(`ConstraintEnforcer[4]`)两种粒度重复上报、计数相同(2026-08-12 现场核实)。作业级总量 = 桶内 MAX → 单算子内跨 subtask **求和** → **跨算子 MAX 去重**(不分算子直接 SUM 会重复计数,吞吐虚高约 2 倍)。
- **吞吐 anchor 用"包含"不是"前缀"**:算子链名以 `Source:` 开头,故用 `metric_name LIKE '%ConstraintEnforcer%numRecordsOut'`(旧版 `ConstraintEnforcer%` 前缀匹配不到)。
- **上报周期 3 分钟**:Flink 作业指标（写入/compaction/流读）每 3 分钟上报一批,相关视图每 3 分钟一行有效数据;吞吐类一律用"相邻桶差分 ÷ 实际间隔秒"（不写死 60,兼容采样缺口）,算出的是窗口平均速率。meta-collect ODS 通路（表侧）约每 3 分钟一轮,落在轮次分钟上。
- **读取性能 = 流式读**:流读作业（`streaming_read_job`）分析见 `09_streaming_read.sql`;点查/批 OLAP 仍无作业,不出对应视图（不留空占位）。
- **没有真实端到端延迟**:延迟探针 `ingest.e2e_latency_ms` 从未产出(探针代码已移除),故不做延迟 SLA 判定;数据可见延迟用 `08_checkpoint_health.sql` 的 `commit_interval_sec` 近似。
- `metric_value` / `metric_ts` 是 **varchar**,视图里 `CAST` 成 DOUBLE / BIGINT。

## 脚本清单

| 文件 | 内容 |
|------|------|
| `01_metrics_view.sql` | 基础视图:字段映射 + 分钟分桶,过滤到五个真实 job_name |
| `01_metrics_view_test.sql` | 分钟分桶逻辑验证(自包含临时表) |
| `02_four_category_metrics.sql` | 观测视图集(见下) |
| `02_four_category_metrics_test.sql` | 写入吞吐聚合验证:双粒度重复上报样本下跨算子 MAX 去重不翻倍(自包含临时表) |
| `05_health_flags.sql` | 可读健康标志:L0 堆积 / 反压 / Compaction 活跃度(表侧列改接 meta-collect ODS) |
| `05_health_flags_test.sql` | 健康标志判定验证(自包含临时表) |
| `08_checkpoint_health.sql` | 快照推进健康度(按采集轮次的 latest snapshot / 停滞 STALL / 新鲜度超标 STALE,改接 ODS 快照表) |
| `08_checkpoint_health_test.sql` | 快照停滞检测验证(自包含临时表) |
| `09_streaming_read.sql` | 流式读性能:流读吞吐 / Source 反压 / 读写对照(消费是否跟得上写入) |
| `09_streaming_read_test.sql` | 流读差分 + 反压标志 + 读写对照判定验证(自包含临时表) |

`02_four_category_metrics.sql` 内含的视图:

| 视图 | 类别 | 数据源 |
|------|------|--------|
| `metrics_ingest_perf` | 1 写入性能 | 写入作业源链路 numRecordsOut(桶内MAX→单算子内subtask求和→跨算子MAX去重)→ `records_out_total` + `throughput_rps`(相邻桶差分/实际秒) |
| `metrics_write_health` | 1 写入健康 | 写入作业 `checkpointStartDelayNanos`(最大,纳秒→毫秒,反压信号) |
| `metrics_update_delete_eff` | 2 更新删除效率 | meta-collect ODS `rdw_ods_paimon_meta_snapshots` 的 `commit_kind`(COMPACT 占比;原 `paimon.last.commit.kind` 指标随采集器退役) |
| `metrics_compaction_job` | 4 Compaction 开销 | `compaction_job` 的 Paimon Compaction Metrics:`compactionThreadBusy`(0~100 繁忙度)、`avgCompactionTime`(ms),仅历史数据 |

> 已删除:`metrics_resource_compaction`(2026-08-11)——YARN/HDFS 信号随 resource-collector 退役无替代;
> Paimon 文件数/Level 由 meta-collect 的 `v_paimon_meta_level_stats` 覆盖,不再重复建视图。

> 已移除:`03_sla_check`(延迟无数据源、无真实吞吐目标 → 砍掉)、`04_baseline_compare`(跨阶段对比,依赖三阶段方案,**先搁置**,待方案定后重建)。均可从 git 历史恢复。

## 执行方式

```sql
SOURCE 01_metrics_view.sql;              -- 基础视图(其余视图的底座)
SOURCE 02_four_category_metrics.sql;     -- 观测视图集(metrics_update_delete_eff 依赖 meta-collect 的 rdw_ods_paimon_meta_snapshots)
SOURCE 05_health_flags.sql;              -- 依赖 02 + meta-collect 的 v_paimon_meta_level_stats
SOURCE 08_checkpoint_health.sql;         -- 依赖 meta-collect 的 rdw_ods_paimon_meta_snapshots
SOURCE 09_streaming_read.sql;            -- 依赖 01、02(metrics_ingest_perf)
```

> 前置:02/05/08 的表侧信号来自 meta-collect ODS,需先部署 `scripts/meta-collect/sr/` 的
> `01_ods_tables.sql`(建 `rdw_ods_paimon_meta_*`)与 `02_analysis_views.sql`(建
> `v_paimon_meta_level_stats` 等),采集链路部署见 `scripts/meta-collect/README.md`。

测试 SQL(`_test.sql`)均为**自包含逻辑验证**:建临时表插固定样本、复现判定逻辑、断言、清理,不触碰真实分区表,本地即可执行。

```sql
SOURCE 01_metrics_view_test.sql;      -- 预期:分桶断言 PASS
SOURCE 02_four_category_metrics_test.sql; -- 预期:断言0/断言1 均 PASS
SOURCE 05_health_flags_test.sql;      -- 预期:断言1/断言2 均 PASS
SOURCE 08_checkpoint_health_test.sql; -- 预期:推进/停滞/超标断言 PASS
SOURCE 09_streaming_read_test.sql;    -- 预期:3 个断言全 PASS
```

## 健康标志说明（05_health_flags）

只呈现"看得见的事实 + 可调阈值软标志",不替用户做武断的根因归因:

- `write_rps`:写入吞吐(写入作业)。
- `level0_file_count` / `paimon_file_count`:L0 堆积是"Compaction 是否跟得上"的直接证据;来自 meta-collect 的 `v_paimon_meta_level_stats`,只在采集轮次分钟(约每 3 分钟)有值,中间分钟 NULL 属正常。
- `max_checkpoint_start_delay_ms`:反压信号(barrier 迟迟到不了 task)。
- `compact_ratio`:COMPACT commit 占比(来自 ODS 快照表),反映 Compaction 活跃度。
- 软标志:`l0_flag`(L0>1000 → `L0_PILEUP`)、`backpressure_flag`(>30000ms → `BACKPRESSURE`)。阈值为可调起点,按实测基线调整。
- 已移除:`compaction_thread_busy_max` / `avg_compaction_time_ms` / `compaction_flag` —— 输入 `metrics_compaction_job` 在 crontab 批任务形态下本就无数据,永久 NULL 有误导性;compaction 作业侧看 cron 日志与 YARN 应用历史。

> 设计取舍:判断"合不过来"看表侧直接证据——**L0 堆积**(`l0_flag`)叠加 **COMPACT 提交不活跃**(`compact_ratio` 持续偏低),而不是"写速率−合速率"的减法(两者不是同一记录总体,单位不可直接相减)。快照推进/停滞另见 `08`。

## 流式读说明（09_streaming_read）

- `metrics_streaming_read`:流读吞吐(`read_rps`,与写入同口径:单算子内 subtask 求和 + 跨算子 MAX 去重后差分) + Source 反压(`backPressuredTimeMsPerSecond`,最大/平均) + 繁忙度(`busyTimeMsPerSecond`,反压未上报时的替代),软标志 `read_backpressure_flag`(>500 `READ_BACKPRESSURE` / >100 `ELEVATED`,阈值来自指标地图场景3,可调)。
- `metrics_read_vs_write`:以写入吞吐为基准的读写对照,`unconsumed_records`(写累计−读累计)趋势扩大 = 消费滞后累积;`consume_status` 四态(`KEEPING_UP`/`LAGGING`/`NO_READ_DATA`/`NO_WRITE_BASELINE`)。
- 读作业 `scan.mode` 默认(当前快照全量+增量),启动初期 `read_rps` 因追全量冲高,判读时排除追数据阶段。
- 数据可见延迟/快照停滞不在 09,复用 `08_checkpoint_health`。

## 依赖前置

- 既有表 `RDW_ODS_FLINK_METRICS`(12 列:etl_dt / metric_id / job_name / app_id / job_id / host_name / container_id / container_rule / metric_name / metric_type / metric_value / metric_ts)。
- 写入作业(`job_name='DataStreamperf_paimon'`)原生 metrics 已由既有链路上报到该表(每 3 分钟一批)。
- 独立 Compaction 作业:现行形态为 crontab 批任务 `paimon-compact`(每 5 分钟一轮、几十秒退出),生命周期短于 3 分钟上报周期,**`metrics_compaction_job` 无数据属预期(查不到 ≠ 没跑)**;仅切回流式常驻 `compaction_job` 时,Paimon 桥接指标(`compactionThreadBusy` / `avgCompactionTime`,按短名后缀匹配)才有数据。合并效果分析口径见 `../docs/写入与合并性能分析.md`。
- 流式读作业(`job_name='streaming_read_job'`,`scripts/sql/07_streaming_read.sql` 提交)指标经同一既有链路上报;若实际作业名不同,改 `01_metrics_view.sql` 白名单。
- meta-collect ODS 已部署:`rdw_ods_paimon_meta_*` 表与 `v_paimon_meta_level_stats` 视图存在且有新数据(部署见 `../scripts/meta-collect/README.md`);02/05/08 的表侧信号依赖它。
- ~~Paimon 元数据采集器(`job_name='wide_table'`)~~ 与 ~~YARN/HDFS 资源采集器(`job_name='cluster'`)~~ 均已于 2026-08 现场退役,无需再启动;两个 job_name 保留在 01 白名单仅为可查历史数据。

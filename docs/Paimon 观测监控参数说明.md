# Paimon 观测监控参数说明

## 1. 文档目标

本文档定义本轮 PoC 中 Paimon 的观测监控参数、采集来源、阶段标签、告警方向和执行边界。

本文档不重新定义测试方案。测试目标、业务数据模型、TPS 分布、通过标准和结论规则以 `docs/Paimon 与 Iceberg 测试方案设计说明.md` 为准。

## 2. 观测目标

Paimon 观测重点不是只看作业是否存活，而是回答以下问题：

1. 主键表写入是否持续提交成功
2. `changelog-producer` 相关链路是否持续产出下游可消费变更
3. LSM compaction 是否跟得上高频 upsert/delete
4. 写缓冲是否导致频繁 flush 和小文件
5. 下游流读是否产生 snapshot 消费滞后
6. 表级 snapshot、文件、manifest 与 metadata 趋势是否收敛

Paimon 的头号风险是 compaction 跟不上后带来的 L0 文件堆积、读放大和延迟抖动；这与 Iceberg 更偏重 commit 存活、delete file 和 manifest 膨胀的风险不同。

## 3. 指标来源

| 来源 | 用途 | 采集方式 | 结果出口 |
|---|---|---|---|
| Paimon 原生 Flink 指标 | commit、compaction、write buffer、scan | Flink Web UI、Flink REST API 或平台 MetricReporter | 运行态指标系统 |
| Flink 连接器标准指标 | source/sink 吞吐、反压、事件时间滞后 | Flink Web UI、Flink REST API 或平台 MetricReporter | 运行态指标系统 |
| Paimon 系统表 / metadata | snapshot、consumer、files、partitions、tags | Spark、Flink、平台任务或轻量 adapter 单次采样 | metrics Kafka，最终落 StarRocks `rtp_observe.storage_metrics_snapshot` 或等价分析表 |

本仓库口径固定为：业务 Kafka 只用于主测试输入、消费 lag 和探针链路；表侧 metadata 指标不写业务 topic，应进入平台 metrics Kafka，最终落 StarRocks 做即席分析。

## 4. 统一标签

所有 Paimon 指标至少保留以下标签：

| 标签 | 说明 |
|---|---|
| `engine` | 固定为 `paimon` |
| `database_name` | Paimon catalog 内数据库名 |
| `table_name` | 被观测业务表名 |
| `stage_label` | `warmup`、`baseline`、`peak`、`valley`、`hotspot`、`replay`、`recover` 等阶段 |
| `collector_run_id` | 单次 metadata 采样唯一编号 |
| `job_name` | Flink 作业名 |
| `yarn_app_id` | YARN application id |
| `partition` | 分区级指标可选 |
| `bucket` | bucket 级 compaction 指标可选 |

## 5. 运行态参数

### 5.1 Commit

| 参数 | 类型 | 观测含义 | PoC 用途 |
|---|---|---|---|
| `lastCommitDuration` | Gauge | 最近一次 commit 耗时 | 判断提交是否变慢 |
| `commitDuration` | Histogram | commit 耗时分布 | 看 P95/P99 提交抖动 |
| `lastCommitAttempts` | Gauge | 最近一次提交尝试次数 | `>1` 表示提交冲突或重试 |
| `lastGeneratedSnapshots` | Gauge | 最近一次提交生成的 snapshot 数 | 判断提交形态是否异常 |
| `lastTableFilesAdded` | Gauge | 最近一次新增 data file 数 | 判断小文件生成速度 |
| `lastTableFilesDeleted` | Gauge | 最近一次删除 data file 数 | 判断 compaction 或 delete 影响 |
| `lastTableFilesAppended` | Gauge | 最近一次 append 文件数 | 判断 checkpoint 内文件产出 |
| `lastTableFilesCommitCompacted` | Gauge | 最近一次 compaction 产出文件数 | 判断 compaction 是否参与提交 |
| `lastChangelogFilesAppended` | Gauge | 最近一次追加 changelog 文件数 | 判断下游变更来源 |
| `lastChangelogFilesCommitCompacted` | Gauge | 最近一次 compaction changelog 文件数 | 判断 changelog compaction 影响 |
| `lastDeltaRecordsAppended` | Gauge | 最近一次追加 delta 记录数 | 对齐写入量 |
| `lastChangelogRecordsAppended` | Gauge | 最近一次追加 changelog 记录数 | 判断变更量 |
| `lastPartitionsWritten` | Gauge | 最近一次写入分区数 | 判断分区分散度 |
| `lastBucketsWritten` | Gauge | 最近一次写入 bucket 数 | 判断 bucket 分散度 |

### 5.2 Compaction

| 参数 | 类型 | 观测含义 | PoC 用途 |
|---|---|---|---|
| `maxLevel0FileCount` | Gauge | L0 文件最大堆积数 | Paimon 小文件/读放大核心指标 |
| `avgLevel0FileCount` | Gauge | L0 文件平均堆积数 | 判断整体 compaction 压力 |
| `compactionThreadBusy` | Gauge | compaction 线程繁忙度 | 接近 100 且 L0 上升表示压不住 |
| `avgCompactionTime` | Gauge | compaction 平均耗时 | 判断 compaction 是否变慢 |
| `compactionQueuedCount` | Counter | 排队 compaction 数 | 判断积压 |
| `compactionCompletedCount` | Counter | 已完成 compaction 数 | 判断处理能力 |
| `maxCompactionInputSize` | Gauge | 最大 compaction 输入大小 | 判断单次合并成本 |
| `avgCompactionInputSize` | Gauge | 平均 compaction 输入大小 | 判断合并规模 |
| `maxCompactionOutputSize` | Gauge | 最大 compaction 输出大小 | 判断输出文件形态 |
| `avgCompactionOutputSize` | Gauge | 平均 compaction 输出大小 | 判断输出文件是否过小 |
| `maxTotalFileSize` | Gauge | 单 bucket 文件总大小最大值 | 判断热点 bucket |
| `avgTotalFileSize` | Gauge | 单 bucket 文件总大小平均值 | 判断全局文件规模 |

### 5.3 Write Buffer

| 参数 | 类型 | 观测含义 | PoC 用途 |
|---|---|---|---|
| `numWriters` | Gauge | 当前并行度 writer 数 | 判断分区/bucket 基数压力 |
| `usedWriteBufferSizeByte` | Gauge | 已用写缓冲字节数 | 判断缓冲是否吃紧 |
| `totalWriteBufferSizeByte` | Gauge | 总写缓冲字节数 | 计算缓冲使用率 |
| `bufferPreemptCount` | Gauge | 写缓冲抢占次数 | 抢占频繁表示内存不足或 flush 频繁 |

### 5.4 Scan

| 参数 | 类型 | 观测含义 | PoC 用途 |
|---|---|---|---|
| `lastScanDuration` | Gauge | 最近一次 scan 规划耗时 | 判断读规划是否变慢 |
| `scanDuration` | Histogram | scan 耗时分布 | 看规划耗时抖动 |
| `lastScannedManifests` | Gauge | 最近一次扫描 manifest 数 | 判断 metadata 读取压力 |
| `lastScanSkippedTableFiles` | Gauge | 最近一次跳过文件数 | 判断裁剪是否生效 |
| `lastScanResultedTableFiles` | Gauge | 最近一次命中文件数 | 判断实际扫描量 |

### 5.5 Flink 标准指标

| 参数 | 来源 | 观测含义 |
|---|---|---|
| `numRecordsInPerSecond` | Flink operator | 输入吞吐 |
| `numRecordsOutPerSecond` | Flink operator | 输出吞吐 |
| `busyTimeMsPerSecond` | Flink operator | 算子繁忙度 |
| `backPressuredTimeMsPerSecond` | Flink operator | 反压时间 |
| `currentEmitEventTimeLag` | Paimon source | 输出事件时间滞后 |
| `currentFetchEventTimeLag` | Paimon source | 读取事件时间滞后 |

## 6. 表级 metadata 参数

表侧 metadata 采集执行器每分钟或按阶段采样一次，标准化为 metrics 事件进入平台 metrics Kafka，最终落入 `rtp_observe.storage_metrics_snapshot` 或等价 StarRocks 分析表。最小字段如下：

| StarRocks 字段 | Paimon 采样来源 | 含义 |
|---|---|---|
| `snapshot_count` | `$snapshots` | 当前可见 snapshot 数 |
| `latest_commit_time` | `$snapshots.commit_time` 最大值 | 最近一次表级提交时间 |
| `data_file_count` | `$files` | 当前可见 data file 数 |
| `avg_data_file_bytes` | `$files.file_size_in_bytes` 平均值 | 平均 data file 大小 |
| `last_file_creation_time` | `$files.creation_time` 最大值 | 最近文件创建时间 |
| `manifest_count` | `$manifests` | manifest 数 |
| `manifest_bytes` | `$manifests.file_size` 汇总 | manifest 总字节数 |
| `metadata_source` | collector 固定值 | `metadata_table`、`native_api` 等 |

建议扩展采样但不强制进入最小 StarRocks 表的参数：

| 参数 | 来源 | 用途 |
|---|---|---|
| latest snapshot 距今秒数 | `$snapshots` | 表侧提交存活 |
| consumer next snapshot 落后数 | `$consumers` | 下游流读滞后与 snapshot 钉住风险 |
| 分区文件数分布 | `$partitions` / `$files` | 分区倾斜 |
| bucket 文件数分布 | `$files` | bucket 热点 |
| tag 数与保留时间 | `$tags` | snapshot 过期是否被 tag 阻断 |

## 7. 告警与判定方向

| 优先级 | 告警项 | 参数 | 建议规则 |
|---|---|---|---|
| P0 | compaction 压不住 | `maxLevel0FileCount` + `compactionThreadBusy` | L0 持续走高且 busy 接近 100 |
| P0 | 表级提交中断 | `latest_commit_time` 距今 | 超过预期 checkpoint 周期多个倍数 |
| P1 | 提交冲突 | `lastCommitAttempts` | 持续 `>1` |
| P1 | 提交变慢 | `lastCommitDuration` / `commitDuration` | 相对基线突增 |
| P1 | 写缓冲吃紧 | `usedWriteBufferSizeByte / totalWriteBufferSizeByte`、`bufferPreemptCount` | 使用率持续高或抢占频繁 |
| P1 | 下游消费滞后 | `currentEmitEventTimeLag`、`$consumers` 落后数 | 超过分钟级新鲜度目标 |
| P2 | snapshot 膨胀 | `snapshot_count`、consumer 落后数 | 持续增长且不能过期 |
| P2 | 小文件 | `data_file_count`、`avg_data_file_bytes`、L0 文件数 | 文件数持续增长且平均文件过小 |
| P2 | 读裁剪失效 | `lastScanSkippedTableFiles / lastScanResultedTableFiles` | 跳过比例持续偏低 |

## 8. 执行要求

1. 运行态指标按 Flink job 维度采集，必须带 `job_name` 和 `yarn_app_id`。
2. 表级 metadata 指标按 `engine + database_name + table_name + stage_label + collector_run_id` 采集。
3. `peak` 与 `hotspot` 阶段必须额外记录 compaction、write buffer 与 checkpoint 详情。
4. `recover` 阶段必须记录从作业恢复到 lag 清零、对账恢复一致的时间。
5. Paimon 指标不可直接与 Iceberg 同名指标做数值等价比较，只能按风险轴对齐比较。

## 9. 表侧 metadata 指标采集映射边界

Paimon metadata 指标采集参数必须保留 Paimon 原生 catalog 口径：

1. `--paimon-warehouse` 对应 Paimon catalog 的 `warehouse`。
2. `--paimon-database` 对应 Paimon catalog 内数据库名。
3. `--table-names` 中的业务表名由采集执行器在 Paimon database 下逐表采样。
4. `--stage-label` 与 `--collector-run-id` 或平台等价 run id 必须写入 metrics 事件，并最终进入 StarRocks 分析表。

不允许事项：

1. 不允许把 Paimon 与 Iceberg warehouse 合并成一个通用 `warehouse` 参数。
2. 不允许把业务 Kafka `bootstrap.servers`、topic 或 consumer group 带入 Paimon metadata 指标事件。
3. 不允许把完整 Paimon catalog 配置、HDFS 地址或 metrics Kafka 连接串写入采样元信息。

## 10. 待验证项

1. 当前 Paimon 版本实际暴露的 metric scope 与指标名。
2. `$consumers`、`$partitions`、`$tags` 在目标版本和 catalog 下的可用性。
3. Flink REST API 是否能稳定拉取 Paimon source/sink 自定义指标。
4. Spark、Flink、平台任务或轻量 adapter 是否能在不依赖 Flink SQL 流式 planner 的情况下读取 Paimon metadata。

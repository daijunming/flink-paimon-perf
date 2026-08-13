# Paimon 读、写、合并任务配置基线

本文记录当前现场已确认配置。参数效果仍以目标 CDH 集群的运行结果为准，不把后续调优候选写成现行配置。

## 一、任务拓扑

```text
DataStreamperf_paimon（STREAMING，Writer 只写不合）
        ↓
wide_table（512 Bucket，deduplicate + lookup）
        ↓
paimon-compact（crontab 每 5 分钟提交 BATCH Minor Compact）
        ↓
streaming_read_job / streaming_agg_job（可选流式读）
```

## 二、表配置

```sql
WITH (
    'bucket' = '512',
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'event_time',
    'changelog-producer' = 'lookup',
    'changelog-producer.row-deduplicate' = 'true',
    'write-only' = 'true',
    'snapshot.num-retained.min' = '10'
)
```

- `bucket=512`：按 2 天、2000 条/s 与现场真实 Data Files 规模估算，单 Bucket 约 1GB。Bucket 数与写入并行度分别按存储规模和作业资源确定，不要求相等。
- `deduplicate + sequence.field=event_time`：同一主键保留 `event_time` 较新的整行；现场必须保证同 PK 的 `event_time` 能表达可靠先后顺序。
- `lookup + row-deduplicate=true`：Compaction 负责生成完整更新前后 Changelog；值未变化时不生成无意义的 `-U/+U`。
- `write-only=true`：Writer 跳过 Compaction 与 Snapshot Expiration，合并交给独立 Compact Action。
- `snapshot.num-retained.min=10`：只规定最少保留数。独立 Compact 是否持续触发清理、读任务需要多长恢复窗口，仍需在现场核实后确定独立清理策略。

## 三、写入任务

```sql
INSERT INTO paimon_obs.paimon_database.wide_table
/*+ OPTIONS(
    'sink.parallelism' = '3',
    'write-buffer-spillable' = 'true',
    'write-buffer-size' = '64 mb',
    'sink.use-managed-memory-allocator' = 'true'
) */
SELECT ...
```

`write-only` 已持久化为表级语义，不在 INSERT Hint 中重复声明。`num-sorted-run.compaction-trigger`、`parquet.enable.dictionary` 和 `read.batch-size` 不属于当前写入任务基线。

写入作业其余 Flink 参数由 `scripts/sql/job-run-params.json` 承载：当前 `parallelism.default=3`、Checkpoint 间隔 3 分钟。

## 四、独立 Compact Action

```bash
-Dexecution.runtime-mode=BATCH
-Dexecution.attached=true
-Djobmanager.memory.process.size=2048m
-Dtaskmanager.memory.process.size=15360m
-Dtaskmanager.memory.managed.fraction=0.1
-Dyarn.appmaster.vcores=1
-Dyarn.containers.vcores=6
-Dtaskmanager.numberOfTaskSlots=3

compact \
  --warehouse "${WAREHOUSE}" \
  --database "${DATABASE}" \
  --table "${TABLE}" \
  --compact_strategy minor \
  --table-conf changelog-producer=lookup \
  --table-conf changelog-producer.row-deduplicate=true \
  --table-conf lookup-compact=radical \
  --table-conf num-sorted-run.compaction-trigger=5 \
  --table-conf scan.split-enumerator.batch-size=1 \
  --table-conf sink.use-managed-memory-allocator=true \
  --table-conf sink.parallelism=3 \
  --table-conf parquet.enable.dictionary=false \
  --table-conf read.batch-size=512
```

当前依据：

- TM 15GB 已实测解决原来的 GC overhead。
- `managed.fraction=0.1` 在当前 Heap 压力明显、Managed Memory 实际使用较低的条件下采用；仍需同步观察 Spill 与本地磁盘。
- `scan.split-enumerator.batch-size=1` 已实测解决 AddSplitEvents 过大。
- `parquet.enable.dictionary=false` 与 `read.batch-size=512` 用于控制宽字符串表 Compact 的写入和读取内存。
- `sink.parallelism=3` 是当前可运行基线，不代表最终最优值。
- `lookup-compact=radical` 明确采用 Lookup 的激进合并策略：每次触发 Compaction 时推动 L0 向高层合并。
- `num-sorted-run.compaction-trigger=5` 保留 Paimon 1.1 默认值，控制 Universal Compaction 何时选择更大范围的 Sorted Run；在取得 Minor 实测数据前不沿用原来的激进值 3。

## 五、流式读任务

当前仓库只提供可选的读性能脚本：

- `07_streaming_read.sql`：默认从当前快照全量读取并持续消费后续 Changelog，写入 Blackhole。
- `08_streaming_agg.sql`：对完整 Changelog 做持续全表聚合。

读任务尚未形成生产恢复基线；`consumer-id`、`consumer.expiration-time`、明确的 `scan.mode` 与允许的最大停机恢复窗口仍是待确认项。本阶段不擅自补入运行配置。

## 六、现场验收

1. 对同一 PK 写入确定的 `I → U → 相同值 U → U → D` 样本。
2. 运行一次 `paimon-compact`，确认 YARN 状态成功且脚本退出码为 0。
3. 在 `$snapshots` 核对 `COMPACT` 提交和 `changelog_record_count`。
4. 用流读任务核对实际 RowKind，确认相同值更新被过滤，普通更新具备旧值和新值，删除语义正确。
5. 在 `$files` 核对 Minor 前后的 L0 数量、最老 L0 文件年龄、文件大小分布和每 Bucket 有效体积；Minor 不要求每轮把 L0 清零，但多轮后应收敛。
6. 观察 Snapshot 数量与磁盘占用是否持续回落；未回落时再部署独立 Snapshot/Changelog 清理任务。

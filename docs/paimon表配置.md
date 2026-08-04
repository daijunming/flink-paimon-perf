围绕一张paimon表，有三个flink任务，flink 写入，flink action 压缩，flink 读取，写入和读取，都要求流式的进行，压缩按批模式进行



流式 Writer 持续写入 Paimon
        ↓
Paimon 暂存最新增量，暂不向 Reader 输出完整变更流
        ↓
定时启动 BATCH + MINOR Compaction
        ↓
Compaction 根据压缩前后的主键状态生成一次规范化 Changelog
        ↓
流式 Reader 收到 -U/+U/-D/+I，更新指标



这三个作业应按如下配置paimon表的相关属性：

这个架构是标准的读写合并分离模式。配置分四层：表级属性、写作业、压缩作业、读作业。

核心原则是表级属性放共性配置，
作业级用 dynamic options（`/*+ OPTIONS(...) */` 或 action 的 `--table_conf`）覆盖，避免互相干扰。


## 一、表级属性（建表时或 ALTER TABLE 设置）

```sql
CREATE TABLE t (
    ...
    PRIMARY KEY (...) NOT ENFORCED
) WITH (
    'bucket' = '256',                          -- 按 586G 的量级
    'changelog-producer' = 'lookup',           -- 关键项，下面解释
    'snapshot.time-retained' = '2h',           -- 快照保留，需覆盖读作业可能的追赶延迟
    'snapshot.num-retained.min' = '10',
    'file.format' = 'parquet',
    'file.compression' = 'zstd'
);
```

关键决策点：

**`changelog-producer`**：流读作业如果需要完整的 changelog（含 UPDATE_BEFORE，比如下游做聚合），必须设置。可选值的权衡：

- `lookup`：写入时通过 lookup 生成 changelog，延迟低，但写入端有额外开销。注意 lookup 模式对 compaction 有隐含要求，写作业即使 write-only 也需要 lookup 相关内存。
- `full-compaction`：changelog 由 full compaction 产生。但你的压缩是批模式定时跑，changelog 时效性等于压缩周期，流读延迟会变成小时级。与你的流式读取需求冲突，除非下游能接受。
- `input`：仅当上游写入本身是完整 CDC 流（如 canal/debezium 直灌，无部分更新）时可用，开销最小。
- 不设（none）：流读端自行 normalize，读作业需要大状态节点还原 before 值，代价转移到读侧。

如果上游是 CDC 完整流，选 `input`；否则选 `lookup`。避开 `full-compaction`，它和你的批式压缩节奏绑定后会牺牲读延迟。

**`snapshot.time-retained`**：三个作业共享快照生命周期。流读断流恢复时如果消费的 snapshot 已过期，作业失败。保留时长必须大于读作业最大可能的中断加追赶时间。同时批式压缩产生的大 snapshot 也占存储，不宜留太长，2 小时到 1 天之间按运维能力取。

## 二、写作业（流式，只写不压）

```sql
INSERT INTO t /*+ OPTIONS(
    'write-only' = 'true',
    'write-buffer-size' = '512mb',
    'sink.parallelism' = '...'
) */ SELECT ...
```

- `write-only = true` 是这个架构的核心开关：writer 不做任何 compaction，也不触发 snapshot 过期清理和 `num-sorted-run.stop-trigger` 的写停等待，写入吞吐稳定。
- 用 dynamic option 而非表属性设置 `write-only`，因为它只对写作业成立，设成表属性会让压缩作业也读到（压缩 action 会忽略它，但语义上放作业级更干净）。
- 风险点：write-only 后 L0 文件持续堆积，sorted run 无上限增长。如果压缩作业挂掉或周期太长，流读的 merge 代价和文件数会恶化。需要对 `$files` 系统表或 `numSortedRuns` 指标做监控告警。

## 三、压缩作业（批模式，dedicated）

```bash
flink run -D execution.runtime-mode=batch \
    paimon-flink-action-*.jar compact \
    --warehouse ... --database ... --table t \
    --compact_strategy full \
    --table_conf snapshot.expire.limit=... \
    --table_conf sink.parallelism=256
```

- 批模式 + 定时调度（外部调度器按周期拉起）。并行度上限受 bucket 数约束，bucket 调到 256 后并行度才有意义。
- snapshot 过期和 changelog 清理默认由 committer 执行。写作业 write-only 时不做过期，所以过期职责落在压缩作业上，这是隐含耦合：压缩周期决定了过期执行频率。若压缩周期长，可单独跑 `expire_snapshots` procedure 解耦。
- 策略选择：不必每次 FULL。日常用默认策略（minor 语义）控制 sorted run 数，低峰期再跑 FULL 做读优化，重写量差一个量级。

## 四、读作业（流式）

```sql
SELECT ... FROM t /*+ OPTIONS(
    'scan.mode' = 'latest-full',           -- 或按需 latest / from-timestamp
    'streaming-read-mode' = 'log',         -- 消费 changelog
    'scan.snapshot-id' = '...'             -- 恢复场景按需
) */
```

- 若表设了 `changelog-producer = lookup/input`，流读直接消费 changelog 文件，无需 normalize 节点。
- `consumer-id`：可选但建议设置（`'consumer-id' = 'my-reader'`）。它让 Paimon 记录该消费者的进度，snapshot 过期时不会删除消费者尚未读到的 snapshot，解决前面说的"读作业中断后 snapshot 被过期"风险。代价是消费者停用后要手动清理，否则 snapshot 永不过期、存储膨胀。设了 consumer-id 就必须配套 `consumer.expiration-time`。

## 主要风险点汇总

1. **压缩作业是单点**：write-only 架构下它停了，文件堆积没有任何兜底。监控必须覆盖它的调度成功率和表的 sorted run 数。
2. **snapshot 过期与流读进度的竞态**：用 consumer-id 或足够长的 `snapshot.time-retained` 二选一兜底，两者都不配则读作业长时间故障后无法恢复。
3. **changelog-producer 与压缩模式的交互**：确认你选的 producer 不依赖 full compaction 的执行时机。
4. **批压缩期间的 commit 冲突**：写作业和压缩作业并发提交，Paimon 靠快照隔离和冲突重试处理，正常情况兼容，但 FULL 压缩全表重写窗口长，若遇到频繁冲突重试，观察压缩作业日志中的 conflict 记录。

需要确认的一点：你上游写入是完整 CDC 流还是可能存在部分列更新（partial-update / aggregation 引擎）？这直接决定 changelog-producer 的选择，也影响流读端的正确性。
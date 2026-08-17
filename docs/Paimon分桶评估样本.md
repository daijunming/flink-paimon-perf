# Paimon 分桶评估样本

## 一、样本用途

本样本来自现场测试消息截图，表结构为 `pk + c1..c99 + event_time`，共 101 列：

- [原始 UPDATE 样本](samples/wide-table-update-screenshot.json)：`before` 与 `after` 完全相同，用于测量现场 OGG 信封大小。
- [有效 UPDATE 样本](samples/wide-table-update-effective.json)：同一 `pk`，`after.c1_bigint` 与 `after.event_time` 各增加 1，用于代表能被 `sequence.field=event_time` 判为新版本的更新。

原始样本在 `changelog-producer.row-deduplicate=true` 下不会代表一次有效的值变化。分桶压力测算采用有效样本，但两条样本长度相同。

## 二、实际测量结果

测量口径为紧凑 JSON、UTF-8、排除文件末尾换行：

| 指标 | 原始样本 | 有效更新样本 |
|------|---------:|-------------:|
| 完整 OGG 事件 | 7,460 B | 7,460 B |
| `before` 行对象 | 3,630 B | 3,630 B |
| `after` 行对象 | 3,630 B | 3,630 B |
| 信封字段开销 | 200 B | 200 B |
| 行字段数 | 101 | 101 |
| STRING 字段数 / 字符总数 | 49 / 1,396 | 49 / 1,396 |
| 数值字段数 | 52 | 52 |

完整 OGG 事件可以估算 Kafka/Flink 反序列化前的负载：

| 吞吐 | JSON Value 负载，不含 Kafka 协议开销 |
|-----:|--------------------------------------:|
| 2,000 条/秒 | 14.23 MiB/秒 |
| 20,000 条/秒 | 142.29 MiB/秒 |

Paimon 不会把完整 OGG 信封原样存入 Data File。下文只用单侧 3,630 B 行对象作为候选 Bucket 之间的**未压缩相对比较代理**，不能把它当成 Parquet 实际行大小或压缩率。

## 三、每 Bucket 每 Checkpoint 增量代理

计算公式：

```text
每 Bucket 平均事件数 = 吞吐 × Checkpoint 秒数 / Bucket 数
每 Bucket 行镜像代理 = 每 Bucket 平均事件数 × 3,630 B
```

同一 Checkpoint 内同一 PK 多次更新时，实际待合并的不同 PK 数会更少；表中没有假设这一比例，因而只用于比较配置之间的相对压力。

| 吞吐（条/秒） | Checkpoint（秒） | Bucket | 每 Bucket 事件数 | 行镜像代理（MiB） |
|---------------:|-----------------:|-------:|------------------:|------------------:|
| 2,000 | 180 | 256 | 1,406.25 | 4.87 |
| 2,000 | 180 | 512 | 703.13 | 2.43 |
| 2,000 | 180 | 1024 | 351.56 | 1.22 |
| 2,000 | 60 | 256 | 468.75 | 1.62 |
| 2,000 | 60 | 512 | 234.38 | 0.81 |
| 2,000 | 60 | 1024 | 117.19 | 0.41 |
| 2,000 | 30 | 256 | 234.38 | 0.81 |
| 2,000 | 30 | 512 | 117.19 | 0.41 |
| 2,000 | 30 | 1024 | 58.59 | 0.20 |
| 20,000 | 180 | 256 | 14,062.50 | 48.68 |
| 20,000 | 180 | 512 | 7,031.25 | 24.34 |
| 20,000 | 180 | 1024 | 3,515.63 | 12.17 |
| 20,000 | 60 | 256 | 4,687.50 | 16.23 |
| 20,000 | 60 | 512 | 2,343.75 | 8.11 |
| 20,000 | 60 | 1024 | 1,171.88 | 4.06 |
| 20,000 | 30 | 256 | 2,343.75 | 8.11 |
| 20,000 | 30 | 512 | 1,171.88 | 4.06 |
| 20,000 | 30 | 1024 | 585.94 | 2.03 |

直接结论：在吞吐不变时，Checkpoint 从 180 秒缩短到 60 秒，每 Bucket 单次增量降为三分之一；Bucket 从 256 增加到 512，每 Bucket 单次增量再减半。因此“缩短 Checkpoint + 维持较多 Bucket”会直接加重 L0 和 Changelog 小文件压力。

## 四、现场物理文件样本

2026-08-14 截图采集值：

```text
data_bytes      = 140,863,930,419 B = 131.19 GiB
data_file_count = 19,357
物理 Data File 平均大小 = 6.94 MiB
```

若只把这 131.19 GiB 物理 Data File 均分，候选值如下：

| Bucket | 每 Bucket 物理 Data File |
|-------:|--------------------------:|
| 256 | 524.76 MiB |
| 512 | 262.38 MiB |
| 1024 | 131.19 MiB |

这不是最终 Bucket 结论。物理目录还包含保留 Snapshot 引用的旧文件和潜在孤儿文件；最新 Snapshot 的有效数据量只会小于或等于该物理值。Paimon 1.1 的一般建议是每 Bucket 约 200 MB 到 1 GB：按当前物理上界，256 和 512 落在该区间，1024 已低于下界；换成最新 Snapshot 有效数据后，512 也可能低于下界。

## 五、用已采集数据核实有效存量

以下 SQL 只读取现有 meta-collect ODS，不需要直接查询现场 Paimon 系统表：

```sql
-- 最新采集 Snapshot 的有效 Data File 总量，并换算三个候选 Bucket 的平均体积。
WITH latest AS (
  SELECT MAX(source_snapshot_id) AS snapshot_id
  FROM RDW_DATA.rdw_ods_paimon_meta_files
  WHERE table_name = 'wide_table'
), active AS (
  SELECT
    f.source_snapshot_id,
    COUNT(*) AS active_file_count,
    SUM(f.file_size_in_bytes) AS active_bytes,
    SUM(f.record_count) AS active_records
  FROM RDW_DATA.rdw_ods_paimon_meta_files f
  JOIN latest l ON f.source_snapshot_id = l.snapshot_id
  WHERE f.table_name = 'wide_table'
  GROUP BY f.source_snapshot_id
)
SELECT
  source_snapshot_id,
  active_file_count,
  active_bytes,
  active_records,
  ROUND(active_bytes / 256 / 1024 / 1024, 2) AS bucket_256_avg_mib,
  ROUND(active_bytes / 512 / 1024 / 1024, 2) AS bucket_512_avg_mib,
  ROUND(active_bytes / 1024 / 1024 / 1024, 2) AS bucket_1024_avg_mib
FROM active;
```

```sql
-- 当前 512 Bucket 的真实分布。不能只看平均数，p90/max 过大说明存在倾斜。
WITH latest AS (
  SELECT MAX(source_snapshot_id) AS snapshot_id
  FROM RDW_DATA.rdw_ods_paimon_meta_files
  WHERE table_name = 'wide_table'
)
SELECT
  f.bucket,
  COUNT(*) AS file_count,
  SUM(f.file_size_in_bytes) AS active_bytes,
  SUM(f.record_count) AS active_records,
  SUM(CASE WHEN f.level = 0 THEN 1 ELSE 0 END) AS l0_file_count,
  SUM(CASE WHEN f.level = 0 THEN f.file_size_in_bytes ELSE 0 END) AS l0_bytes,
  MIN(CASE WHEN f.level = 0 THEN f.creation_time END) AS oldest_l0_time
FROM RDW_DATA.rdw_ods_paimon_meta_files f
JOIN latest l ON f.source_snapshot_id = l.snapshot_id
WHERE f.table_name = 'wide_table'
GROUP BY f.bucket
ORDER BY active_bytes DESC;
```

## 六、分桶选择依据

当前证据支持以下判断：

1. **1024 暂不进入候选**：连物理 Data File 上界都只有 131.19 MiB/Bucket，同时进一步放大短 Checkpoint 的小文件数量。
2. **512 是当前运行基线，不是已被样本证明的最优值**：物理上界为 262.38 MiB/Bucket，换成最新 Snapshot 有效数据后可能低于约 200 MB 的一般建议下界。
3. **256 是后续 Rescale 对照测试的优先候选**：在当前物理上界下约 524.76 MiB/Bucket；相对 512，可把每 Bucket 每 Checkpoint 的事件数和行镜像代理提高一倍，减少潜在小文件数。
4. **本阶段仍保持 `bucket=512`**：改变固定 Bucket 涉及停止、Rescale 和回退验证，不应与缩短 Checkpoint、缩短 Minor 周期同时实施。

最终决策门槛：

- 最新 Snapshot 在 512 Bucket 下的实际 p50/p90 有效体积仍处于约 200 MB–1 GB，且存在需要 512 路读取或写入并行的证据：保留 512。
- 最新 Snapshot 在 512 Bucket 下长期低于约 200 MB，L0/Changelog 小文件持续增长，且没有超过 256 路并行的实际需要：优先验证 256。
- 单 Bucket p90/max 明显高于平均值：先处理主键 Hash 倾斜，不能只靠增加 Bucket 掩盖倾斜。

## 七、边界

- 单条样本不能代表字段长度分布、UPDATE 热点、不同 PK 比例或 Parquet 压缩率。
- `target-file-size` 决定 Compact 后目标文件大小，`write-buffer-size` 不是最终 Data File 大小。
- Bucket 数同时影响每次提交的小文件数量和最大处理并行度，不能只按总数据字节选择。
- 本文只形成评估依据，不修改生产表配置。

## 八、官方依据

- [Apache Paimon 1.1 Primary Key Table Overview](https://paimon.apache.org/docs/1.1/primary-key-table/overview/)：Bucket 是最小读写单元，一般建议每 Bucket 约 200 MB–1 GB。
- [Apache Paimon 1.1 DataFile](https://paimon.apache.org/docs/1.1/concepts/spec/datafile/)：主键表 Data File 存储业务列和 Paimon 系统列。
- [Apache Flink 1.19 OGG Format](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/table/formats/ogg/)：UPDATE 通过 `before` 与 `after` 对象表达。

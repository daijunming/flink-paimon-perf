# scripts/sql（入湖作业交付：SQL body + 运行参数 + 独立 compaction + 流式读作业）

对齐真实环境（2026-07-07 核对）。真实拓扑是**平台作业**,不是 sql-client 手工 `-i/-f`:

- **写入作业**（`DataStreamperf_paimon`,STREAMING）：SQL body（建源 + INSERT）+ 运行参数 JSON。写入 **write-only**（只写不合并）。
- **Compaction 作业**（`paimon-compact`）：crontab 每 5 分钟提交一次 BATCH Minor Compact,跑完即退出。
- **读取作业（流式）**：`07_streaming_read.sql`（流式读取 changelog）与 `08_streaming_agg.sql`（流式全表持续聚合）,叠加在写入作业上观测读写并发。真实需要的是**流式读取**与**流式聚合**,不是点查（Lookup Join）或批 OLAP 扫描。

## 文件清单

| 文件 | 内容 | 角色 |
|------|------|------|
| `01_catalog.sql` | 建 Paimon catalog `paimon_obs` + database `paimon_database` | preflight（一次性） |
| `02_sink_paimon.sql` | 建 `wide_table`（100 列 + event_time,**bucket=512**,deduplicate + lookup + write-only） | preflight（一次性） |
| `03_source_kafka.sql` | Kafka source 临时表（topic `src_pref_paimon`,ogg-json） | 写入作业 SQL body |
| `05_ingest_insert.sql` | `INSERT /*+ OPTIONS('sink.parallelism'='3', ...) */ SELECT … FROM kafka_source` | 写入作业 SQL body |
| `job-run-params.json` | 写入作业真实运行参数（parallelism=3、checkpoint 3min、rocksdb、mini-batch、not-null-enforcer=ERROR 等） | 写入作业运行参数 |
| `06_compaction_job.sh` | 独立 BATCH Minor Compact（`paimon-flink-action compact`,YARN 应用名 `paimon-compact`） | crontab 周期批任务 |
| `07_streaming_read.sql` | 流式读取 `wide_table` changelog（blackhole sink,测流读吞吐/延迟） | 读作业（流式,可选） |
| `08_streaming_agg.sql` | 流式全表持续聚合 COUNT/AVG/SUM/MAX（print sink） | 读作业（流式,可选） |

## 提交流程

1. **preflight（一次性建表）**：执行 `01_catalog.sql`、`02_sink_paimon.sql`（catalog + database + wide_table）。
2. **写入作业**：以 `03_source_kafka.sql` + `05_ingest_insert.sql` 为 SQL body,配 `job-run-params.json` 的运行参数,作为平台 STREAMING 作业提交（作业名 `DataStreamperf_paimon`）。
3. **compaction 作业**：crontab 每 5 分钟调用 `bash 06_compaction_job.sh`，提交独立 BATCH Minor Compact（YARN 应用名 `paimon-compact`，跑完即退出）。
4. **读作业（可选）**：`07_streaming_read.sql` / `08_streaming_agg.sql` 以流模式提交,与写入作业并发运行,观测读写相互影响。

## 关键设计（对齐真实环境）

1. **列/类型**：`pk` + `c1..c20` BIGINT + `c21..c40` DECIMAL(20,4) + `c41..c89` STRING + `c90..c99` BIGINT（epoch 毫秒） + `event_time` BIGINT。时间列用 BIGINT（生成器写毫秒数字,非 TIMESTAMP）。
2. **bucket=512（固定）**：按 2 天、2000 条/s 与现场真实 Data Files 规模估算，单 Bucket 约 1GB；不是变量 `${BUCKET_NUM}`。写入 parallelism=3 继续与 Kafka 3 分区对齐，两者不再要求相等。
3. **write-only + 独立 compaction**：`write-only=true` 持久化在表上，Writer 跳过 Compaction 与 Snapshot Expiration；合并由 `06_compaction_job.sh` 的 BATCH Minor Compact 完成。Action 显式使用 `lookup-compact=radical` 与 `num-sorted-run.compaction-trigger=5`。
4. **deduplicate + sequence + lookup**：同一 pk 按 `event_time` 毫秒"新值胜出"；`changelog-producer=lookup` 由 Compaction 生成完整变更，`row-deduplicate=true` 过滤值未变化的 `-U/+U`。
5. **ogg-json**：源为 OGG CDC（op_type=I/U/D）,支持 INSERT/UPDATE/DELETE。
6. **运行参数在 JSON,不在 SQL 里 SET**：`parallelism`/`checkpointing`/`table.exec.*` 等属于平台作业配置。其中 `not-null-enforcer=ERROR`（旧脚本 SET 的 DROP 已弃用,以运行参数为准）。

## 脱敏（提交到远程）

- **敏感基础设施用占位符,部署时填**：Kafka 地址 `${KAFKA_BOOTSTRAP_SERVERS}`（`03`）、HDFS 仓库路径 `${PAIMON_WAREHOUSE}`（`01`、`06`）;`job-run-params.json` 的 `sqlPath`/`modelTablePath` 已用 `flink_user` / `<task-uuid>` 脱敏。
- **逻辑值保留真实**：catalog `paimon_obs`、database `paimon_database`、table `wide_table`、topic `src_pref_paimon`、group `job_pref_paimon`、`bucket=512`（均非敏感）。

## 相比旧交付改了什么

- `06_point_lookup.sql`（点查 Lookup Join）/ `07_olap_scan.sql`（批 OLAP 扫描）：形态不符,已用流式 `07_streaming_read.sql` / `08_streaming_agg.sql` 取代。
- `init_phase1.sql` / `init_phase2.sql`：`SET 'BUCKET_NUM'=…` 变量注入在 Flink SQL 不生效;且 bucket 已固定=512、并发/checkpoint 等参数进了 `job-run-params.json`。删除。
- 建表 WITH 里的 write-buffer / compaction 选项：移到 INSERT hint（写入侧）与 compaction 作业的 `--table_conf`（合并侧）。

## 待定

- **阶段化（6.2 三阶段方案）**：真实作业当前是单一配置（bucket=512、parallelism=3、checkpoint 3min）。若后续要变 bucket / 并发 / 负载,方案定了再落成对应参数集。

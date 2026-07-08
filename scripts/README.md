# scripts（部署脚本与配置目录）

Paimon 表性能测试工具链的部署资源：Flink SQL 脚本（入湖 + 流式读作业）、Java 组件配置模板、编排脚本（待任务 9）。

## 目录结构

```
scripts/
├─ sql/                        # 入湖 + 读作业（组件 b）：SQL body + 运行参数 + 独立 compaction + 流式读
│  ├─ 01_catalog.sql           # 创建 Paimon catalog paimon_obs + database
│  ├─ 02_sink_paimon.sql       # 创建 100 列主键宽表（bucket=3, deduplicate）
│  ├─ 03_source_kafka.sql      # Kafka source 临时表（topic src_pref_paimon, ogg-json）
│  ├─ 05_ingest_insert.sql     # write-only INSERT（/*+ OPTIONS() */ hint 传写入参数）
│  ├─ 06_compaction_job.sh     # 独立 compaction 作业（paimon-flink-action compact）
│  ├─ 07_streaming_read.sql    # 流式读取 changelog（读性能，可选）
│  ├─ 08_streaming_agg.sql     # 流式全表持续聚合（读性能，可选）
│  ├─ job-run-params.json      # 写入作业真实运行参数（parallelism=3 / ckpt 3min / …）
│  └─ README.md                # 提交方式与设计决策说明
├─ conf/                       # Java 组件配置模板
│  ├─ data-generator.properties.template          # 数据生成器配置
│  ├─ metadata-collector.properties.template      # 元数据采集器配置
│  ├─ resource-collector.properties.template      # 资源采集器配置
│  └─ README.md                # 占位符含义与阶段化配置建议
└─ README.md（本文件）
```

> 编排脚本（任务 9）待实现：`env.sh`、`preflight.sh`、`start-*.sh`、`stop-all.sh` 等，
> 用于协调全流程（生成器 → 入湖 → 采集器 → 分析 SQL），届时将更新此 README。

## 交付形态

- **入湖作业**（组件 b）：平台 STREAMING 作业 = SQL body（`03`+`05`）+ 运行参数 JSON（`job-run-params.json`），作业名 `DataStreamperf_paimon`；建表 DDL（`01`+`02`）preflight 一次性执行；合并由独立 compaction 作业（`06_compaction_job.sh`，作业名 `compaction_job`）完成。另有流式读作业（`07_streaming_read.sql` / `08_streaming_agg.sql`）观测读性能。不打 jar。
- **Java 组件配置**：`.properties.template` 模板，部署时复制并填入真实环境值（或由编排脚本自动化替换占位符）
- **Java 组件产物**：三个 shaded fat jar（`data-generator.jar` / `metadata-collector.jar` / `resource-collector.jar`），
  由 Maven 构建在各自模块的 `target/` 目录产出，搬运到离线集群后用 `java -jar xxx.jar config.properties` 启动

## 占位符约定（运行环境注入，仓库内不填真值）

| 占位符 | 含义 | 用于 |
|--------|------|------|
| `${PAIMON_WAREHOUSE}` | Paimon 仓库 HDFS 路径（如 `hdfs:///user/<user>/paimon`） | SQL 脚本、metadata-collector |
| `${KAFKA_BOOTSTRAP_SERVERS}` | Kafka 地址（如 `kafka1:9092,kafka2:9092`） | 全部组件 |
| `${KAFKA_TOPIC}` | 测试数据 topic | data-generator |
| `${KAFKA_METRICS_TOPIC}` | 既有 metrics topic（`RDW_ODS_FLINK_METRICS_TOPIC`） | metadata-collector、resource-collector |
| `${YARN_RM_URL}` | YARN ResourceManager 基地址（如 `http://rm-host:8088`） | resource-collector |
| `${HDFS_NN_URL}` | HDFS NameNode 基地址（如 `http://nn-host:9870`） | resource-collector |

> 入湖 SQL 脚本（`sql/`）的**脱敏约定**：逻辑值（catalog `paimon_obs` / database `paimon_database` / table `wide_table` / topic `src_pref_paimon` / group `job_pref_paimon` / `bucket=3`）写真实值；**敏感基础设施用占位符**——Kafka 地址 `${KAFKA_BOOTSTRAP_SERVERS}`、HDFS 仓库路径 `${PAIMON_WAREHOUSE}`，部署时填。已删除失效的 `${BUCKET_NUM}` / `${SCAN_STARTUP_MODE}`（`init_phase{1,2}.sql` 的 `SET` 变量注入在 Flink SQL 不生效）。

详见各子目录的 README.md。

# scripts（部署脚本与配置目录）

Paimon 表性能测试工具链的部署资源：Flink SQL 脚本（入湖作业）、Java 组件配置模板、编排脚本（待任务 9）。

## 目录结构

```
scripts/
├─ sql/                        # Flink SQL 脚本（入湖作业，组件 b）
│  ├─ 01_catalog.sql           # 创建 Paimon catalog + database
│  ├─ 02_sink_paimon.sql       # 创建 100 列主键宽表（pk + 99 业务列 + event_time）
│  ├─ 03_source_kafka.sql      # Kafka source 临时表（format=json）
│  ├─ 05_ingest_insert.sql     # 入湖 INSERT 主脚本（透传 event_time）
│  ├─ init_phase1.sql          # 阶段1 极限压测参数（并发32 / bucket64 / earliest-offset）
│  ├─ init_phase2.sql          # 阶段2 生产模拟参数（并发8 / bucket16 / latest-offset）
│  └─ README.md                # SQL 脚本提交方式与设计决策说明
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

- **Flink SQL 脚本**（组件 b，入湖作业）：独立 `.sql` 文件，用 `flink sql-client -i/-f` 提交，不打 jar
- **Java 组件配置**：`.properties.template` 模板，部署时复制并填入真实环境值（或由编排脚本自动化替换占位符）
- **Java 组件产物**：三个 shaded fat jar（`data-generator.jar` / `metadata-collector.jar` / `resource-collector.jar`），
  由 Maven 构建在各自模块的 `target/` 目录产出，搬运到离线集群后用 `java -jar xxx.jar config.properties` 启动

## 占位符约定（运行环境注入，仓库内不填真值）

| 占位符 | 含义 | 用于 |
|--------|------|------|
| `${PAIMON_WAREHOUSE}` | Paimon 仓库 HDFS 路径（如 `hdfs:///warehouse/paimon_perf`） | SQL 脚本、metadata-collector |
| `${KAFKA_BOOTSTRAP_SERVERS}` | Kafka 地址（如 `kafka1:9092,kafka2:9092`） | 全部组件 |
| `${KAFKA_TOPIC}` | 测试数据 topic | data-generator、SQL 脚本 |
| `${KAFKA_METRICS_TOPIC}` | 既有 metrics topic（`RDW_ODS_FLINK_METRICS_TOPIC`） | metadata-collector、resource-collector |
| `${YARN_RM_URL}` | YARN ResourceManager 基地址（如 `http://rm-host:8088`） | resource-collector |
| `${HDFS_NN_URL}` | HDFS NameNode 基地址（如 `http://nn-host:9870`） | resource-collector |
| `${BUCKET_NUM}` | Paimon 表 bucket 数 | SQL 脚本（由 init_phase{1,2}.sql 注入） |
| `${SCAN_STARTUP_MODE}` | Kafka 起始位移模式 | SQL 脚本（由 init_phase{1,2}.sql 注入） |

详见各子目录的 README.md。

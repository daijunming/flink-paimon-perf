# Paimon 元数据采集器 — 本地开发指导

> 组件 c：周期读取 Paimon 表元数据（文件数 / 各 Level 数据量 / Compaction 信息 / 快照号与时间），
> 封装为统一指标信封发往既有 Kafka metrics topic，由既有 Flink→StarRocks 链路落表。

## 1. 前置环境

| 项 | 要求 | 本机现状 |
|----|------|----------|
| JDK | 8（目标 CDH 运行时） | `D:\soft\jdk-8u202` |
| Maven | 3.6+ | `C:\soft\apache-maven-3.8.5` |
| 网络 | 构建机需联网拉依赖（Paimon/Hadoop/Kafka）；目标集群离线 | 构建机有外网 |

构建用 JDK 8，每个 PowerShell 会话先设一次 `JAVA_HOME`：

```powershell
$env:JAVA_HOME="D:\soft\jdk-8u202"
```

> 不设的话会用默认的 JDK 11，虽然 `source/target=8` 仍产出 8 字节码，但工具链不一致。统一用 8 最稳。

## 2. 模块结构

```
metadata-collector/
├─ pom.xml                       # 依赖 common + paimon-bundle + hadoop-client，shade 出 fat jar
└─ src/main/java/com/paimonperf/metadata/
   ├─ MetadataReader.java                 # 读取接口（解耦 Paimon API）
   ├─ PaimonSystemTableMetadataReader.java# 实现：读 <table>$snapshots / <table>$files 系统表
   ├─ PaimonTableMetadata.java            # 中间模型（与 Paimon API 解耦）
   ├─ MetadataMetricMapper.java           # 纯映射：中间模型 → List<MetricEnvelope>（可单测）
   └─ MetadataCollectorMain.java          # 主入口：配置加载 + 周期采集 + 优雅关闭
```

设计上把 **Paimon API 调用**（`PaimonSystemTableMetadataReader`）与 **纯映射逻辑**（`MetadataMetricMapper`）
分开：前者依赖真实仓库、本地只能保证编译；后者纯数据、可在本机充分单测。

## 3. 本地能验证什么、不能验证什么

| 环节 | 本地可否 | 方式 |
|------|----------|------|
| 编译（含 Paimon API 签名） | ✅ | `mvn -pl metadata-collector compile` |
| 指标映射逻辑（`MetadataMetricMapper`） | ✅ 充分覆盖 | 单测 `MetadataMetricMapperTest` |
| 采集流程（读→映射→投递，用 mock） | ✅ | 集成测试 `MetadataCollectorIntegrationTest` |
| 真实读 Paimon 仓库（`$snapshots/$files`） | ❌ 需 HDFS + Paimon 表 | 仅能上目标集群验证 |
| 真实 Kafka 投递 | ❌ 需 broker | 集成测试用 mock sink 替代 |

> 一句话：**逻辑正确性本地全覆盖；与 Paimon/HDFS/Kafka 的真实交互留到集群冒烟。**

## 4. 常用命令

首次需先把 parent pom 与 common 装进本地仓库（子模块依赖它们）：

```powershell
$env:JAVA_HOME="D:\soft\jdk-8u202"
# 在工程根 flink-paimon-perf/ 下执行
mvn -N install -DskipTests          # 安装 parent pom
mvn -pl common install              # 安装 common（被采集器依赖）
```

日常开发循环：

```powershell
# 仅编译（最快，验证 Paimon API 用法是否正确）
mvn -pl metadata-collector compile

# 跑测试（映射单测 + 采集集成测试）
mvn -pl metadata-collector test

# 打 shaded fat jar（产物含全部依赖，可搬运到离线集群）
mvn -pl metadata-collector package -DskipTests
# 产物：metadata-collector/target/metadata-collector.jar（约 107 MB）
```

## 5. 配置项

主入口从 **properties 文件**（首个命令行参数）或 **系统属性 `-D`** 读取，缺必填项立即终止并打印缺失项名。

| 配置项 | 必填 | 默认 | 说明 |
|--------|------|------|------|
| `warehouse` | 是 | - | Paimon 仓库路径，如 `hdfs:///warehouse/paimon_perf` |
| `database` | 是 | - | 数据库名 |
| `table` | 是 | - | 业务宽表名（不带 `$` 后缀） |
| `kafka.bootstrap` | 是 | - | Kafka 地址，如 `kafka1:9092,kafka2:9092` |
| `kafka.metrics.topic` | 是 | - | 既有 metrics topic 名 |
| `collect.interval.seconds` | 否 | 30 | 采集周期（秒），须为正 |

示例 `metadata-collector.properties`：

```properties
warehouse=hdfs:///warehouse/paimon_perf
database=perf
table=wide_table
kafka.bootstrap=kafka1:9092,kafka2:9092
kafka.metrics.topic=RDW_ODS_FLINK_METRICS_TOPIC
collect.interval.seconds=30
```

## 6. 运行（目标集群）

```bash
# 方式一：properties 文件
java -jar metadata-collector.jar metadata-collector.properties

# 方式二：系统属性
java -Dwarehouse=hdfs:///warehouse/paimon_perf -Ddatabase=perf -Dtable=wide_table \
     -Dkafka.bootstrap=kafka1:9092 -Dkafka.metrics.topic=RDW_ODS_FLINK_METRICS_TOPIC \
     -jar metadata-collector.jar
```

进程常驻，按周期采集；`Ctrl+C` / `kill` 触发优雅关闭（停调度、flush+close Kafka、关 Paimon catalog）。

## 7. 产出的指标

`source=PAIMON_METADATA`，每周期产出：

| metric_name | 含义 | tags |
|-------------|------|------|
| `paimon.file.count` | 当前快照数据文件总数 | table |
| `paimon.snapshot.id` | 最新快照号 | table |
| `paimon.snapshot.time.millis` | 快照提交时间（毫秒） | table |
| `paimon.level.size.bytes` | 各 Level 数据量（字节） | table, level |
| `paimon.level.file.count` | 各 Level 文件数 | table, level |
| `paimon.last.commit.kind` | 提交类型编码（COMPACT=1.0 / APPEND=0.0 / 其他=0.5） | table |

## 8. 常见问题

- **`Could not find artifact com.paimonperf:paimon-perf-test:pom`**：parent pom 没装。先在工程根跑 `mvn -N install`。
- **`Could not find artifact com.paimonperf:common:jar`**：common 没装。跑 `mvn -pl common install`。
- **编译报 Paimon API 找不到符号**：检查 `paimon.version`（parent pom 中为 `1.1.0`）与系统表列名是否随版本变动；列名读取集中在 `PaimonSystemTableMetadataReader`，在那里调整。
- **想换 Paimon/Hadoop 版本**：改 parent `pom.xml` 的 `paimon.version` / `hadoop.version`，重新 `mvn -pl common install` 后再编译。

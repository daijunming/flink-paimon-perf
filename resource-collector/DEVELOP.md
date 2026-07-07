# YARN/HDFS 资源采集器 — 本地开发指导

> 组件 d：周期调用 YARN / HDFS REST API 采集 CPU/内存/存储指标，
> 封装为统一指标信封发往既有 Kafka metrics topic，由既有 Flink→StarRocks 链路落表。

## 1. 前置环境

| 项 | 要求 | 本机现状 |
|----|------|----------|
| JDK | 8（目标 CDH 运行时） | `D:\soft\jdk-8u202` |
| Maven | 3.6+ | `C:\soft\apache-maven-3.8.5` |
| 网络 | 构建机需联网拉依赖（Jackson/Kafka）；目标集群离线 | 构建机有外网 |

构建用 JDK 8，每个 PowerShell 会话先设一次 `JAVA_HOME`：

```powershell
$env:JAVA_HOME="D:\soft\jdk-8u202"
```

> 本采集器**不依赖 Hadoop 客户端**——只用 JDK 内置 HTTP 调 REST，依赖轻（产物约 15 MB，远小于 metadata-collector 的 107 MB）。

## 2. 模块结构

```
resource-collector/
├─ pom.xml                       # 依赖 common + jackson + kafka-clients（无 Hadoop），shade 出 fat jar
└─ src/main/java/com/paimonperf/resource/
   ├─ RestClient.java                  # REST 调用接口（解耦 HTTP）
   ├─ RestException.java               # REST 调用异常
   ├─ HttpRestClient.java              # 实现：JDK 内置 HttpURLConnection（兼容 JDK 8、零额外依赖）
   ├─ ResourceMetricParser.java        # 纯解析：YARN/HDFS JSON → List<MetricEnvelope>（可单测）
   └─ ResourceCollectorMain.java       # 主入口：配置加载 + 周期采集 + 容错
```

设计上把 **HTTP 调用**（`HttpRestClient`）与 **纯解析逻辑**（`ResourceMetricParser`）分开：
前者依赖真实 REST 端点、本地只能保证编译；后者纯数据（入参为 `JsonNode`），可用固定响应样本充分单测。

## 3. 本地能验证什么、不能验证什么

| 环节 | 本地可否 | 方式 |
|------|----------|------|
| 编译 | ✅ | `mvn -pl resource-collector compile` |
| 解析逻辑（`ResourceMetricParser`） | ✅ 充分覆盖 | 单测 `ResourceMetricParserTest`（固定 YARN/HDFS 响应样本） |
| 采集流程 + 容错（用 mock RestClient） | ✅ | 集成测试 `ResourceCollectorIntegrationTest` |
| 真实调 YARN/HDFS REST | ❌ 需集群端点 | 仅能上目标集群验证 |
| 真实 Kafka 投递 | ❌ 需 broker | 集成测试用 mock sink 替代 |

> 可选的真实端点冒烟：若本机能访问某 YARN/HDFS，可直接 `curl` 验证 REST 返回结构（见第 7 节），
> 再对照 `ResourceMetricParser` 的字段名核对。

## 4. 常用命令

首次需先把 parent pom 与 common 装进本地仓库：

```powershell
$env:JAVA_HOME="D:\soft\jdk-8u202"
# 在工程根 flink-paimon-perf/ 下执行
mvn -N install -DskipTests          # 安装 parent pom
mvn -pl common install              # 安装 common（被采集器依赖）
```

日常开发循环：

```powershell
mvn -pl resource-collector compile              # 仅编译
mvn -pl resource-collector test                 # 解析单测 + 采集集成测试
mvn -pl resource-collector package -DskipTests  # 打 shaded fat jar
# 产物：resource-collector/target/resource-collector.jar（约 15 MB）
```

> 集成测试日志里会出现 `ERROR ... YARN/HDFS 资源采集失败` —— 这是**故意触发的容错用例**
> （验证一侧/两侧 REST 失败时的隔离行为），只要最终 `BUILD SUCCESS`、`Failures: 0` 即正常。

## 5. 配置项

主入口从 **properties 文件**（首个命令行参数）或 **系统属性 `-D`** 读取，缺必填项立即终止并打印缺失项名。

| 配置项 | 必填 | 默认 | 说明 |
|--------|------|------|------|
| `yarn.rm.url` | 是 | - | YARN ResourceManager 基地址，如 `http://rm-host:8088` |
| `hdfs.nn.url` | 是 | - | HDFS NameNode 基地址，如 `http://nn-host:9870` |
| `kafka.bootstrap` | 是 | - | Kafka 地址 |
| `kafka.metrics.topic` | 是 | - | 既有 metrics topic 名 |
| `collect.interval.seconds` | 否 | 30 | 采集周期（秒），须为正 |

> 基地址只到 host:port，REST 路径由程序拼接（YARN `/ws/v1/cluster/metrics`、HDFS `/jmx`）。

示例 `resource-collector.properties`：

```properties
yarn.rm.url=http://rm-host:8088
hdfs.nn.url=http://nn-host:9870
kafka.bootstrap=kafka1:9092,kafka2:9092
kafka.metrics.topic=RDW_ODS_FLINK_METRICS_TOPIC
collect.interval.seconds=30
```

## 6. 运行（目标集群）

```bash
java -jar resource-collector.jar resource-collector.properties
```

进程常驻，按周期采集；`Ctrl+C` / `kill` 触发优雅关闭（停调度、flush+close Kafka）。

**容错语义**：每周期 YARN 与 HDFS 各自独立采集——一侧 REST 失败仅记录该侧错误、不影响另一侧，
两侧都失败时本周期投递空集；整体不抛异常，配合周期调度保证单次失败不中断后续周期。

## 7. 真实 REST 响应核对（可选）

若能访问真实集群，用 `curl` 看响应结构，对照解析字段：

```bash
# YARN：解析 clusterMetrics 下的 allocatedVirtualCores/availableVirtualCores/allocatedMB/availableMB
curl http://rm-host:8088/ws/v1/cluster/metrics

# HDFS：解析 beans 中 name 含 "FSNamesystem" 的 bean 的 CapacityTotal/CapacityUsed/CapacityRemaining
curl http://nn-host:9870/jmx
```

字段名变动时，集中在 `ResourceMetricParser.parseYarn` / `parseHdfs` 调整。

## 8. 产出的指标

| source | metric_name | 含义 |
|--------|-------------|------|
| YARN | `yarn.allocated.vcores` | 已分配 vCores |
| YARN | `yarn.available.vcores` | 可用 vCores |
| YARN | `yarn.allocated.memory.mb` | 已分配内存(MB) |
| YARN | `yarn.available.memory.mb` | 可用内存(MB) |
| HDFS | `hdfs.capacity.total.bytes` | 总容量(字节) |
| HDFS | `hdfs.capacity.used.bytes` | 已用容量(字节) |
| HDFS | `hdfs.capacity.remaining.bytes` | 剩余容量(字节) |

## 9. 常见问题

- **`Could not find artifact com.paimonperf:paimon-perf-test:pom`**：parent pom 没装。工程根跑 `mvn -N install`。
- **`Could not find artifact com.paimonperf:common:jar`**：common 没装。跑 `mvn -pl common install`。
- **REST 返回 200 但解析报缺字段**：集群版本的 REST schema 与默认字段名不符，按第 7 节核对后在 `ResourceMetricParser` 调整。
- **连接超时**：`HttpRestClient` 默认连接 5s / 读取 10s 超时；网络慢可在构造处调整。

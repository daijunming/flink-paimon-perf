# AGENTS.md — Paimon 性能测试工具链（paimon-perf-test）

> 面向 AI 编码代理的项目说明。阅读本文即可上手；更深入的设计细节见各目录的
> `README.md` / `DEVELOP.md` 与 `docs/`。

## 一、项目概述

验证 **Paimon 主键表在高频 UPDATE/DELETE 场景下的性能**，目标是 SLA 达标判定
（写入吞吐 ≥ 20000 条/秒、端到端延迟 ≤ 3 分钟）与瓶颈定位。

- **业务场景**：3000 万账户、100 列宽表、主键表、约 50% 高频 update。
- **测试分两阶段**：阶段1 单表极限压测（探上限、建基线）；阶段2 生产模拟负载
  （限速 20000 rps，连跑 5-7 天验证 SLA）。阶段3（多表混合负载）确认不做。
- **目标环境**：离线 CDH 集群（无外网），Flink 1.19.2 + Paimon 1.1 + YARN + HDFS
  + Kafka + StarRocks。所有产物在外网构建机打包后搬运到离线集群运行。
- **指标链路**：采集器/生成器 → Kafka metrics topic → 既有 Flink 链路 →
  StarRocks 表 `RDW_ODS_FLINK_METRICS`（12 列，`metric_value`/`metric_ts` 为 varchar）→
  `analysis-sql/` 视图分析。

## 二、仓库结构与模块划分

Maven 多模块工程（根 `pom.xml`，groupId `com.paimonperf`，packaging `pom`）。
**只有 4 个 Java 模块进 `<modules>`**；其余为资源目录（非 Maven 模块），随包搬运。

### Java 模块（包根 `com.paimonperf.<模块名>`）

| 模块 | 角色 | 产物 | 主类 |
|------|------|------|------|
| `common/` | 公共工具：`MetricEnvelope`（指标信封，对齐 `RDW_ODS_FLINK_METRICS` 12 字段）、`CollectorScheduler`、`KafkaMetricsSink` 等 | 普通 jar（不打 fat jar） | — |
| `data-generator/` | 组件 a：Kafka 测试数据生成器。生成 100 列宽表记录（OGG-JSON，op_type=I/U/D），UPDATE/DELETE 复用历史主键，可选令牌桶限速 | shaded fat jar `data-generator.jar`（约 15 MB） | `com.paimonperf.generator.GeneratorMain` |
| `metadata-collector/` | 组件 c：Paimon 元数据采集器（快照号/文件数/Level 分布/commit kind），含端到端延迟探针 | shaded fat jar `metadata-collector.jar`（约 107 MB，含 paimon-bundle + hadoop-client） | `com.paimonperf.metadata.MetadataCollectorMain` |
| `resource-collector/` | 组件 d：YARN/HDFS 资源采集器（JDK 内置 HTTP 调 REST，无 Hadoop 依赖） | shaded fat jar `resource-collector.jar`（约 15 MB） | `com.paimonperf.resource.ResourceCollectorMain` |

### 资源目录（非 Maven 模块）

- `scripts/sql/` — 组件 b：入湖作业 SQL。**不打 jar**，真实提交形态是"平台 STREAMING 作业
  = SQL body（`03_source_kafka.sql` + `05_ingest_insert.sql`）+ 运行参数 `job-run-params.json`"
  （作业名 `DataStreamperf_paimon`，write-only）；建表 DDL（`01`/`02`）preflight 一次性执行；
  合并由独立 compaction 作业（`06_compaction_job.sh`，paimon-flink-action `compact`，作业名
  `compaction_job`）完成；`07_streaming_read.sql` / `08_streaming_agg.sql` 为可选流式读作业。
- `scripts/conf/` — 三个 Java 组件的 `.properties.template` 配置模板（含 `${...}` 占位符）。
- `analysis-sql/` — 组件 f：StarRocks 分析 SQL。视图统一建在 `RDW_DATA` 库，底座是
  `01_metrics_view.sql`（分钟分桶 + 五个真实 job_name 白名单），其余为
  `02_four_category_metrics.sql` / `05_health_flags.sql` / `08_checkpoint_health.sql` /
  `09_streaming_read.sql`（流式读性能）及各自自包含的 `_test.sql`。
- `docs/` — 需求、设计、验证文档（中文）。
- `.kiro/` — 可移植的 AI 协作护栏**模板套件**（steering toolkit），非本项目的活跃 steering，
  不会被自动加载；其中 `git-commit-language.md` 描述了本仓库沿用的提交信息口径。

### 真实作业拓扑（分析 SQL 的事实基础，2026-07-07 核对）

指标按 `job_name` 区分来源（不要用 `app_id` 过滤）：

| job_name | 来源 |
|----------|------|
| `DataStreamperf_paimon` | 写入作业（write-only，Flink 任务级指标按 subtask 分行，聚合需先桶内取 MAX 再跨 subtask 求和） |
| `compaction_job` | 独立 compaction 作业（含 Paimon 桥接的 `compactionThreadBusy` / `avgCompactionTime`） |
| `wide_table` | metadata-collector（`paimon.*` 指标） |
| `cluster` | resource-collector（`yarn.*` / `hdfs.*` 指标） |
| `streaming_read_job` | 流式读作业（`scripts/sql/07_streaming_read.sql`，blackhole sink 流读 changelog；任务级指标同写入作业形态，分析见 `analysis-sql/09_streaming_read.sql`） |

## 三、技术栈

- **语言/构建**：Java 8（`maven.compiler.source/target=8`，目标 CDH 运行时为 JDK 8）、
  Maven 3.6+（本机 `C:\soft\apache-maven-3.8.5`，本机默认 `java` 即 JDK 1.8.0_202）。
- **依赖版本统一在根 `pom.xml` 的 `dependencyManagement`**（子模块不各自声明版本）：
  kafka-clients 2.8.1、jackson-databind 2.15.4、slf4j 1.7.36、
  paimon-bundle 1.1.1（仅 metadata-collector）、hadoop-client 3.3.6（仅 metadata-collector）。
- **打包**：maven-shade-plugin 3.2.4 打 fat jar（排除 `META-INF/*.SF|*.DSA|*.RSA` 签名文件），
  离线运行不联网拉依赖。
- **测试**：JUnit 5（5.10.2）+ jqwik 1.8.4 属性测试（`@Property(tries=N)`），
  surefire 3.0.0-M5 自动发现 JUnit Platform。
- **刻意排除 Python**：离线依赖传递复杂，采集器一律用 Java 单 jar 交付。

## 四、构建与测试命令

> 在工程根 `flink-paimon-perf/` 下执行；构建机需联网拉依赖，目标集群离线。

```bash
# 首次准备（把 parent pom 与 common 装进本地仓库；直接从根构建可跳过）
mvn -N install -DskipTests
mvn -pl common install

# 全量构建（产物在各模块 target/ 下）
mvn clean package -DskipTests

# 全量测试
mvn test

# 单模块开发循环（以 metadata-collector 为例，其余模块同理）
mvn -pl metadata-collector compile
mvn -pl metadata-collector test
mvn -pl metadata-collector package -DskipTests
```

Windows PowerShell 下若默认 JDK 不是 8，先设 `$env:JAVA_HOME="D:\soft\jdk-8u202"`。

常见报错：`Could not find artifact com.paimonperf:paimon-perf-test:pom` → parent pom 没装，
先 `mvn -N install`；`Could not find artifact com.paimonperf:common:jar` → 先
`mvn -pl common install`。

### StarRocks 分析 SQL 执行顺序（在 StarRocks 客户端）

```sql
SOURCE 01_metrics_view.sql;           -- 基础视图（其余视图的底座）
SOURCE 02_four_category_metrics.sql;  -- 观测视图集
SOURCE 05_health_flags.sql;           -- 依赖 02
SOURCE 08_checkpoint_health.sql;      -- 依赖 01

-- 逻辑验证（自包含临时表，不触碰真实分区表，本地即可执行）
SOURCE 01_metrics_view_test.sql;
SOURCE 05_health_flags_test.sql;
SOURCE 08_checkpoint_health_test.sql;
```

## 五、代码风格与设计约定

- **语言**：代码注释、文档、提交信息一律用**中文**；标识符用英文。
  提交信息格式 `type(scope): 描述`（type/scope 英文，描述中文，
  如 `refactor(analysis-sql,scripts): 对齐真实环境并脱敏敏感信息`），与 git 历史一致。
- **I/O 与纯逻辑分离**（本仓库最重要的结构约定）：
  Paimon API 调用（`PaimonSystemTableMetadataReader`）与映射逻辑（`MetadataMetricMapper`）分开；
  HTTP 调用（`HttpRestClient`）与解析逻辑（`ResourceMetricParser`）分开。
  纯数据逻辑在本机充分单测；与 Paimon/HDFS/Kafka/YARN 的真实交互留到目标集群冒烟。
- **失败隔离**：采集器单次失败不得中断后续周期（Property 5）；resource-collector 的
  YARN/HDFS 两侧各自独立 try/catch，一侧失败不影响另一侧。
- **配置契约**：主入口从 **properties 文件（首个命令行参数）** 或 **`-D` 系统属性**读取；
  缺必填项启动即终止并打印缺失项名（fail-fast）。必填项见各模块 `DEVELOP.md`。
- **优雅关闭**：常驻进程注册 shutdown hook（停调度 → flush+close Kafka → 关 catalog）。
- **Java 风格**：工具类/入口类用 `final class` + 私有构造；构造参数即校验，
  非法抛 `IllegalArgumentException`（中文消息）；日志用 slf4j（`LOG.info/error`）；
  内部集合存不可变副本（`Collections.unmodifiableMap`）。
- **指标信封**：所有采集器统一产出 `MetricEnvelope`（common 模块），
  `source` 枚举（PAIMON_METADATA/YARN/HDFS）映射到表字段 `metric_type`，
  `job_name` 取 tags 里的 `table`，`metric_id` 与 `etl_dt` 自动生成。改信封结构前先看
  `common/src/main/java/com/paimonperf/common/MetricEnvelope.java` 的类注释（含真实表 DDL）。

### Flink SQL 侧约定（改 `scripts/sql/` 时必读）

- **不用 SQL `SET` 做变量注入**（Flink SQL 不生效）；运行参数（parallelism/checkpoint/
  `table.exec.*` 等）放 `job-run-params.json`，由平台作业配置承载。
- 写入参数（`write-only=true` 等）经 INSERT 的 `/*+ OPTIONS() */` 动态 hint 传入，
  **不**写进建表 WITH；compaction 参数放 compaction 作业的 `--table_conf`。
- 表结构事实：`wide_table` = `pk` + c1..c20 BIGINT + c21..c40 DECIMAL(20,4) +
  c41..c89 STRING + c90..c99 BIGINT（epoch 毫秒）+ `event_time` BIGINT；
  **bucket=3 固定**（与 parallelism=3、Kafka 3 分区对齐）；
  `merge-engine=deduplicate` + `sequence.field=event_time`（同 pk 新值胜出）+
  `changelog-producer=input`。source/sink/INSERT 三处 100 列定义必须严格对齐。
- 源格式为 **ogg-json**（op_type=I/U/D）；DELETE 记录只含 pk + event_time，业务列缺失是正常设计。

## 六、测试策略

- **Java**：jqwik 属性测试（验证跨输入不变量，如比例、单调性、信息守恒）+ JUnit 5
  固定样本单测 + mock 集成测试（用 mock sink/RestClient 替代真实 Kafka/REST）。
  测试类命名：`*PropertyTest`（jqwik）、`*Test`（单测）、`*IntegrationTest`（mock 集成）。
- **本地能验证什么**：编译、纯映射/解析逻辑、mock 下的采集流程与容错。
  **本地不能验证什么**：真实 Paimon 仓库读写、真实 Kafka 投递、真实 YARN/HDFS REST
  ——这些只能在目标 CDH 集群冒烟（验证清单见 `docs/VALIDATION.md`，注意其部分脚本名已过时）。
- **集成测试日志出现 `ERROR ... 采集失败` 是故意触发的容错用例**，
  只要 `BUILD SUCCESS`、`Failures: 0` 即正常。
- **分析 SQL**：`*_test.sql` 均为自包含逻辑验证（建临时表、插固定样本、复现判定逻辑、
  断言、清理），不依赖真实数据。
- 属性测试与需求条目的对应关系见 `docs/DELIVERY.md` 第五节（Property 1-14）。

## 七、部署与运行

1. 外网构建机 `mvn clean package -DskipTests`，把三个 fat jar + `scripts/` +
   `analysis-sql/` 搬运到离线集群。
2. 复制 `scripts/conf/*.template` 为真实 `.properties` 并填入占位符值。
3. Preflight 建表：执行 `01_catalog.sql`、`02_sink_paimon.sql`。
4. 提交写入作业：SQL body（`03`+`05`）+ `job-run-params.json`，作业名 `DataStreamperf_paimon`。
5. 提交独立 compaction 作业：`bash 06_compaction_job.sh`（作业名 `compaction_job`）。
6. 启动 Java 组件：`java -jar data-generator.jar data-generator.properties`、
   `java -jar metadata-collector.jar metadata-collector.properties`、
   `java -jar resource-collector.jar resource-collector.properties`（常驻，Ctrl+C 优雅关闭）。
7. StarRocks 侧按第四节顺序执行分析 SQL 观测。

阶段化差异（生成器/采集器配置）：阶段1 `rate.limit.enabled=false`（不限速探上限）、
采集周期 30s；阶段2 `rate.limit.enabled=true` + `rate.limit.rps=20000`、采集周期 60s。
编排脚本（env.sh / preflight.sh / start-*.sh / stop-all.sh，任务 9）尚未实现。

## 八、安全与脱敏约定

- **敏感基础设施一律用 `${...}` 占位符，仓库内不填真值**：
  `${KAFKA_BOOTSTRAP_SERVERS}`、`${KAFKA_TOPIC}`、`${KAFKA_METRICS_TOPIC}`、
  `${PAIMON_WAREHOUSE}`、`${YARN_RM_URL}`、`${HDFS_NN_URL}`。
  部署时复制模板手工填入或由编排脚本 `sed` 替换。
- **逻辑值保留真实**（非敏感）：catalog `paimon_obs`、database `paimon_database`、
  table `wide_table`、topic `src_pref_paimon`、group `job_pref_paimon`、bucket=3。
- 已删除失效占位符 `${BUCKET_NUM}` / `${SCAN_STARTUP_MODE}`（`SET` 变量注入不生效），
  不要重新引入。
- metadata-collector 含 Kerberos 支持（`KerberosAuthenticator`）；密钥/keytab 不进仓库。

## 九、文档时效性提醒（避免被过时信息误导）

- **以各目录就近的 README/DEVELOP.md 为准**：`scripts/README.md`、`scripts/sql/README.md`、
  `scripts/conf/README.md`、`analysis-sql/README.md`、`metadata-collector/DEVELOP.md`、
  `resource-collector/DEVELOP.md` 均已对齐真实环境（2026-07-07）。
- **`docs/DELIVERY.md` 与 `docs/VALIDATION.md` 部分过时**：其中引用的
  `init_phase{1,2}.sql`、`06_point_lookup.sql`、`07_olap_scan.sql` 已被
  `job-run-params.json` + `06_compaction_job.sh` + 流式读 `07/08` 取代；
  `analysis-sql/` 的 `03_sla_check` / `04_baseline_compare` / `05_bottleneck_identify`
  已移除（可从 git 历史恢复）；`data-generator/DEVELOP.md` 实际不存在；
  `.kiro/specs/paimon-perf-test/` 目录已不存在。
- **延迟探针未实现**：`LatencyProbe.readMaxEventTime` 是占位实现（抛
  `UnsupportedOperationException`），`ingest.e2e_latency_ms` 从未产出，
  因此分析 SQL 刻意不做延迟 SLA 判定；读取性能仅覆盖流式读（`09_streaming_read.sql`，
  对应 `streaming_read_job`），点查/批 OLAP 仍无作业、不出视图。
- 改 Paimon/Hadoop 版本只动根 `pom.xml` 的 `paimon.version` / `hadoop.version`；
  Paimon 系统表列名读取集中在 `PaimonSystemTableMetadataReader`，REST 字段名集中在
  `ResourceMetricParser.parseYarn/parseHdfs`。

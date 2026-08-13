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
| `metadata-collector/` | 组件 c：Paimon 元数据采集器（快照号/文件数/Level 分布/commit kind）。**2026-08-11 现场退役**（代码保留、可构建可测试），无新数据；表侧元数据由 meta-collect ODS 承载 | shaded fat jar `metadata-collector.jar`（约 107 MB，含 paimon-bundle + hadoop-client） | `com.paimonperf.metadata.MetadataCollectorMain` |
| `resource-collector/` | 组件 d：YARN/HDFS 资源采集器（JDK 内置 HTTP 调 REST，无 Hadoop 依赖）。**2026-08-11 现场退役**（代码保留、可构建可测试），无新数据、无替代通路 | shaded fat jar `resource-collector.jar`（约 15 MB） | `com.paimonperf.resource.ResourceCollectorMain` |

### 资源目录（非 Maven 模块）

- `scripts/sql/` — 组件 b：入湖作业 SQL。**不打 jar**，真实提交形态是"平台 STREAMING 作业
  = SQL body（`03_source_kafka.sql` + `05_ingest_insert.sql`）+ 运行参数 `job-run-params.json`"
  （作业名 `DataStreamperf_paimon`，write-only）；建表 DDL（`01`/`02`）preflight 一次性执行；
  合并由 crontab 周期提交的批式 compact 任务（`06_compaction_job.sh`，paimon-flink-action
  `compact`，作业名 `paimon-compact`，每 5 分钟一轮、跑完即退出）完成；
  `07_streaming_read.sql` / `08_streaming_agg.sql` 为可选流式读作业。
- `scripts/conf/` — 三个 Java 组件的 `.properties.template` 配置模板（含 `${...}` 占位符）。
- `analysis-sql/` — 组件 f：StarRocks 分析 SQL。视图统一建在 `RDW_DATA` 库，底座是
  `01_metrics_view.sql`（分钟分桶 + 五个真实 job_name 白名单），其余为
  `02_four_category_metrics.sql` / `05_health_flags.sql` / `08_checkpoint_health.sql` /
  `09_streaming_read.sql`（流式读性能）及各自自包含的 `_test.sql`。
- `scripts/meta-collect/` — Paimon 元数据周期批采集（crontab + Flink SQL Batch，无状态简化版）。
  每轮全量重采 `$snapshots`/`$statistics` 未过期部分 + `$files`/`$manifests`/`$options`/`$consumers`
  当前态（作业内 `MAX(snapshot_id)` 打标），经 Kafka（topic 名与 SR 表一致：
  `rdw_ods_paimon_meta_*`）由 Routine Load 进 StarRocks ODS 层（`RDW_DATA.rdw_ods_paimon_meta_*`，
  只存可复核事实，健康结论由分析层计算；PRIMARY KEY 主键覆盖保证幂等，无本地游标）。
  原与 metadata-collector **并存**（聚合指标通路 vs 行级历史通路）；后者 2026-08-11 退役后，
  本目录成为唯一表侧元数据通路（analysis-sql 02/05/08 的表侧信号已改接这里）。详见其 README.md。
- `docs/` — 需求、设计、验证文档（中文）。
- `.kiro/` — 可移植的 AI 协作护栏**模板套件**（steering toolkit），非本项目的活跃 steering，
  不会被自动加载；其中 `git-commit-language.md` 描述了本仓库沿用的提交信息口径。

### 真实作业拓扑（分析 SQL 的事实基础，2026-08-06 核对）

指标按 `job_name` 区分来源（不要用 `app_id` 过滤）：

| job_name | 来源 |
|----------|------|
| `DataStreamperf_paimon` | 写入作业（write-only，Flink 任务级指标按 subtask 分行；同一指标存在任务级链名/算子级名双粒度重复上报、计数相同（2026-08-12 核实），聚合需桶内取 MAX → 单算子内跨 subtask 求和 → 跨算子取 MAX 去重，不可直接 SUM） |
| `compaction_job` | 旧流式常驻 compaction 作业（已退役；现行合并是 crontab 批任务 `paimon-compact`，生命周期几十秒、**不上报指标**，该行白名单保留仅为兼容历史数据） |
| `wide_table` | metadata-collector（`paimon.*` 指标）。**2026-08-11 现场退役**（代码保留可构建），无新数据；白名单保留仅为兼容历史数据，表侧元数据由 meta-collect ODS 承载 |
| `cluster` | resource-collector（`yarn.*` / `hdfs.*` 指标）。**2026-08-11 现场退役**（代码保留可构建），无新数据、无替代通路；白名单保留仅为兼容历史数据 |
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
SOURCE 02_four_category_metrics_test.sql;
SOURCE 05_health_flags_test.sql;
SOURCE 08_checkpoint_health_test.sql;
SOURCE 09_streaming_read_test.sql;
```

## 五、代码风格与设计约定

- **语言**：代码注释、文档、提交信息一律用**中文**；标识符用英文。
  提交信息格式 `type(scope): 描述`（type/scope 英文，描述中文，
  如 `refactor(analysis-sql,scripts): 对齐真实环境并脱敏敏感信息`），与 git 历史一致。
- **注释与解释的信息标准**：只承载代码里看不出来的信息——动机与约束（为什么存在）、
  触发条件（什么时候要动它）、后果与风险（动了会怎样）。禁止三种写法：复述代码
  （读者看代码即得的信息再说一遍）、名词套名词（用术语定义术语，不落到读者的具体处境）、
  指针接力（"见 XX 说明"而两处都不给实质内容）。自检方式：这句话删掉，读者会失去什么
  判断依据？什么都不失去就删。解释代码时同理：说"它存在的理由"，不逐行复述结构。
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
- `write-only=true`、`changelog-producer=lookup` 与 `row-deduplicate=true` 是表级语义，
  写入缓冲/并行度走 INSERT 的 `/*+ OPTIONS() */` 动态 hint；compaction 调优放 Action 的 `--table_conf`。
- 表结构事实：`wide_table` = `pk` + c1..c20 BIGINT + c21..c40 DECIMAL(20,4) +
  c41..c89 STRING + c90..c99 BIGINT（epoch 毫秒）+ `event_time` BIGINT；
  **bucket=512 固定**（按 2 天、2000 条/s 与现场真实 Data Files 规模估算，单 Bucket 约 1GB）；
  写入 parallelism=3 继续与 Kafka 3 分区对齐，不要求与 Bucket 数相等；
  `merge-engine=deduplicate` + `sequence.field=event_time`（同 pk 新值胜出）+
  `changelog-producer=lookup` + `changelog-producer.row-deduplicate=true` + `write-only=true`。
  source/sink/INSERT 三处 100 列定义必须严格对齐。
- 源格式为 **ogg-json**（op_type=I/U/D）；DELETE 记录只含 pk + event_time，业务列缺失是正常设计。

## 六、测试策略

- **Java**：jqwik 属性测试（验证跨输入不变量，如比例、单调性、信息守恒）+ JUnit 5
  固定样本单测 + mock 集成测试（用 mock sink/RestClient 替代真实 Kafka/REST）。
  测试类命名：`*PropertyTest`（jqwik）、`*Test`（单测）、`*IntegrationTest`（mock 集成）。
- **本地能验证什么**：编译、纯映射/解析逻辑、mock 下的采集流程与容错。
  **本地不能验证什么**：真实 Paimon 仓库读写、真实 Kafka 投递、真实 YARN/HDFS REST
  ——这些只能在目标 CDH 集群冒烟（验证清单见 `docs/开测前检查清单.md`）。
- **集成测试日志出现 `ERROR ... 采集失败` 是故意触发的容错用例**，
  只要 `BUILD SUCCESS`、`Failures: 0` 即正常。
- **分析 SQL**：`*_test.sql` 均为自包含逻辑验证（建临时表、插固定样本、复现判定逻辑、
  断言、清理），不依赖真实数据。
- 现存测试与验证点（原 `docs/DELIVERY.md` 的 Property 1-14 对照表已随文档删除）：
  data-generator `RecordFactoryPropertyTest`（宽表 100 列结构、I/U/D 比例、UPDATE/DELETE
  复用历史主键）、`GeneratorConfigPropertyTest`（配置校验拒绝非法参数）；
  metadata-collector `MetadataCollectorIntegrationTest`（采集流程与失败隔离）、
  `MetadataMetricMapperTest`（映射信息守恒）；resource-collector `ResourceMetricParserTest` /
  `ResourceCollectorIntegrationTest`（REST 解析与采集容错）；common `MetricEnvelopePropertyTest` /
  `CollectorSchedulerPropertyTest`；analysis-sql 五个 `_test.sql`（分桶/写入吞吐跨算子去重/健康标志/checkpoint/流式读）。

## 七、部署与运行

1. 外网构建机 `mvn clean package -DskipTests`，把 `data-generator.jar` + `scripts/` +
   `analysis-sql/` 搬运到离线集群（metadata/resource 两采集器 2026-08-11 退役，无需搬运）。
2. 复制 `scripts/conf/*.template` 为真实 `.properties` 并填入占位符值。
3. Preflight 建表：执行 `01_catalog.sql`、`02_sink_paimon.sql`。
4. 提交写入作业：SQL body（`03`+`05`）+ `job-run-params.json`，作业名 `DataStreamperf_paimon`。
5. 配置合并批任务：把 `06_compaction_job.sh` 挂入 crontab（`*/5` 分钟一轮，BATCH 模式
   `paimon-compact`，跑完即退出，每轮日志含耗时与退出码）。
6. 启动生成器：`java -jar data-generator.jar data-generator.properties`（常驻，Ctrl+C 优雅关闭）。
7. StarRocks 侧按第四节顺序执行分析 SQL 观测。
8. 表侧元数据通路（两采集器退役后为必选）：按 `scripts/meta-collect/README.md` 部署
   （SR 建表 + Routine Load + `meta-collect.properties` + crontab 每 3 分钟 `collect_once.sh`）；
   analysis-sql 02/05/08 的表侧信号依赖这里的 ODS 表与视图。

阶段化差异（生成器配置）：阶段1 `rate.limit.enabled=false`（不限速探上限）；
阶段2 `rate.limit.enabled=true` + `rate.limit.rps=20000`。
编排脚本（env.sh / preflight.sh / start-*.sh / stop-all.sh，任务 9）尚未实现。

## 八、安全与脱敏约定

- **敏感基础设施一律用 `${...}` 占位符，仓库内不填真值**：
  `${KAFKA_BOOTSTRAP_SERVERS}`、`${KAFKA_TOPIC}`、`${KAFKA_METRICS_TOPIC}`、
  `${PAIMON_WAREHOUSE}`、`${YARN_RM_URL}`、`${HDFS_NN_URL}`。
  部署时复制模板手工填入或由编排脚本 `sed` 替换。
- **逻辑值保留真实**（非敏感）：catalog `paimon_obs`、database `paimon_database`、
  table `wide_table`、topic `src_pref_paimon`、group `job_pref_paimon`、bucket=512。
- 已删除失效占位符 `${BUCKET_NUM}` / `${SCAN_STARTUP_MODE}`（`SET` 变量注入不生效），
  不要重新引入。
- metadata-collector 含 Kerberos 支持（`KerberosAuthenticator`）；密钥/keytab 不进仓库。

## 九、文档时效性提醒（避免被过时信息误导）

- **以各目录就近的 README/DEVELOP.md 为准**：`scripts/README.md`、`scripts/sql/README.md`、
  `scripts/conf/README.md`、`analysis-sql/README.md`、`metadata-collector/DEVELOP.md`、
  `resource-collector/DEVELOP.md` 均已对齐真实环境（2026-07-07）。
- **合并作业真实形态为 crontab 批任务（2026-08-06 核对）**：现场实际是 crontab 每 5 分钟
  提交一次 BATCH 模式的 `paimon-compact` action（跑完即退出），`06_compaction_job.sh`
  已对齐为该形态（旧流式常驻 `compaction_job` 描述退役）。批任务生命周期短于
  指标上报周期（3 分钟）且作业名不在分析视图白名单，compaction 作业侧指标
  （`metrics_compaction_job`、`compaction_flag`）在该形态下无数据——查不到 ≠ 没跑；
  写入/合并的分析口径以 `docs/写入与合并性能分析.md` 为准。
- **两个 Java 采集器已现场退役（2026-08-11）**：metadata-collector（job_name='wide_table'）
  与 resource-collector（job_name='cluster'）停止部署运行、无新数据（代码保留、可构建可测试）。
  analysis-sql 的表侧信号已改接 meta-collect ODS（`rdw_ods_paimon_meta_*` /
  `v_paimon_meta_level_stats`）：02 的 `metrics_update_delete_eff` 与 08 的
  `checkpoint_health` 改用快照表，05 的 `health_flags` 表侧列改用 level 汇总视图并移除
  `compaction_flag` 等三列；02 的 `metrics_resource_compaction` 视图已删除
  （yarn/hdfs 资源信号无替代，随之退役）。`docs/观测指标地图.md`、`docs/开测前检查清单.md`、
  `docs/测试报告模板.md`、`scripts/README.md`、`scripts/conf/README.md` 中仍有按两采集器
  在线编写的段落，未逐一更新，阅读时以本条为准。
- **2026-08-06 过期材料清理**：已删除（均可从 git 历史恢复）——`docs/ADAPTATION_*.md` 三件套
  （12 字段适配的中间态记录）、`docs/Paimon 观测监控参数说明.md` 与
  `docs/存储层观测执行说明.md`（PoC 阶段文物，采集执行器决策已被现行实现推翻）、
  `docs/VALIDATION.md`（由 `docs/开测前检查清单.md` 取代）、`docs/DELIVERY.md`
  （交付清单，测试对照已内嵌至本文档第六节）、`LatencyProbe`（延迟探针占位实现，
  每周期打 ERROR 日志但永不产出指标）。仍不存在的工件：`init_phase{1,2}.sql`、
  `06_point_lookup.sql`、`07_olap_scan.sql`、`analysis-sql/` 的 `03_sla_check` /
  `04_baseline_compare` / `05_bottleneck_identify`、`data-generator/DEVELOP.md`、
  `.kiro/specs/paimon-perf-test/`。
- **文档已于 2026-08-06 中文化重命名**（见名知意）：如 `PAIMON_METRICS_COVERAGE.md` →
  `观测指标地图.md`、`PRE_TEST_CHECKLIST.md` → `开测前检查清单.md`、
  `paimon-write-compact-analysis.md` → `写入与合并性能分析.md`；通用知识类文档归入
  `docs/archive/`；文档地图与推进顺序见 `docs/README.md`（导读）。
- **延迟探针已移除（2026-08-06 清理）**：原 `LatencyProbe` 为占位实现，
  `ingest.e2e_latency_ms` 从未产出，分析 SQL 不做延迟 SLA 判定（数据可见延迟用
  `08_checkpoint_health.sql` 的提交间隔近似）；读取性能仅覆盖流式读（`09_streaming_read.sql`，
  对应 `streaming_read_job`），点查/批 OLAP 仍无作业、不出视图。
- 改 Paimon/Hadoop 版本只动根 `pom.xml` 的 `paimon.version` / `hadoop.version`；
  Paimon 系统表列名读取集中在 `PaimonSystemTableMetadataReader`，REST 字段名集中在
  `ResourceMetricParser.parseYarn/parseHdfs`。

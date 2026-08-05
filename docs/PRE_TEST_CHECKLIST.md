# Paimon 性能测试 · 前置验证清单

> **使用说明**：在正式开始性能测试之前，逐项确认以下清单，确保测试环境、数据链路、指标采集均已就绪。每个检查项包含验证命令和预期结果。

---

## ✅ 一、环境基础设施检查

### 1.1 集群资源确认

- [ ] **YARN 集群可用**
  ```bash
  # 访问 YARN ResourceManager Web UI 或执行：
  curl -s ${YARN_RM_URL}/ws/v1/cluster/metrics | grep -i availableMB
  ```
  **预期**：返回可用内存 > 0，集群状态 `STARTED`

- [ ] **HDFS 存储可用**
  ```bash
  # 访问 HDFS NameNode Web UI 或执行：
  curl -s ${HDFS_NN_URL}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState \
    | grep -i capacityremaining
  ```
  **预期**：剩余容量 > 测试数据预估量（建议 > 500 GB）

- [ ] **Kafka 集群可用**
  ```bash
  # 检查 Kafka broker 连通性（需 kafka-tools 或 telnet）
  kafka-broker-api-versions.sh --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS}
  ```
  **预期**：返回 broker 列表，无连接错误

### 1.2 Flink 环境确认

- [ ] **Flink 版本**：1.19.2
  ```bash
  flink --version
  ```

- [ ] **Paimon 依赖**：paimon-bundle-1.1.1.jar 在 `$FLINK_HOME/lib/` 下
  ```bash
  ls -lh $FLINK_HOME/lib/paimon-bundle*.jar
  ```

- [ ] **Hadoop 配置**：`$HADOOP_CONF_DIR` 已设置，包含 `core-site.xml`、`hdfs-site.xml`
  ```bash
  ls -lh $HADOOP_CONF_DIR/*.xml
  ```

---

## ✅ 二、数据链路验证

### 2.1 Kafka Topic 就绪

- [ ] **源数据 Topic 已创建**：`${KAFKA_TOPIC}`（建议名称：`src_pref_paimon`）
  ```bash
  kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} --describe --topic ${KAFKA_TOPIC}
  ```
  **预期**：Partitions: 3（与 bucket 数对齐），ReplicationFactor: ≥ 2

- [ ] **指标 Topic 已创建**：`${KAFKA_METRICS_TOPIC}`
  ```bash
  kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} --describe --topic ${KAFKA_METRICS_TOPIC}
  ```
  **预期**：Topic 存在，可写入

### 2.2 Paimon 表与 Catalog 就绪

- [ ] **Catalog 已创建**：`paimon_obs`
  ```sql
  -- 在 Flink SQL Client 执行：
  SHOW CATALOGS;
  ```
  **预期**：返回结果包含 `paimon_obs`

- [ ] **Database 已创建**：`paimon_database`
  ```sql
  USE CATALOG paimon_obs;
  SHOW DATABASES;
  ```
  **预期**：返回结果包含 `paimon_database`

- [ ] **测试表已创建**：`wide_table`（100 列，bucket=3，主键 `pk`）
  ```sql
  USE paimon_obs.paimon_database;
  SHOW TABLES;
  DESC wide_table;
  ```
  **预期**：
  - 表存在
  - 列数 = 101（pk + c1..c99 + event_time）
  - Bucket 数 = 3（查看表选项）
  - 主键 = `pk`
  - `merge-engine` = `deduplicate`
  - `changelog-producer` = `input`

---

## ✅ 三、采集器与生成器就绪

### 3.1 Java 组件打包验证

- [ ] **data-generator.jar 已打包**
  ```bash
  ls -lh data-generator/target/data-generator.jar
  # 预期大小：~15 MB
  ```

- [ ] **metadata-collector.jar 已打包**
  ```bash
  ls -lh metadata-collector/target/metadata-collector.jar
  # 预期大小：~107 MB（包含 paimon-bundle + hadoop-client）
  ```

- [ ] **resource-collector.jar 已打包**
  ```bash
  ls -lh resource-collector/target/resource-collector.jar
  # 预期大小：~15 MB
  ```

### 3.2 配置文件准备

- [ ] **data-generator.properties 已配置**（从 template 复制并填充占位符）
  ```bash
  cat scripts/conf/data-generator.properties | grep -v '^#' | grep '='
  ```
  **必填项验证**：
  - `kafka.bootstrap.servers`（非占位符）
  - `kafka.topic`（非占位符）
  - `rate.limit.enabled`（true/false）
  - `rate.limit.rps`（阶段 2 必填）

- [ ] **metadata-collector.properties 已配置**
  ```bash
  cat scripts/conf/metadata-collector.properties | grep -v '^#' | grep '='
  ```
  **必填项验证**：
  - `paimon.warehouse`（非占位符）
  - `paimon.database`
  - `paimon.table`
  - `kafka.bootstrap.servers`
  - `kafka.metrics.topic`

- [ ] **resource-collector.properties 已配置**
  ```bash
  cat scripts/conf/resource-collector.properties | grep -v '^#' | grep '='
  ```
  **必填项验证**：
  - `yarn.resourcemanager.url`（非占位符）
  - `hdfs.namenode.url`（非占位符）
  - `kafka.bootstrap.servers`
  - `kafka.metrics.topic`

### 3.3 冒烟测试（单次采集验证）

- [ ] **data-generator 可启动**
  ```bash
  java -jar data-generator/target/data-generator.jar \
    scripts/conf/data-generator.properties &
  # 等待 10 秒后 Ctrl+C 停止
  ```
  **预期**：日志无 ERROR，Kafka topic 有数据写入

- [ ] **metadata-collector 可启动**
  ```bash
  java -jar metadata-collector/target/metadata-collector.jar \
    scripts/conf/metadata-collector.properties &
  # 等待 30 秒后 Ctrl+C 停止
  ```
  **预期**：日志显示 `采集成功`，Kafka metrics topic 有 `paimon.*` 指标

- [ ] **resource-collector 可启动**
  ```bash
  java -jar resource-collector/target/resource-collector.jar \
    scripts/conf/resource-collector.properties &
  # 等待 30 秒后 Ctrl+C 停止
  ```
  **预期**：日志显示 `YARN 采集成功` + `HDFS 采集成功`，Kafka metrics topic 有 `yarn.*` / `hdfs.*` 指标

---

## ✅ 四、Flink 作业验证

### 4.1 写入作业（DataStreamperf_paimon）

- [ ] **SQL 文件准备**：`scripts/sql/03_source_kafka.sql` + `05_ingest_insert.sql`
- [ ] **运行参数准备**：`scripts/sql/job-run-params.json`
  ```bash
  cat scripts/sql/job-run-params.json | jq .
  ```
  **关键参数验证**：
  - `parallelism`: 3
  - `checkpoint.interval`: `3min`
  - `execution.checkpointing.mode`: `EXACTLY_ONCE`

- [ ] **作业提交验证**（提交到平台或 Flink CLI）
  ```bash
  # 提交后，访问 Flink Web UI 确认：
  # 1. 作业名称 = DataStreamperf_paimon
  # 2. 状态 = RUNNING
  # 3. Parallelism = 3
  # 4. Checkpoint 已启用（3 分钟周期）
  ```

### 4.2 Compaction 作业（compaction_job）

- [ ] **启动脚本准备**：`scripts/sql/06_compaction_job.sh`
- [ ] **脚本内占位符已替换**
  ```bash
  cat scripts/sql/06_compaction_job.sh | grep '\${' || echo "无占位符"
  ```
  **预期**：返回 `无占位符`

- [ ] **作业提交验证**
  ```bash
  bash scripts/sql/06_compaction_job.sh
  # 等待 1-2 分钟，访问 Flink Web UI 确认：
  # 1. 作业名称 = compaction_job
  # 2. 状态 = RUNNING
  # 3. 日志显示 `Starting compaction service`
  ```

### 4.3 流式读/聚合作业（可选）

- [ ] **流式读作业脚本**：`scripts/sql/07_streaming_read.sql`
- [ ] **流式聚合作业脚本**：`scripts/sql/08_streaming_agg.sql`
- [ ] **job_name 已明确**：建议分别为 `streaming_read_job` 和 `streaming_agg_job`

---

## ✅ 五、指标链路验证

### 5.1 指标上报到 Kafka

- [ ] **生成器指标已上报**
  ```bash
  kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} \
    --topic ${KAFKA_METRICS_TOPIC} --from-beginning --max-messages 10 \
    | grep '"job_name":"DataStreamperf_paimon"'
  ```
  **预期**：至少有一条记录

- [ ] **metadata-collector 指标已上报**
  ```bash
  kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} \
    --topic ${KAFKA_METRICS_TOPIC} --from-beginning --max-messages 10 \
    | grep '"job_name":"wide_table"'
  ```
  **预期**：包含 `paimon.snapshot.id`、`paimon.file.count` 等指标

- [ ] **resource-collector 指标已上报**
  ```bash
  kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} \
    --topic ${KAFKA_METRICS_TOPIC} --from-beginning --max-messages 10 \
    | grep '"job_name":"cluster"'
  ```
  **预期**：包含 `yarn.allocated.vcores`、`hdfs.capacity.used.bytes` 等指标

### 5.2 指标流转到 StarRocks

- [ ] **既有 Flink 链路正常消费 Kafka metrics topic**
- [ ] **RDW_ODS_FLINK_METRICS 表有数据**
  ```sql
  -- 在 StarRocks 客户端执行：
  SELECT COUNT(*) AS total_records
  FROM RDW_ODS_FLINK_METRICS
  WHERE etl_dt = DATE_FORMAT(NOW(), '%Y%m%d');
  ```
  **预期**：total_records > 0

- [ ] **四个 job_name 均有数据**
  ```sql
  SELECT job_name, COUNT(*) AS cnt
  FROM RDW_ODS_FLINK_METRICS
  WHERE etl_dt = DATE_FORMAT(NOW(), '%Y%m%d')
  GROUP BY job_name;
  ```
  **预期**：返回包含 `DataStreamperf_paimon`、`compaction_job`、`wide_table`、`cluster`

---

## ✅ 六、分析视图验证

### 6.1 视图创建

- [ ] **01_metrics_view.sql 已执行**
  ```sql
  SHOW CREATE VIEW RDW_DATA.metrics_base_view_1min;
  ```
  **预期**：视图存在，WHERE 子句包含四个 job_name 白名单

- [ ] **02_four_category_metrics.sql 已执行**
  ```sql
  SHOW TABLES IN RDW_DATA LIKE 'metrics_%';
  ```
  **预期**：返回 `metrics_ingest_perf`、`metrics_compaction_job`、`metrics_resource_compaction`、`metrics_update_delete_eff`

- [ ] **05_health_flags.sql 已执行**
  ```sql
  SELECT * FROM RDW_DATA.health_flags LIMIT 1;
  ```
  **预期**：视图存在，包含 `l0_flag`、`compaction_flag`、`backpressure_flag`、`checkpoint_flag` 列

- [ ] **08_checkpoint_health.sql 已执行**
  ```sql
  SELECT * FROM RDW_DATA.checkpoint_health LIMIT 1;
  SELECT * FROM RDW_DATA.checkpoint_stall_alert LIMIT 1;
  ```
  **预期**：两个视图均存在

### 6.2 视图逻辑验证（运行 *_test.sql）

- [ ] **01_metrics_view_test.sql 通过**
  ```sql
  SOURCE analysis-sql/01_metrics_view_test.sql;
  ```
  **预期**：所有 assertion 通过（如 `分钟分桶逻辑正确`）

- [ ] **05_health_flags_test.sql 通过**
  ```sql
  SOURCE analysis-sql/05_health_flags_test.sql;
  ```
  **预期**：软标志判定逻辑正确（L0/Compaction/反压/停滞）

- [ ] **08_checkpoint_health_test.sql 通过**
  ```sql
  SOURCE analysis-sql/08_checkpoint_health_test.sql;
  ```
  **预期**：快照间隔计算与停滞告警逻辑正确

### 6.3 实时数据验证

- [ ] **metrics_ingest_perf 有实时数据**
  ```sql
  SELECT * FROM RDW_DATA.metrics_ingest_perf
  WHERE time_bucket_minute >= DATE_SUB(NOW(), INTERVAL 10 MINUTE)
  ORDER BY time_bucket_minute DESC;
  ```
  **预期**：返回最近 10 分钟的写入吞吐数据

- [ ] **metrics_compaction_job 有实时数据**
  ```sql
  SELECT * FROM RDW_DATA.metrics_compaction_job
  WHERE time_bucket_minute >= DATE_SUB(NOW(), INTERVAL 10 MINUTE)
  ORDER BY time_bucket_minute DESC;
  ```
  **预期**：返回最近 10 分钟的 Compaction 繁忙度数据

- [ ] **health_flags 逻辑正确**
  ```sql
  SELECT * FROM RDW_DATA.health_flags
  WHERE time_bucket_minute >= DATE_SUB(NOW(), INTERVAL 10 MINUTE)
  ORDER BY time_bucket_minute DESC;
  ```
  **预期**：所有 flag 列值为 `OK` 或触发告警（与实际情况一致）

---

## ✅ 七、测试阶段特定配置

### 阶段 1：极限压测

- [ ] **data-generator 配置**
  ```properties
  rate.limit.enabled=false  # 不限速
  ```

- [ ] **采集周期**：30 秒（metadata-collector 和 resource-collector 的 `collect.interval.seconds=30`）

### 阶段 2：生产模拟负载

- [ ] **data-generator 配置**
  ```properties
  rate.limit.enabled=true
  rate.limit.rps=20000
  ```

- [ ] **采集周期**：60 秒（`collect.interval.seconds=60`）

---

## ✅ 八、文档与工具准备

- [ ] **PAIMON_METRICS_COVERAGE.md 已阅读**：了解每个场景关注的指标
- [ ] **TEST_REPORT_TEMPLATE.md 已准备**：明确测试结果记录格式
- [ ] **StarRocks 客户端可用**：能执行查询和导出数据
- [ ] **监控大盘（可选）**：Grafana / Kibana 等可视化工具配置（如有）

---

## ⚠️ 常见问题排查

### Q1：Kafka 指标未上报

**排查步骤**：
1. 检查采集器日志：`ERROR` / `Connection refused`
2. 验证 Kafka 连通性：`telnet ${KAFKA_HOST} 9092`
3. 确认 topic 存在且有写入权限

### Q2：StarRocks 表无数据

**排查步骤**：
1. 确认既有 Flink 链路作业正常运行
2. 检查 Kafka topic 有数据：`kafka-console-consumer.sh`
3. 查看 Flink 链路日志：sink 是否报错

### Q3：分析视图返回空结果

**排查步骤**：
1. 确认 `RDW_ODS_FLINK_METRICS` 有对应 job_name 数据
2. 检查视图 WHERE 子句的 job_name 白名单是否包含目标作业
3. 确认 `metric_value` / `metric_ts` 不为 NULL（部分指标可能缺失）

### Q4：作业提交失败

**排查步骤**：
1. 检查 Flink 日志：`$FLINK_HOME/log/`
2. 验证 Paimon warehouse 路径可访问：`hdfs dfs -ls ${PAIMON_WAREHOUSE}`
3. 确认 YARN 资源充足：访问 YARN Web UI

---

## 📋 验证完成签署

**验证人**：[姓名]  
**验证日期**：[YYYY-MM-DD]  
**完成情况**：
- 一、环境基础设施：✅ 全部完成 / ⚠️ 部分完成（缺失项：______）
- 二、数据链路：✅ 全部完成 / ⚠️ 部分完成（缺失项：______）
- 三、采集器与生成器：✅ 全部完成 / ⚠️ 部分完成（缺失项：______）
- 四、Flink 作业：✅ 全部完成 / ⚠️ 部分完成（缺失项：______）
- 五、指标链路：✅ 全部完成 / ⚠️ 部分完成（缺失项：______）
- 六、分析视图：✅ 全部完成 / ⚠️ 部分完成（缺失项：______）

**是否可开始正式测试**：✅ 是 / ❌ 否（需先完成：______）

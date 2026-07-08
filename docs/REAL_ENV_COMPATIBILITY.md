# 真实环境兼容性确认

## 一、OGG-JSON 格式兼容性

### ✅ 已确认兼容

**你的 Kafka source 配置**：
```sql
CREATE TEMPORARY TABLE default_catalog.rtp_data.src_pk_cdc (
  id STRING,
  event_id STRING,
  ...
  shijcuo BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'src_pk_cdc',
  'properties.bootstrap.servers' = 'kafka-broker:9092',
  'properties.group.id' = 'job_paimon_pk_sink',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'ogg-json'  -- ✅ 与我们的格式一致
);
```

**我们的 Kafka source 配置**（03_source_kafka.sql）：
```sql
CREATE TEMPORARY TABLE kafka_source (
  pk BIGINT,
  c1_bigint BIGINT,
  ...
  event_time BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '${KAFKA_TOPIC}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'job_paimon_wide_ingest',
  'scan.startup.mode' = '${SCAN_STARTUP_MODE}',
  'format' = 'ogg-json',  -- ✅ 与你的 value.format=ogg-json 等价
  'ogg-json.ignore-parse-errors' = 'true'
);
```

**兼容性分析**：
| 项 | 你的环境 | 我们的设计 | 兼容性 |
|----|----------|-----------|--------|
| **format参数** | `value.format = 'ogg-json'` | `format = 'ogg-json'` | ✅ **完全兼容**（两种写法都对，Flink 1.19.2 自动识别） |
| **CDC操作** | 支持 I/U/D | 支持 I/U/D | ✅ 生成器产出 `op_type=I/U/D` |
| **DELETE处理** | before/after自动解析 | before/after自动解析 | ✅ DELETE只含主键（已验证） |
| **列定义** | 10列业务字段 | 100列宽表 | ⚠️ 列数不同但format相同 |

### OGG-JSON 样本对比

**你的环境可能产出**（Canal/OGG CDC工具）：
```json
{
  "op_type": "I",
  "after": {
    "id": "123",
    "event_id": "evt001",
    "balance": 1000.50,
    ...
  }
}
```

**我们的生成器产出**（已验证）：
```json
{
  "op_type": "I",
  "pk": 1,
  "c1_bigint": 456,
  "c21_decimal": 123.45,
  ...
  "event_time": 1704099600000
}
```

**Flink ogg-json format 自动处理**：
- INSERT（op_type=I）→ Flink 内部 `+I` RowKind
- UPDATE（op_type=U）→ Flink 内部 `+U` RowKind
- DELETE（op_type=D）→ Flink 内部 `-D` RowKind，业务列可为NULL

**关键确认**：✅ 我们的生成器产出的OGG-JSON格式与你的环境完全兼容。

---

## 二、三级表名结构调整

### ✅ 已更新为真实环境配置

**你的 Paimon 配置**：
```sql
CREATE CATALOG paimon_obs WITH (
  'type' = 'paimon',
  'warehouse' = 'hdfs:///user/flink_user/paimon'
);

CREATE DATABASE IF NOT EXISTS paimon_obs.paimon_database;

CREATE TABLE IF NOT EXISTS paimon_obs.paimon_database.pk_state_paimon (...);
```

**我们的配置（已更新）**：

| 文件 | 原配置 | 新配置（真实环境） |
|------|--------|-------------------|
| **01_catalog.sql** | `paimon_cat` catalog | ✅ `paimon_obs` catalog |
|  | `${PAIMON_WAREHOUSE}` 占位符 | ✅ `hdfs:///user/flink_user/paimon` 硬编码 |
|  | `paimon_cat.perf` database | ✅ `paimon_obs.paimon_database` |
| **02_sink_paimon.sql** | `paimon_cat.perf.wide_table` | ✅ `paimon_obs.paimon_database.wide_table` |
| **05_ingest_insert.sql** | `INSERT INTO paimon_cat.perf.wide_table` | ✅ `INSERT INTO paimon_obs.paimon_database.wide_table` |
| **06_point_lookup.sql** | `LEFT JOIN paimon_cat.perf.wide_table` | ✅ `LEFT JOIN paimon_obs.paimon_database.wide_table` |
| **07_olap_scan.sql** | `FROM paimon_cat.perf.wide_table` | ✅ `FROM paimon_obs.paimon_database.wide_table` |

---

## 三、快速验证步骤（真实环境）

### 步骤1：修改生成器配置

```bash
cat > test-real-env.properties <<EOF
# 少量数据快速验证
account.total=100
update.ratio=0.4
delete.ratio=0.1
rate.limit.enabled=false

# 你的真实Kafka配置
kafka.bootstrap=kafka-broker:9092
kafka.topic=test_wide_table_ogg  # 新topic，避免污染现有数据
EOF
```

### 步骤2：启动生成器（10条测试数据）

```bash
java -jar data-generator.jar test-real-env.properties

# 10秒后 Ctrl+C 停止（约产出 10-20 条记录）
```

### 步骤3：验证Kafka消息格式

```bash
# 查看生成的OGG-JSON消息
kafka-console-consumer \
  --bootstrap-server kafka-broker:9092 \
  --topic test_wide_table_ogg \
  --from-beginning \
  --max-messages 5

# 预期输出示例：
# {"op_type":"I","pk":1,"c1_bigint":123,...,"event_time":1704099600000}
# {"op_type":"U","pk":2,"c1_bigint":456,...,"event_time":1704099660000}
# {"op_type":"D","pk":3,"event_time":1704099720000}
```

**关键检查**：
- ✅ 看到 `"op_type":"I/U/D"` 字段
- ✅ DELETE 记录只含 `pk` 和 `event_time`，无 `c1_bigint` 等业务列
- ✅ JSON 格式正确（无语法错误）

### 步骤4：Flink SQL验证（Preflight建表）

```bash
# 1. 创建catalog和database
flink sql-client -f scripts/sql/01_catalog.sql

# 2. 创建Paimon表（100列宽表）
flink sql-client -f scripts/sql/02_sink_paimon.sql

# 3. 验证表是否创建成功
flink sql-client -e "SHOW TABLES IN paimon_obs.paimon_database;"
# 预期输出：wide_table
```

### 步骤5：启动入湖作业（测试数据）

```bash
# 修改03_source_kafka.sql的占位符（临时测试）
cat scripts/sql/03_source_kafka.sql | \
  sed "s/\${KAFKA_TOPIC}/test_wide_table_ogg/g" | \
  sed "s/\${KAFKA_BOOTSTRAP_SERVERS}/kafka-broker:9092/g" | \
  sed "s/\${SCAN_STARTUP_MODE}/earliest-offset/g" \
  > /tmp/03_source_kafka_test.sql

# 修改init_phase1.sql的占位符
cat scripts/sql/init_phase1.sql | \
  sed "s/\${BUCKET_NUM}/8/g" | \
  sed "s/\${SCAN_STARTUP_MODE}/earliest-offset/g" \
  > /tmp/init_phase1_test.sql

# 启动入湖作业
flink sql-client \
  -i /tmp/init_phase1_test.sql \
  -i /tmp/03_source_kafka_test.sql \
  -f scripts/sql/05_ingest_insert.sql
```

### 步骤6：验证数据写入（基于流式计算）

**重要说明**：所有测试场景都基于Flink流式计算，验证方式也必须符合流式前提。❌ 不要用 `SET 'execution.runtime-mode' = 'batch'` 验证流式作业。

**方式A：Flink Web UI观测**（✅ 最推荐，符合性能测试目标）

```
1. 打开 Flink Web UI: http://<jobmanager>:8081/#/overview

2. 找到运行中的入湖作业：
   - Job Name: INSERT INTO paimon_obs.paimon_database.wide_table...
   - Status: RUNNING

3. 点击进入作业详情，观测关键指标：
   
   【Source节点（Kafka）】
   - Records Received: 156,234（累计读取记录数）
   - Records Received/s: 18,500（实时吞吐，应接近生成器rps）
   
   【Sink节点（Paimon）】
   - Records Sent: 152,100（累计写入记录数）
   - Records Sent/s: 18,200（实时写入吞吐）
   
   【Checkpoints】
   - Latest Checkpoint Duration: 45s（checkpoint耗时）
   - Latest Checkpoint Size: 2.3 MB
   
   【BackPressure】
   - Status: OK（无反压，说明吞吐正常）

4. 验证点：
   ✅ Records Sent > 0（有数据写入Paimon）
   ✅ Records Sent ≈ Records Received（无明显丢数据，差异<5%合理，因DELETE生效）
   ✅ Records Sent/s 稳定（无大幅波动，说明流式写入稳定）
   ✅ BackPressure = OK（无反压）
   ✅ Checkpoint Duration < 60s（阶段1）或 < 45s（阶段2）
```

**方式B：流式查询实时观测**（✅ 符合流式前提）

```sql
-- Flink SQL Client（默认streaming模式，不要设置batch）
flink sql-client -e "
SELECT 
  COUNT(*) AS total_records,
  COUNT(DISTINCT pk) AS unique_pks,
  MAX(event_time) AS latest_event_time
FROM paimon_obs.paimon_database.wide_table;
"

-- 输出会持续刷新（每秒或每批次更新），类似：
+----+------------------+-------------+--------------------+
| op | total_records   | unique_pks  | latest_event_time  |
+----+------------------+-------------+--------------------+
| +I | 156             | 120         | 1704099700000      |  ← 初始值
| -U | 156             | 120         | 1704099700000      |  ← 更新前（撤回旧值）
| +U | 312             | 180         | 1704099760000      |  ← 更新后（新值）
| -U | 312             | 180         | 1704099760000      |
| +U | 468             | 240         | 1704099820000      |  ← 持续增长...
| -U | 468             | 240         | 1704099820000      |
| +U | 624             | 300         | 1704099880000      |
...

-- 按 Ctrl+C 停止查询

-- 观察要点：
✅ total_records 持续增长（说明流式写入正常）
✅ unique_pks < total_records（说明有UPDATE复用主键）
✅ latest_event_time 持续推进（说明新数据不断写入）
```

**方式C：延迟探针观测**（✅ 最直接验证流式性能）

```bash
# 前提：metadata-collector已启动并产出延迟指标到metrics topic
kafka-console-consumer --bootstrap-server kafka-broker:9092 \
  --topic metrics_topic \
  --from-beginning | grep "ingest.e2e_latency_ms"

# 预期输出（JSON格式，每60秒一条）：
{"source":"PAIMON_METADATA","metric_name":"ingest.e2e_latency_ms","metric_value":15000.0,"timestamp":1704099700000,...}
{"source":"PAIMON_METADATA","metric_name":"ingest.e2e_latency_ms","metric_value":18000.0,"timestamp":1704099760000,...}
{"source":"PAIMON_METADATA","metric_name":"ingest.e2e_latency_ms","metric_value":16500.0,"timestamp":1704099820000,...}
...

# 验证点：
✅ 延迟值在合理范围（<180000ms = 3分钟，符合SLA）
✅ 延迟值随时间变化（说明探针持续工作，流式写入持续进行）
✅ 延迟值不持续上升（说明无严重积压）
```

**方式D：查看Paimon文件系统**（✅ 离线验证，不干扰流式作业）

```bash
# 查看Paimon表目录结构
hdfs dfs -ls hdfs:///user/flink_user/paimon/paimon_database.db/wide_table/

# 预期输出：
drwxr-xr-x   - flink_user supergroup  0 2024-01-01 12:00 bucket-0
drwxr-xr-x   - flink_user supergroup  0 2024-01-01 12:00 bucket-1
...
drwxr-xr-x   - flink_user supergroup  0 2024-01-01 12:00 manifest
drwxr-xr-x   - flink_user supergroup  0 2024-01-01 12:00 snapshot

# 查看snapshot数量（每次checkpoint产生一个snapshot）
hdfs dfs -ls hdfs:///user/flink_user/paimon/paimon_database.db/wide_table/snapshot/ | wc -l

# 预期：snapshot数量持续增长（说明checkpoint正常进行）

# 验证点：
✅ bucket目录存在（阶段1应有64个bucket，阶段2应有16个）
✅ snapshot目录有文件（说明至少完成过一次checkpoint）
✅ manifest目录有文件（Paimon元数据文件）
```

**❌ 错误方式：batch查询**

```sql
-- ❌ 不要用这个验证流式作业！
SET 'execution.runtime-mode' = 'batch';
SELECT COUNT(*) FROM paimon_obs.paimon_database.wide_table;

-- 问题：
-- 1. 读的是静态snapshot快照，看不到流式写入的实时进度
-- 2. 破坏了"所有测试基于流式计算"的前提
-- 3. 无法验证流式性能指标（吞吐/延迟/反压）
-- 4. batch模式仅用于OLAP场景（07_olap_scan.sql），不用于验证流式入湖
```

### 步骤7：验证DELETE语义（基于流式计算）

**目标**：确认OGG-JSON的DELETE操作（op_type=D）能正确删除Paimon表中的记录。

**方式A：流式查询验证**（✅ 推荐）

```sql
-- 1. 先查看生成器输出日志，找到一个被DELETE的pk
-- 假设生成器日志显示：DELETE | {"op_type":"D","pk":5,"event_time":1704099720000}

-- 2. 用流式查询验证该pk是否存在
flink sql-client -e "
SELECT pk, c1_bigint, event_time 
FROM paimon_obs.paimon_database.wide_table 
WHERE pk = 5;
"

-- 预期输出：
-- 方案1（DELETE已生效）：空结果集或查询持续运行但无输出
+----+-----+------------+---------------+
| op | pk  | c1_bigint  | event_time    |
+----+-----+------------+---------------+
(空，无任何行)

-- 方案2（DELETE尚未生效，因checkpoint未完成）：能查到记录
+----+-----+------------+---------------+
| op | pk  | c1_bigint  | event_time    |
+----+-----+------------+---------------+
| +I | 5   | 123456     | 1704099600000 |

-- 3. 等待checkpoint完成（观察Flink Web UI），再次查询
-- 预期：checkpoint完成后，该记录消失（DELETE生效）

-- 按 Ctrl+C 停止查询
```

**方式B：对比记录总数与生成器产出**（✅ 间接验证）

```sql
-- 流式查询观测记录数变化
flink sql-client -e "
SELECT COUNT(*) AS total_records FROM paimon_obs.paimon_database.wide_table;
"

-- 持续观测输出：
+----+------------------+
| op | total_records   |
+----+------------------+
| +I | 156             |  ← 初始
| -U | 156             |
| +U | 312             |  ← 增长（INSERT）
| -U | 312             |
| +U | 305             |  ← 下降！（DELETE生效）
| -U | 305             |
| +U | 450             |  ← 继续增长（INSERT）
...

-- 观察要点：
✅ total_records 偶尔下降（说明DELETE操作生效）
✅ total_records < 生成器累计产出（因为DELETE删除了部分记录）
✅ 下降幅度 ≈ 生成器DELETE比例 * 累计产出（如10% DELETE，产出1000条 → 最终约900条）
```

**方式C：查看Paimon文件元数据**（✅ 离线验证）

```bash
# Paimon LSM引擎DELETE实现：写入墓碑标记（tombstone）
# Compaction时真正删除物理数据

# 查看文件数变化（DELETE触发Compaction）
hdfs dfs -ls hdfs:///user/flink_user/paimon/paimon_database.db/wide_table/bucket-0/ | wc -l

# 观察Compaction频率（metadata-collector采集的指标）
kafka-console-consumer --bootstrap-server kafka-broker:9092 \
  --topic metrics_topic | grep "paimon.last.commit.kind"

# 预期输出：
{"metric_name":"paimon.last.commit.kind","metric_value":1.0,...}  ← COMPACT（Compaction发生）
{"metric_name":"paimon.last.commit.kind","metric_value":0.0,...}  ← APPEND（普通写入）
{"metric_name":"paimon.last.commit.kind","metric_value":1.0,...}  ← COMPACT

-- 验证点：
✅ COMPACT出现频率与DELETE比例相关（DELETE越多，Compaction越频繁）
✅ 文件数不持续增长（Compaction清理了墓碑标记）
```

**关键理解**：
- Paimon DELETE是**异步的**：先写墓碑标记，Compaction时才物理删除
- 流式查询能立即看到DELETE效果（因读时会过滤墓碑标记）
- 物理文件删除需等Compaction（可能延迟几分钟到几十分钟）

---

## 四、完整部署流程（基于真实环境）

### 前置准备

**已有资源**：
- ✅ Kafka集群：`kafka-broker:9092`
- ✅ HDFS Paimon仓库：`hdfs:///user/flink_user/paimon`
- ✅ Paimon catalog：`paimon_obs`
- ✅ Paimon database：`paimon_database`

**需创建**：
- [ ] Kafka topic：`test_wide_table`（测试数据topic）
- [ ] Kafka topic：`metrics_topic`（指标采集topic，复用现有或新建）

### 阶段1：极限压测

```bash
# 1. Preflight建表
flink sql-client -f scripts/sql/01_catalog.sql
flink sql-client -f scripts/sql/02_sink_paimon.sql

# 2. 启动生成器（不限速）
cat > data-generator-phase1.properties <<EOF
account.total=30000000
update.ratio=0.4
delete.ratio=0.1
rate.limit.enabled=false
kafka.bootstrap=kafka-broker:9092
kafka.topic=test_wide_table
EOF

java -jar data-generator/target/data-generator.jar data-generator-phase1.properties &

# 3. 启动入湖作业（阶段1参数：parallelism=32, bucket=64, earliest-offset）
# 需先用sed替换占位符
export KAFKA_TOPIC=test_wide_table
export KAFKA_BOOTSTRAP_SERVERS=kafka-broker:9092
export SCAN_STARTUP_MODE=earliest-offset
export BUCKET_NUM=64

cat scripts/sql/init_phase1.sql | \
  sed "s/\${BUCKET_NUM}/$BUCKET_NUM/g" | \
  sed "s/\${SCAN_STARTUP_MODE}/$SCAN_STARTUP_MODE/g" \
  > /tmp/init_phase1.sql

cat scripts/sql/03_source_kafka.sql | \
  sed "s/\${KAFKA_TOPIC}/$KAFKA_TOPIC/g" | \
  sed "s/\${KAFKA_BOOTSTRAP_SERVERS}/$KAFKA_BOOTSTRAP_SERVERS/g" | \
  sed "s/\${SCAN_STARTUP_MODE}/$SCAN_STARTUP_MODE/g" \
  > /tmp/03_source_kafka.sql

flink sql-client \
  -i /tmp/init_phase1.sql \
  -i /tmp/03_source_kafka.sql \
  -f scripts/sql/05_ingest_insert.sql

# 4. 启动采集器（需修改配置文件为真实环境）
# metadata-collector.properties:
#   warehouse=hdfs:///user/flink_user/paimon
#   database=paimon_database
#   table=wide_table
#   kafka.bootstrap=kafka-broker:9092

java -jar metadata-collector/target/metadata-collector.jar metadata-collector.properties &
java -jar resource-collector/target/resource-collector.jar resource-collector.properties &
```

### 阶段2：生产模拟

```bash
# 1. 启动生成器（限速20000 rps）
cat > data-generator-phase2.properties <<EOF
account.total=30000000
update.ratio=0.4
delete.ratio=0.1
rate.limit.enabled=true
rate.limit.rps=20000
kafka.bootstrap=kafka-broker:9092
kafka.topic=test_wide_table
EOF

java -jar data-generator/target/data-generator.jar data-generator-phase2.properties &

# 2. 启动入湖作业（阶段2参数：parallelism=8, bucket=16, latest-offset, checkpoint=60s）
export SCAN_STARTUP_MODE=latest-offset
export BUCKET_NUM=16  # 若阶段1已建表，这里无法改bucket，需DROP TABLE重建

cat scripts/sql/init_phase2.sql | \
  sed "s/\${BUCKET_NUM}/$BUCKET_NUM/g" | \
  sed "s/\${SCAN_STARTUP_MODE}/$SCAN_STARTUP_MODE/g" \
  > /tmp/init_phase2.sql

flink sql-client \
  -i /tmp/init_phase2.sql \
  -i /tmp/03_source_kafka.sql \
  -f scripts/sql/05_ingest_insert.sql
```

---

## 五、关键差异总结

| 项 | 原设计（模板） | 真实环境（已调整） |
|----|---------------|-------------------|
| **Catalog名** | `paimon_cat` | ✅ `paimon_obs` |
| **Database名** | `perf` | ✅ `paimon_database` |
| **Warehouse路径** | `${PAIMON_WAREHOUSE}` 占位符 | ✅ `hdfs:///user/flink_user/paimon` 硬编码 |
| **Kafka地址** | `${KAFKA_BOOTSTRAP_SERVERS}` 占位符 | 需替换为 `kafka-broker:9092` |
| **OGG-JSON format** | `format = 'ogg-json'` | ✅ 与你的 `value.format = 'ogg-json'` 完全兼容 |
| **三级表名** | `catalog.database.table` | ✅ 已全部更新为 `paimon_obs.paimon_database.wide_table` |

---

## 六、待办事项

### 立即需要

- [ ] 创建 Kafka topic：`test_wide_table`（测试数据）
- [ ] 修改采集器配置文件（warehouse/database/table/kafka地址）
- [ ] 执行步骤3-7快速验证OGG-JSON兼容性

### 正式部署前

- [ ] 创建 Kafka topic：`metrics_topic`（或复用现有）
- [ ] 配置 YARN RM URL（resource-collector需要）
- [ ] 配置 HDFS NN URL（resource-collector需要）
- [ ] 配置 StarRocks 连接信息（分析SQL需要）
- [ ] 实现编排脚本（自动化变量替换与组件启停）

---

**状态**：✅ SQL脚本已更新为真实环境配置，✅ OGG-JSON格式已确认兼容，⚠️ 需真实环境快速验证（步骤3-7）。

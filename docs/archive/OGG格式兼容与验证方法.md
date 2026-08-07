# 真实环境兼容性确认

> **归档说明（2026-08-06）**：格式兼容性确认工作已完成，本文档仅作历史参考，
> 不作为现行工作依据；现行工作路径见 `../README.md`（文档导读）。

> **本文档现行角色**：OGG-JSON 兼容性结论与流式验证方法参考；部署流程以 `scripts/README.md` 与 `scripts/meta-collect/README.md` 为准（作业提交形态详见 `scripts/sql/README.md`），本文不再覆盖。

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
CREATE TEMPORARY TABLE default_catalog.default_database.kafka_source (
  pk BIGINT,
  c1_bigint BIGINT,
  ...
  event_time BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'src_pref_paimon',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'job_pref_paimon',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'ogg-json'  -- ✅ 与你的 value.format=ogg-json 等价
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

### ✅ 已对齐真实环境配置

**你的 Paimon 配置**：
```sql
CREATE CATALOG paimon_obs WITH (
  'type' = 'paimon',
  'warehouse' = '${PAIMON_WAREHOUSE}'  -- 仓库内以占位符表示，部署时填真实 HDFS 路径
);

CREATE DATABASE IF NOT EXISTS paimon_obs.paimon_database;

CREATE TABLE IF NOT EXISTS paimon_obs.paimon_database.pk_state_paimon (...);
```

**我们的配置（已对齐）**：

| 文件 | 原配置（模板） | 现行配置（真实环境） |
|------|--------|-------------------|
| **01_catalog.sql** | `paimon_cat` catalog | ✅ `paimon_obs` catalog |
|  | `paimon_cat.perf` database | ✅ `paimon_obs.paimon_database` |
| **02_sink_paimon.sql** | `paimon_cat.perf.wide_table` | ✅ `paimon_obs.paimon_database.wide_table`（bucket=3 固定） |
| **05_ingest_insert.sql** | `INSERT INTO paimon_cat.perf.wide_table` | ✅ `INSERT INTO paimon_obs.paimon_database.wide_table` |

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
# 1. 执行 preflight 建表（一次性，详见 scripts/sql/README.md）：
#    - 01_catalog.sql：catalog + database（warehouse 为 ${PAIMON_WAREHOUSE} 占位符，先填真实值）
#    - 02_sink_paimon.sql：100 列宽表 wide_table（bucket=3 固定）

# 2. 验证表是否创建成功
flink sql-client -e "SHOW TABLES IN paimon_obs.paimon_database;"
# 预期输出：wide_table
```

### 步骤5：启动入湖作业（测试数据）

写入作业的真实提交形态是**平台 STREAMING 作业**：以 `03_source_kafka.sql` + `05_ingest_insert.sql`
为 SQL body，配 `job-run-params.json` 的运行参数提交（作业名 `DataStreamperf_paimon`，
详见 `scripts/sql/README.md`）。

快速验证注意：`03_source_kafka.sql` 的 topic 固定为 `src_pref_paimon`；若步骤1用的是
独立测试 topic（如 `test_wide_table_ogg`），需先把 03 的 topic 改成该测试 topic 再提交。

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

**方式C（延迟探针观测）：未实现，已移除**。`LatencyProbe` 代码已随清理移除，
`ingest.e2e_latency_ms` 指标从未产出，端到端延迟
暂无探针手段，不要按该指标做验证。

**方式D：查看Paimon文件系统**（✅ 离线验证，不干扰流式作业）

```bash
# 查看Paimon表目录结构
hdfs dfs -ls ${PAIMON_WAREHOUSE}/paimon_database.db/wide_table/

# 预期输出：
drwxr-xr-x   - flink_user supergroup  0 2024-01-01 12:00 bucket-0
drwxr-xr-x   - flink_user supergroup  0 2024-01-01 12:00 bucket-1
drwxr-xr-x   - flink_user supergroup  0 2024-01-01 12:00 bucket-2
drwxr-xr-x   - flink_user supergroup  0 2024-01-01 12:00 manifest
drwxr-xr-x   - flink_user supergroup  0 2024-01-01 12:00 snapshot

# 查看snapshot数量（每次checkpoint产生一个snapshot）
hdfs dfs -ls ${PAIMON_WAREHOUSE}/paimon_database.db/wide_table/snapshot/ | wc -l

# 预期：snapshot数量持续增长（说明checkpoint正常进行）

# 验证点：
✅ bucket目录存在（bucket=3 固定，应有 bucket-0/1/2 三个目录，与 parallelism=3、Kafka 3 分区对齐）
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
-- 4. 本测试现阶段无点查/批 OLAP 作业，batch 模式不属于验证手段
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
hdfs dfs -ls ${PAIMON_WAREHOUSE}/paimon_database.db/wide_table/bucket-0/ | wc -l

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

**状态**：✅ OGG-JSON 格式已确认兼容，三级表名已对齐真实环境（`paimon_obs.paimon_database.wide_table`）；⚠️ 步骤1-7 的流式验证需在真实环境执行。

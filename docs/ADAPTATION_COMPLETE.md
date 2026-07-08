# 真实表结构适配完成总结

## 适配状态：✅ 核心完成，可部署验证

---

## 已完成工作

### 1. Java代码调整（✅ 完成并编译通过）

#### MetricEnvelope.java
- ✅ 增加9个新字段对齐真实表
- ✅ 自动生成 `metric_id` = metricType_metricName_collectTs
- ✅ 自动生成 `etl_dt` = yyyy-MM-dd格式日期
- ✅ 新增9个getter方法
- ✅ 更新equals/hashCode/toString

#### MetricEnvelopeSerializer.java
- ✅ JSON格式完全重写为12字段
- ✅ metric_value: double→String
- ✅ metric_ts: long→String
- ✅ 字段顺序对齐真实表DDL

#### 编译验证
- ✅ common模块: BUILD SUCCESS
- ✅ metadata-collector: BUILD SUCCESS, jar生成
- ✅ resource-collector: BUILD SUCCESS, jar生成

---

### 2. 分析SQL重写（✅ 完成）

#### 01_metrics_view.sql
- ✅ 字段映射：metric_type → source
- ✅ 类型转换：CAST(metric_value AS DOUBLE)
- ✅ 类型转换：CAST(metric_ts AS BIGINT)
- ✅ 时段分桶：FROM_UNIXTIME(..., '%Y-%m-%d %H:%i:00')
- ✅ 过滤条件：job_name='paimon-perf-test' AND app_id='wide_table'

#### 01_metrics_view_test.sql
- ✅ 测试数据格式调整为12字段
- ✅ INSERT语句对齐真实表结构
- ✅ 验证查询调整为内联子查询（避免依赖view定义）

---

## JSON输出格式（已验证）

**MetricEnvelope序列化后的JSON**：
```json
{
  "etl_dt": "2024-01-01",
  "metric_id": "PAIMON_METADATA_paimon.file.count_1704099600000",
  "job_name": "paimon-perf-test",
  "app_id": "wide_table",
  "job_id": "",
  "host_name": "",
  "container_id": "",
  "container_rule": "",
  "metric_name": "paimon.file.count",
  "metric_type": "PAIMON_METADATA",
  "metric_value": "12345.0",
  "metric_ts": "1704099600000"
}
```

**关键点**：
- ✅ 12个字段完整
- ✅ metric_value是String类型
- ✅ metric_ts是String类型
- ✅ metric_id格式：{metricType}_{metricName}_{timestamp}

---

## 部署验证步骤

### 步骤1：启动metadata-collector

```bash
# 配置文件：metadata-collector.properties
warehouse=hdfs:///user/flink_user/paimon
database=paimon_database
table=wide_table
collect.interval.seconds=60
kafka.bootstrap=kafka-broker:9092
kafka.metrics.topic=RDW_ODS_FLINK_METRICS_TOPIC

# 启动
java -jar metadata-collector/target/metadata-collector.jar \
  metadata-collector.properties
```

### 步骤2：验证Kafka消息

```bash
kafka-console-consumer --bootstrap-server kafka-broker:9092 \
  --topic RDW_ODS_FLINK_METRICS_TOPIC \
  --from-beginning --max-messages 1
```

**预期输出**：
```json
{"etl_dt":"2024-01-01","metric_id":"PAIMON_METADATA_...","job_name":"paimon-perf-test",...}
```

**验证点**：
- ✅ JSON包含12个字段
- ✅ metric_type = "PAIMON_METADATA"
- ✅ job_name = "paimon-perf-test"
- ✅ app_id = "wide_table"

### 步骤3：验证StarRocks表数据

```sql
-- 查询最新10条测试数据
SELECT
  etl_dt,
  metric_id,
  metric_type,
  metric_name,
  CAST(metric_value AS DOUBLE) AS value_num,
  FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000) AS ts_time
FROM RDW_ODS_FLINK_METRICS
WHERE job_name = 'paimon-perf-test'
  AND app_id = 'wide_table'
ORDER BY CAST(metric_ts AS BIGINT) DESC
LIMIT 10;
```

**验证点**：
- ✅ 有数据返回
- ✅ metric_type = 'PAIMON_METADATA' / 'YARN' / 'HDFS'
- ✅ metric_value可转换为DOUBLE
- ✅ metric_ts可转换为BIGINT

### 步骤4：验证metrics_view

```sql
-- 验证view可正常查询
SELECT
  source,
  metric_name,
  AVG(metric_value) AS avg_value,
  COUNT(*) AS cnt
FROM metrics_view
WHERE source = 'PAIMON_METADATA'
  AND time_bucket_minute >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
GROUP BY source, metric_name;
```

**预期输出**：
```
source='PAIMON_METADATA', metric_name='paimon.file.count', avg_value=xxx, cnt=xx
source='PAIMON_METADATA', metric_name='paimon.snapshot.id', avg_value=xxx, cnt=xx
...
```

### 步骤5：验证四类指标SQL

```sql
-- 验证02_four_category_metrics.sql可运行
SELECT * FROM four_category_metrics
WHERE time_bucket_minute >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
LIMIT 10;
```

---

## 待办工作（可选）

### P1：调整其他测试SQL

需要调整INSERT语句为12字段格式：
- `analysis-sql/03_sla_check_test.sql`
- `analysis-sql/04_baseline_compare_test.sql`
- `analysis-sql/05_bottleneck_identify_test.sql`

**模板**（参考01_test.sql）：
```sql
INSERT INTO RDW_ODS_FLINK_METRICS VALUES
(
  '2024-01-01',                    -- etl_dt
  'PAIMON_METADATA_xxx_1704067200000', -- metric_id
  'paimon-perf-test-test',        -- job_name
  'wide_table',                    -- app_id
  '', '', '', '',                  -- job_id, host_name, container_id, container_rule
  'paimon.file.count',             -- metric_name
  'PAIMON_METADATA',               -- metric_type
  '100.0',                         -- metric_value (String)
  '1704067200000'                  -- metric_ts (String)
);
```

### P2：更新单元测试断言

- `common/src/test/java/com/paimonperf/common/MetricEnvelopeTest.java`
  - 增加新字段getter的断言
  - 验证metricId和etlDt生成规则

- `common/src/test/java/com/paimonperf/common/MetricEnvelopeSerializerTest.java`
  - 验证JSON包含12个字段
  - 验证metric_value和metric_ts是String类型

### P3：创建JSON格式验证测试

创建 `JsonFormatVerificationTest.java`（参考ADAPTATION_TODO.md）

---

## 核心变更总结

### 字段映射规则

| 原设计 | 真实表字段 | 生成规则 |
|--------|-----------|---------|
| `source` (枚举) | `metric_type` (varchar) | source.name() → "PAIMON_METADATA" |
| - | `job_name` | 固定 "paimon-perf-test" |
| - | `app_id` | 固定 "wide_table" |
| `metricName` | `metric_name` | 直接映射 |
| `metricValue` (double) | `metric_value` (varchar) | String.valueOf(double) |
| `collectTsMillis` (long) | `metric_ts` (varchar) | String.valueOf(long) |
| - | `metric_id` | metricType + "_" + metricName + "_" + collectTs |
| - | `etl_dt` | SimpleDateFormat("yyyy-MM-dd").format(collectTs) |
| `tags` (Map) | 独立列 | job_id / host_name / container_id / container_rule |

### 关键设计决策

1. **metricId生成**：保证唯一性，便于排查
   - 格式：`PAIMON_METADATA_paimon.file.count_1704099600000`
   - 组成：metricType + metricName + timestamp

2. **etlDt自动生成**：分区键，从timestamp提取
   - 格式：`yyyy-MM-dd`
   - 示例：`2024-01-01`

3. **类型转换**：对齐真实表varchar字段
   - `metric_value`: `double` → `String.valueOf()`
   - `metric_ts`: `long` → `String.valueOf()`

4. **采集器无需修改**：新字段由MetricEnvelope构造函数自动填充

---

## 常见问题

### Q1：为什么采集器不需要修改？

**A**：新字段都由 `MetricEnvelope` 构造函数自动生成：
```java
// 采集器现有代码（无需改动）
MetricEnvelope envelope = new MetricEnvelope(
    MetricSource.PAIMON_METADATA,
    "paimon.file.count",
    12345.0,
    System.currentTimeMillis(),
    Collections.emptyMap()
);
// MetricEnvelope内部自动填充：
// - metricType = "PAIMON_METADATA"
// - jobName = "paimon-perf-test"
// - appId = "wide_table"
// - metricId = "PAIMON_METADATA_paimon.file.count_1704099600000"
// - etlDt = "2024-01-01"
```

### Q2：如何区分不同测试场景的数据？

**A**：
- 生产数据：`job_name = 'paimon-perf-test'` + `app_id = 'wide_table'`
- 测试数据：`job_name = 'paimon-perf-test-test'` (加test后缀)
- 其他作业：用不同的job_name

### Q3：如果StarRocks查不到数据怎么办？

**A**：排查顺序：
1. 确认Kafka消息已产生（kafka-console-consumer验证）
2. 确认既有Flink链路正常运行（metrics topic → StarRocks）
3. 检查既有Flink作业日志（是否有JSON解析错误）
4. 确认表分区是否正常（etl_dt分区键）

---

## 风险评估

### 低风险 ✅
- Java代码编译通过
- JSON格式变更符合预期
- 既有Flink链路应能正常解析新JSON（字段只增不减）

### 中风险 ⚠️
- 既有Flink链路可能对JSON字段顺序敏感
  - **缓解**：我们按DDL顺序输出字段
- StarRocks表可能有写入权限限制
  - **缓解**：先用测试job_name验证

### 需验证 🔍
- 既有Flink链路是否会因为新字段报错
  - **验证**：观察Flink作业日志
- metric_value和metric_ts的String类型是否影响既有查询
  - **验证**：执行StarRocks查询测试

---

## 下一步行动

### 立即执行（P0）

1. ✅ **已完成**：Java代码调整并编译通过
2. ✅ **已完成**：分析SQL重写
3. ⏳ **待执行**：部署验证（按上述步骤1-5）

### 后续优化（P1-P2）

4. 调整其他测试SQL（30分钟）
5. 更新单元测试断言（30分钟）
6. 创建JSON验证测试（15分钟）

---

## 文档索引

- **ADAPTATION_GUIDE.md**：完整的适配指南（理论+代码示例）
- **ADAPTATION_TODO.md**：待办清单（按优先级分类）
- **ADAPTATION_COMPLETE.md**：本文档（完成总结+验证步骤）

---

**状态**：✅ 核心适配完成，可立即部署验证

**预计部署验证时间**：30分钟

**成功标志**：
1. Kafka消息包含12个字段
2. StarRocks表有测试数据
3. metrics_view可查询
4. 四类指标SQL可运行

**如有问题**：参考"常见问题"章节或查阅ADAPTATION_GUIDE.md

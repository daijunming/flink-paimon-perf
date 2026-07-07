# 真实表结构适配指南

## 概述

本文档说明如何将现有设计适配到真实的 `RDW_ODS_FLINK_METRICS` 表结构。

---

## 一、真实表结构

```sql
CREATE TABLE RDW_ODS_FLINK_METRICS (
  etl_dt date NOT NULL COMMENT "数据时间",
  metric_id varchar(65533) NOT NULL COMMENT "指标主键",
  job_name varchar(65533) NULL COMMENT "作业名称",
  app_id varchar(65533) NULL COMMENT "应用ID",
  job_id varchar(65533) NULL COMMENT "作业ID",
  host_name varchar(65533) NULL COMMENT "主机名",
  container_id varchar(65533) NULL COMMENT "容器ID",
  container_rule varchar(65533) NULL COMMENT "容器角色",
  metric_name varchar(65533) NULL COMMENT "指标名称",
  metric_type varchar(65533) NULL COMMENT "指标类型",  -- 用于区分来源
  metric_value varchar(65533) NULL COMMENT "指标值",    -- varchar非double
  metric_ts varchar(65533) NULL COMMENT "指标时间戳"    -- varchar非bigint
)
ENGINE = OLAP
PRIMARY KEY(etl_dt, metric_id)
PARTITION BY RANGE(etl_dt)
DISTRIBUTED BY HASH(etl_dt, metric_id) BUCKETS 3;
```

---

## 二、字段映射策略

### 原设计 vs 真实表

| 原设计字段 | 真实表字段 | 映射规则 |
|-----------|-----------|---------|
| `source` (枚举) | `metric_type` (varchar) | PAIMON_METADATA / YARN / HDFS |
| - | `job_name` | 固定 "paimon-perf-test" |
| - | `app_id` | 固定 "wide_table" |
| `metricName` | `metric_name` | 直接映射 |
| `metricValue` (double) | `metric_value` (varchar) | String.valueOf(double) |
| `collectTsMillis` (long) | `metric_ts` (varchar) | String.valueOf(long) |
| - | `metric_id` | 生成规则见下文 |
| - | `etl_dt` | 从collectTsMillis提取日期 |
| `tags` (Map) | 多个独立列 | job_id / host_name / container_id |

### metric_id 生成规则

```java
// 保证唯一性：metricType + metricName + 时间戳
String metricId = metricType + "_" + metricName + "_" + collectTsMillis;
// 示例：PAIMON_METADATA_paimon.file.count_1704099600000
```

### etl_dt 提取规则

```java
// 从毫秒时间戳提取日期
SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
String etlDt = sdf.format(new Date(collectTsMillis));
// 示例：2024-01-01
```

---

## 三、Java代码调整清单

### 3.1 MetricEnvelope.java

**位置**：`common/src/main/java/com/paimonperf/common/MetricEnvelope.java`

**修改**：增加字段

```java
public final class MetricEnvelope {
    // 原有字段
    private final MetricSource source;      // 保留，内部使用
    private final String metricName;
    private final double metricValue;
    private final long collectTsMillis;
    private final Map<String, String> tags;
    
    // 新增字段（对齐真实表）
    private final String metricType;        // PAIMON_METADATA / YARN / HDFS
    private final String jobName;           // paimon-perf-test
    private final String appId;             // wide_table
    private final String metricId;          // 主键
    private final String etlDt;             // 分区键
    private final String jobId;             // 可选，默认空
    private final String hostName;          // 可选，默认空
    private final String containerId;       // 可选，默认空
    private final String containerRule;     // 可选，默认空
    
    // 构造函数调整
    public MetricEnvelope(MetricSource source, String metricName, double metricValue,
                          long collectTsMillis, Map<String, String> tags) {
        this.source = source;
        this.metricName = metricName;
        this.metricValue = metricValue;
        this.collectTsMillis = collectTsMillis;
        this.tags = Collections.unmodifiableMap(new LinkedHashMap<>(tags));
        
        // 自动填充新字段
        this.metricType = source.name();  // PAIMON_METADATA / YARN / HDFS
        this.jobName = "paimon-perf-test";
        this.appId = "wide_table";
        this.metricId = generateMetricId();
        this.etlDt = extractEtlDt();
        this.jobId = tags.getOrDefault("job_id", "");
        this.hostName = tags.getOrDefault("host_name", "");
        this.containerId = tags.getOrDefault("container_id", "");
        this.containerRule = tags.getOrDefault("container_rule", "");
    }
    
    private String generateMetricId() {
        return metricType + "_" + metricName + "_" + collectTsMillis;
    }
    
    private String extractEtlDt() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date(collectTsMillis));
    }
    
    // 新增getter方法
    public String getMetricType() { return metricType; }
    public String getJobName() { return jobName; }
    public String getAppId() { return appId; }
    public String getMetricId() { return metricId; }
    public String getEtlDt() { return etlDt; }
    public String getJobId() { return jobId; }
    public String getHostName() { return hostName; }
    public String getContainerId() { return containerId; }
    public String getContainerRule() { return containerRule; }
}
```

### 3.2 MetricEnvelopeSerializer.java

**位置**：`common/src/main/java/com/paimonperf/common/MetricEnvelopeSerializer.java`

**修改**：调整JSON格式

```java
@Override
public byte[] serialize(String topic, MetricEnvelope data) {
    if (data == null) {
        return null;
    }
    try {
        ObjectNode node = MAPPER.createObjectNode();
        
        // 对齐真实表字段
        node.put("etl_dt", data.getEtlDt());
        node.put("metric_id", data.getMetricId());
        node.put("job_name", data.getJobName());
        node.put("app_id", data.getAppId());
        node.put("job_id", data.getJobId());
        node.put("host_name", data.getHostName());
        node.put("container_id", data.getContainerId());
        node.put("container_rule", data.getContainerRule());
        node.put("metric_name", data.getMetricName());
        node.put("metric_type", data.getMetricType());
        node.put("metric_value", String.valueOf(data.getMetricValue()));  // double→String
        node.put("metric_ts", String.valueOf(data.getCollectTsMillis())); // long→String
        
        return MAPPER.writeValueAsBytes(node);
    } catch (Exception e) {
        throw new RuntimeException("MetricEnvelope 序列化失败", e);
    }
}
```

**JSON输出示例**：
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

### 3.3 采集器无需修改

**原因**：
- metadata-collector / resource-collector 都是创建 `MetricEnvelope` 对象
- 新字段由 `MetricEnvelope` 构造函数自动填充
- 采集器代码无需改动

**验证**：
```java
// 采集器现有代码（无需修改）
MetricEnvelope envelope = new MetricEnvelope(
    MetricSource.PAIMON_METADATA,
    "paimon.file.count",
    12345.0,
    System.currentTimeMillis(),
    Collections.emptyMap()
);
// MetricEnvelope内部自动填充 metricType="PAIMON_METADATA" 等字段
```

### 3.4 测试调整

**MetricEnvelopeTest.java**：
- 增加新字段的getter断言
- 验证metricId生成规则
- 验证etlDt提取正确

**MetricEnvelopeSerializerTest.java**：
- 验证JSON包含所有12个字段
- 验证metric_value和metric_ts是String类型

---

## 四、分析SQL调整清单

### 4.1 01_metrics_view.sql（完全重写）

**原设计**（基于假设结构）：
```sql
CREATE VIEW metrics_view AS
SELECT
  source,
  metric_name,
  metric_value,
  FROM_UNIXTIME(collect_ts_millis / 1000) AS time_bucket_minute,
  tags
FROM RDW_ODS_FLINK_METRICS;
```

**新设计**（基于真实表）：
```sql
-- 01_metrics_view.sql —— 指标视图（时段分桶）
-- 基于真实表 RDW_ODS_FLINK_METRICS，字段映射：
--   metric_type → 指标来源（PAIMON_METADATA / YARN / HDFS）
--   metric_ts → 毫秒时间戳字符串，需转换为BIGINT再分桶
--   metric_value → 字符串，需转换为DOUBLE

CREATE OR REPLACE VIEW metrics_view AS
SELECT
  metric_type AS source,                                      -- 来源标识
  metric_name,                                                 -- 指标名
  CAST(metric_value AS DOUBLE) AS metric_value,              -- varchar→double
  CAST(metric_ts AS BIGINT) AS metric_ts_millis,             -- varchar→bigint
  FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
  job_name,
  app_id,
  job_id,
  host_name
FROM RDW_ODS_FLINK_METRICS
WHERE metric_type IN ('PAIMON_METADATA', 'YARN', 'HDFS')      -- 过滤测试相关指标
  AND job_name = 'paimon-perf-test'                           -- 过滤测试作业
  AND app_id = 'wide_table';                                  -- 过滤测试表
```

### 4.2 02_four_category_metrics.sql

**原设计**（基于假设字段）：
```sql
WHERE m.source = 'PAIMON_METADATA'
  AND m.metric_name = 'paimon.file.count'
```

**新设计**（基于metrics_view）：
```sql
-- 02已基于metrics_view，无需修改（view已做字段映射）
-- 但需确认指标名映射关系：

-- 写入性能（7.1）
WHERE m.metric_name = 'numRecordsOut'  -- Flink自动上报，metric_type可能是FLINK_JOB

-- 更新删除效率（7.2）
WHERE m.source = 'PAIMON_METADATA'
  AND m.metric_name = 'paimon.last.commit.kind'

-- 资源Compaction（7.4）
WHERE m.source IN ('YARN', 'HDFS', 'PAIMON_METADATA')
  AND m.metric_name IN ('yarn.allocated.vcores', 'hdfs.capacity.used.bytes', 'paimon.file.count')
```

**关键修改点**：
- `m.source` 现在对应 `metric_type`（已在view中映射）
- 其他SQL（03-05）基于02的结果，无需修改

### 4.3 测试SQL调整

**01_metrics_view_test.sql**：
```sql
-- 插入测试数据（对齐真实表结构）
INSERT INTO RDW_ODS_FLINK_METRICS VALUES
(
  '2024-01-01',                                    -- etl_dt
  'PAIMON_METADATA_paimon.file.count_1704067200000', -- metric_id
  'paimon-perf-test',                              -- job_name
  'wide_table',                                    -- app_id
  '',                                              -- job_id
  '',                                              -- host_name
  '',                                              -- container_id
  '',                                              -- container_rule
  'paimon.file.count',                             -- metric_name
  'PAIMON_METADATA',                               -- metric_type
  '100.0',                                         -- metric_value (String)
  '1704067200000'                                  -- metric_ts (String)
);

-- 验证view映射正确
SELECT
  source,
  metric_name,
  metric_value,
  time_bucket_minute
FROM metrics_view
WHERE source = 'PAIMON_METADATA'
  AND metric_name = 'paimon.file.count';

-- 预期输出：
-- source='PAIMON_METADATA', metric_value=100.0, time_bucket_minute='2024-01-01 00:00:00'
```

---

## 五、编译验证步骤

### 5.1 编译Java组件

```bash
export JAVA_HOME=D:\soft\jdk-8u202
cd d:\ai_workspace\-AI-\flink-paimon-perf

# 编译common模块
mvn -pl common clean compile

# 编译采集器
mvn -pl metadata-collector,resource-collector clean package -DskipTests

# 验证jar包生成
ls -lh metadata-collector/target/metadata-collector.jar
ls -lh resource-collector/target/resource-collector.jar
```

### 5.2 运行测试

```bash
# 单元测试
mvn -pl common test -Dtest=MetricEnvelopeTest
mvn -pl common test -Dtest=MetricEnvelopeSerializerTest

# 集成测试
mvn -pl metadata-collector test -Dtest=MetadataCollectorIntegrationTest
```

### 5.3 验证JSON格式

```java
// 创建测试类验证JSON输出
public class JsonFormatVerification {
    @Test
    void verifyJsonFormat() {
        MetricEnvelope envelope = new MetricEnvelope(
            MetricSource.PAIMON_METADATA,
            "paimon.file.count",
            12345.0,
            1704099600000L,
            Collections.emptyMap()
        );
        
        MetricEnvelopeSerializer serializer = new MetricEnvelopeSerializer();
        byte[] json = serializer.serialize("test", envelope);
        String jsonStr = new String(json);
        
        System.out.println(jsonStr);
        // 预期输出包含12个字段：etl_dt, metric_id, job_name, ...
        
        assertTrue(jsonStr.contains("\"etl_dt\":\"2024-01-01\""));
        assertTrue(jsonStr.contains("\"metric_type\":\"PAIMON_METADATA\""));
        assertTrue(jsonStr.contains("\"metric_value\":\"12345.0\""));  // String类型
        assertTrue(jsonStr.contains("\"metric_ts\":\"1704099600000\""));  // String类型
    }
}
```

---

## 六、部署验证

### 6.1 验证Kafka消息格式

```bash
# 启动采集器
java -jar metadata-collector.jar metadata-collector.properties

# 消费metrics topic验证格式
kafka-console-consumer --bootstrap-server 159.1.41.84:9092 \
  --topic RDW_ODS_FLINK_METRICS_TOPIC \
  --from-beginning --max-messages 5

# 预期输出：
# {"etl_dt":"2024-01-01","metric_id":"PAIMON_METADATA_...","job_name":"paimon-perf-test",...}
```

### 6.2 验证StarRocks表数据

```sql
-- StarRocks
SELECT
  etl_dt,
  metric_type,
  metric_name,
  CAST(metric_value AS DOUBLE) AS value,
  CAST(metric_ts AS BIGINT) AS ts
FROM RDW_ODS_FLINK_METRICS
WHERE job_name = 'paimon-perf-test'
  AND app_id = 'wide_table'
ORDER BY metric_ts DESC
LIMIT 10;

-- 验证点：
-- ✅ metric_type = 'PAIMON_METADATA' / 'YARN' / 'HDFS'
-- ✅ metric_value 可转换为DOUBLE
-- ✅ metric_ts 可转换为BIGINT
-- ✅ etl_dt 格式为 'yyyy-MM-dd'
```

### 6.3 验证分析SQL

```sql
-- 验证metrics_view
SELECT COUNT(*), COUNT(DISTINCT source), COUNT(DISTINCT metric_name)
FROM metrics_view;

-- 验证四类指标
SELECT * FROM four_category_metrics LIMIT 10;

-- 验证SLA判定
SELECT * FROM sla_check WHERE sla_status = 'FAIL' LIMIT 10;
```

---

## 七、待办事项清单

### P0（必须）

- [ ] 修改 `MetricEnvelope.java`（增加字段、构造函数、getter）
- [ ] 修改 `MetricEnvelopeSerializer.java`（JSON格式对齐12个字段）
- [ ] 修改 `01_metrics_view.sql`（字段映射、类型转换）
- [ ] 修改 `01_metrics_view_test.sql`（测试数据格式）
- [ ] 编译验证（mvn clean compile）
- [ ] 运行测试（mvn test）

### P1（强烈建议）

- [ ] 修改 `MetricEnvelopeTest.java`（增加新字段断言）
- [ ] 修改 `MetricEnvelopeSerializerTest.java`（验证JSON格式）
- [ ] 修改 `03/04/05_*_test.sql`（测试数据格式）
- [ ] 创建 `JsonFormatVerification` 测试类
- [ ] 部署验证（Kafka消息格式、StarRocks数据）

### P2（可选）

- [ ] 更新 `DEVELOP.md`（文档同步）
- [ ] 更新 `DELIVERY.md`（字段映射说明）
- [ ] 更新 `REAL_ENV_COMPATIBILITY.md`（验证步骤调整）

---

## 八、常见问题

### Q1：为什么metric_value和metric_ts是varchar？

**A**：这是StarRocks表的设计选择，可能是为了：
- 兼容不同类型的指标值（数值/字符串）
- 避免类型转换错误导致写入失败
- 分析时再根据需要CAST为具体类型

### Q2：如何区分Flink自动上报的指标和采集器指标？

**A**：用 `metric_type` 字段：
- Flink自动上报：`metric_type` 可能是空或 "FLINK_JOB"
- 采集器产出：`metric_type` = "PAIMON_METADATA" / "YARN" / "HDFS"

### Q3：如果采集器写入失败怎么办？

**A**：检查：
1. Kafka topic权限（是否可写）
2. JSON格式是否正确（用kafka-console-consumer验证）
3. 既有Flink链路是否正常运行
4. StarRocks表是否有写入权限

---

## 九、总结

**调整范围**：
- Java代码：2个文件（MetricEnvelope + Serializer）
- 分析SQL：5个文件（01 view + 4个test）
- 测试代码：2个文件（单元测试）

**工作量**：
- 核心修改：2-3小时
- 测试验证：1-2小时
- 部署验证：1小时

**风险**：
- ⚠️ JSON格式变化可能影响既有Flink链路（需验证）
- ⚠️ 字段类型转换可能有精度损失（varchar→double）
- ⚠️ 分析SQL需要真实数据验证（测试SQL只是示例）

---

**状态**：⏳ 待执行（参考本指南逐步完成）

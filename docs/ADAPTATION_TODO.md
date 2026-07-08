# 适配真实表结构 - 后续步骤清单

## 已完成 ✅

### 阶段1：Java代码核心调整（已完成）

- [x] **MetricEnvelope.java** - 增加9个新字段
  - metricType, jobName, appId, metricId, etlDt
  - jobId, hostName, containerId, containerRule
  - 自动生成metricId和etlDt的私有方法
  - 新增9个getter方法
  - 更新equals/hashCode/toString

- [x] **MetricEnvelopeSerializer.java** - JSON格式完全重写
  - 12个字段对齐真实表DDL
  - metric_value: double→String
  - metric_ts: long→String
  - 移除旧的tags嵌套结构

- [x] **编译验证** - common模块编译通过
  - `mvn -pl common clean compile` ✅ BUILD SUCCESS

---

## 待完成工作

### 阶段2：重新编译依赖组件（P0必须）

```bash
# 1. 编译采集器（依赖common模块）
mvn -pl metadata-collector,resource-collector clean package -DskipTests

# 2. 验证jar包生成
ls -lh metadata-collector/target/metadata-collector.jar
ls -lh resource-collector/target/resource-collector.jar
```

**预期**：编译成功，jar包大小与之前类似（~20MB）

---

### 阶段3：调整分析SQL（P0必须）

#### 3.1 重写 01_metrics_view.sql

**文件**：`analysis-sql/01_metrics_view.sql`

**替换内容为**：
```sql
-- 01_metrics_view.sql —— 指标视图（对齐真实表RDW_ODS_FLINK_METRICS）

CREATE OR REPLACE VIEW metrics_view AS
SELECT
  metric_type AS source,                                      -- PAIMON_METADATA / YARN / HDFS
  metric_name,
  CAST(metric_value AS DOUBLE) AS metric_value,              -- varchar→double
  CAST(metric_ts AS BIGINT) AS metric_ts_millis,             -- varchar→bigint
  FROM_UNIXTIME(CAST(metric_ts AS BIGINT) / 1000, '%Y-%m-%d %H:%i:00') AS time_bucket_minute,
  job_name,
  app_id,
  job_id,
  host_name,
  etl_dt
FROM RDW_ODS_FLINK_METRICS
WHERE metric_type IN ('PAIMON_METADATA', 'YARN', 'HDFS')
  AND job_name = 'paimon-perf-test'
  AND app_id = 'wide_table';
```

#### 3.2 调整 01_metrics_view_test.sql

**文件**：`analysis-sql/01_metrics_view_test.sql`

**测试数据格式改为**：
```sql
-- 插入测试数据（对齐真实表12字段）
INSERT INTO RDW_ODS_FLINK_METRICS VALUES
(
  '2024-01-01',                                              -- etl_dt
  'PAIMON_METADATA_paimon.file.count_1704067200000',        -- metric_id
  'paimon-perf-test',                                        -- job_name
  'wide_table',                                              -- app_id
  '',                                                        -- job_id
  '',                                                        -- host_name
  '',                                                        -- container_id
  '',                                                        -- container_rule
  'paimon.file.count',                                       -- metric_name
  'PAIMON_METADATA',                                         -- metric_type
  '100.0',                                                   -- metric_value (String)
  '1704067200000'                                            -- metric_ts (String)
);

-- 验证view
SELECT source, metric_name, metric_value, time_bucket_minute
FROM metrics_view
WHERE source = 'PAIMON_METADATA';
```

#### 3.3 其他测试SQL

- `03_sla_check_test.sql` - 同样调整INSERT格式（12字段）
- `04_baseline_compare_test.sql` - 同样调整INSERT格式
- `05_bottleneck_identify_test.sql` - 同样调整INSERT格式

**模式**：所有INSERT都改为12字段格式，参考01_test.sql

---

### 阶段4：运行测试（P1强烈建议）

```bash
# 1. 运行common模块测试
mvn -pl common test

# 2. 运行采集器集成测试
mvn -pl metadata-collector test -Dtest=MetadataCollectorIntegrationTest
mvn -pl resource-collector test -Dtest=ResourceCollectorIntegrationTest
```

**预期**：部分测试可能失败（因为断言未更新），但编译应该通过

---

### 阶段5：创建JSON格式验证测试（P1强烈建议）

**新建文件**：`common/src/test/java/com/paimonperf/common/JsonFormatVerificationTest.java`

```java
package com.paimonperf.common;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class JsonFormatVerificationTest {

    @Test
    void verifyJsonContains12Fields() {
        MetricEnvelope envelope = new MetricEnvelope(
            MetricSource.PAIMON_METADATA,
            "paimon.file.count",
            12345.0,
            1704099600000L,
            Collections.emptyMap()
        );
        
        MetricEnvelopeSerializer serializer = new MetricEnvelopeSerializer();
        String json = serializer.toJson(envelope);
        
        System.out.println("JSON输出：");
        System.out.println(json);
        
        // 验证12个字段存在
        assertTrue(json.contains("\"etl_dt\":\"2024-01-01\""));
        assertTrue(json.contains("\"metric_id\":\"PAIMON_METADATA_paimon.file.count_1704099600000\""));
        assertTrue(json.contains("\"job_name\":\"paimon-perf-test\""));
        assertTrue(json.contains("\"app_id\":\"wide_table\""));
        assertTrue(json.contains("\"metric_name\":\"paimon.file.count\""));
        assertTrue(json.contains("\"metric_type\":\"PAIMON_METADATA\""));
        assertTrue(json.contains("\"metric_value\":\"12345.0\""));  // String类型
        assertTrue(json.contains("\"metric_ts\":\"1704099600000\""));  // String类型
        
        // 验证类型转换正确
        assertFalse(json.contains("\"metric_value\":12345.0"));  // 不应该是数字
        assertFalse(json.contains("\"metric_ts\":1704099600000"));  // 不应该是数字
    }
}
```

**运行**：
```bash
mvn -pl common test -Dtest=JsonFormatVerificationTest
```

---

### 阶段6：部署验证（P0必须）

#### 6.1 验证Kafka消息格式

```bash
# 1. 启动metadata-collector
java -jar metadata-collector/target/metadata-collector.jar \
  metadata-collector.properties

# 2. 消费metrics topic
kafka-console-consumer --bootstrap-server kafka-broker:9092 \
  --topic RDW_ODS_FLINK_METRICS_TOPIC \
  --from-beginning --max-messages 1

# 3. 预期输出（JSON格式）
# {"etl_dt":"2024-01-01","metric_id":"PAIMON_METADATA_...","job_name":"paimon-perf-test",...}
```

#### 6.2 验证StarRocks表数据

```sql
-- StarRocks查询
SELECT
  etl_dt,
  metric_id,
  metric_type,
  metric_name,
  CAST(metric_value AS DOUBLE) AS value_num,
  CAST(metric_ts AS BIGINT) AS ts_num
FROM RDW_ODS_FLINK_METRICS
WHERE job_name = 'paimon-perf-test'
  AND app_id = 'wide_table'
ORDER BY CAST(metric_ts AS BIGINT) DESC
LIMIT 10;
```

**验证点**：
- ✅ metric_type = 'PAIMON_METADATA' / 'YARN' / 'HDFS'
- ✅ metric_value可转换为DOUBLE
- ✅ metric_ts可转换为BIGINT
- ✅ metric_id格式正确

#### 6.3 验证分析SQL

```sql
-- 验证metrics_view
SELECT COUNT(*), COUNT(DISTINCT source), COUNT(DISTINCT metric_name)
FROM metrics_view;

-- 验证四类指标
SELECT * FROM four_category_metrics
WHERE time_bucket_minute >= '2024-01-01 00:00:00'
LIMIT 10;
```

---

## 优先级说明

**P0（必须立即完成）**：
1. 重新编译采集器（阶段2）
2. 重写01_metrics_view.sql（阶段3.1）
3. 部署验证Kafka消息格式（阶段6.1）

**P1（强烈建议）**：
4. 调整测试SQL（阶段3.2-3.3）
5. 创建JSON验证测试（阶段5）
6. 验证StarRocks数据（阶段6.2）

**P2（可选）**：
7. 更新DEVELOP.md文档
8. 更新所有单元测试断言

---

## 快速验证路径（最小化）

如果时间紧张，按此顺序：

```bash
# 1. 编译采集器
mvn -pl metadata-collector,resource-collector clean package -DskipTests

# 2. 重写01_metrics_view.sql（复制上面的SQL）

# 3. 启动metadata-collector验证JSON格式
java -jar metadata-collector/target/metadata-collector.jar test.properties

# 4. 用kafka-console-consumer看JSON输出
kafka-console-consumer --bootstrap-server ... --topic ... --max-messages 1
```

**如果JSON格式正确**（包含12个字段），说明适配成功！

---

## 常见问题排查

### Q1：编译采集器失败？

**现象**：`mvn -pl metadata-collector compile` 报错

**排查**：
```bash
# 检查common模块是否已安装到本地仓库
mvn -pl common clean install
# 然后重新编译采集器
mvn -pl metadata-collector clean compile
```

### Q2：JSON缺少某些字段？

**现象**：kafka-console-consumer看到的JSON不是12字段

**排查**：
1. 确认MetricEnvelope.java的getter方法是否都正确
2. 确认MetricEnvelopeSerializer.java是否包含全部12个put语句
3. 重新编译并重启采集器

### Q3：StarRocks查不到数据？

**现象**：`SELECT * FROM RDW_ODS_FLINK_METRICS WHERE job_name='paimon-perf-test'` 返回空

**排查**：
1. 确认既有Flink链路是否正常运行（metrics topic → StarRocks）
2. 确认Kafka消息已产生（用console-consumer验证）
3. 检查既有Flink作业日志（是否有解析错误）

---

## 完成标志

当以下都满足时，适配完成：

- [ ] 采集器编译打包成功
- [ ] Kafka消息包含12个字段
- [ ] StarRocks表有数据且metric_type正确
- [ ] metrics_view可查询到数据
- [ ] 四类指标SQL能正常运行

---

**当前进度**：✅ 阶段1完成（Java核心代码），⏳ 阶段2-6待执行

**预计剩余时间**：
- 阶段2-3：1小时（编译+SQL调整）
- 阶段4-5：1小时（测试）
- 阶段6：0.5小时（部署验证）
- 总计：2.5小时

**建议**：先执行P0任务（1小时），验证通过后再做P1/P2。

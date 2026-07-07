# analysis-sql（组件 f：StarRocks 分析 SQL）

StarRocks 分析 SQL 脚本集，针对既有指标表 `RDW_ODS_FLINK_METRICS`（采集器写入的指标统一落表）。

非 Maven 模块，作为资源目录随包搬运、在 StarRocks 上执行。

## 脚本清单

| 文件 | 内容 | 对应任务 |
|------|------|----------|
| `01_metrics_view.sql` | 指标视图与时段分桶（按分钟粒度） | 8.1 |
| `01_metrics_view_test.sql` | 时段分桶逻辑验证（Property 10） | 8.2 |
| `02_four_category_metrics.sql` | 四类指标聚合（写入/更新删除/读取/资源） | 8.3 |
| `03_sla_check.sql` | SLA 达标判定（吞吐≥20000 且 延迟≤180s） | 8.5 |
| `03_sla_check_test.sql` | SLA 判定逻辑验证（Property 7） | 8.6 |
| `04_baseline_compare.sql` | 基线对比（阶段1 vs 阶段2） | 8.7 |
| `04_baseline_compare_test.sql` | 基线对比逻辑验证（Property 8） | 8.8 |
| `05_bottleneck_identify.sql` | 瓶颈定位（RESOURCE/COMPACTION_LAG/COMPACTION/WRITE_CONCURRENCY/NONE） | 8.9 |
| `05_bottleneck_identify_test.sql` | 瓶颈定位逻辑验证（Property 9） | 8.10 |
| `08_checkpoint_health.sql` | 快照推进健康度（snapshot_id 单调递增 / 停滞告警） | 补充 |
| `08_checkpoint_health_test.sql` | 快照停滞检测验证（STALL/SLOW） | 补充 |

## 执行方式

**分析 SQL**（01-05）：在 StarRocks 上创建视图/表，用于日常查询与监控。

```sql
-- 在 StarRocks 客户端依次执行
SOURCE 01_metrics_view.sql;
SOURCE 02_four_category_metrics.sql;
SOURCE 03_sla_check.sql;
SOURCE 04_baseline_compare.sql;
SOURCE 05_bottleneck_identify.sql;
SOURCE 08_checkpoint_health.sql;
```

**测试 SQL**（`_test.sql`）：验证分析逻辑正确性，手动执行后检查断言输出。

```sql
-- 验证时段分桶逻辑
SOURCE 01_metrics_view_test.sql;
-- 预期：三个断言全部 PASS

-- 验证 SLA 判定逻辑
SOURCE 03_sla_check_test.sql;
-- 预期：5 个用例全部 PASS

-- 验证基线对比逻辑
SOURCE 04_baseline_compare_test.sql;
-- 预期：三个断言全部 PASS

-- 验证瓶颈定位逻辑
SOURCE 05_bottleneck_identify_test.sql;
-- 预期：6 个用例全部 PASS

-- 验证快照推进健康度（停滞检测）
SOURCE 08_checkpoint_health_test.sql;
-- 预期：t2 时段 snapshot_id_delta=0 被标记为 STALL
```

## 四类指标说明

### 类别1：写入与入湖性能（Requirements 7.1）
- **数据源**：入湖 Flink 作业 metrics（若 Flink 自动上报 `numRecordsOut` 到 `RDW_ODS_FLINK_METRICS`）
- **指标**：写入吞吐（条/秒）、入湖总耗时、并发写入能力
- **注意**：若 Flink metrics 未自动上报，需编排脚本调 REST API 补采并插入表

### 类别2：更新与删除效率（Requirements 7.2）
- **数据源**：Paimon 元数据采集器（`source='PAIMON_METADATA'`）
- **指标**：commit kind 分布（`paimon.last.commit.kind`：COMPACT=1.0 / APPEND=0.0）
- **说明**：COMPACT 占比高反映 Update/Delete 触发 Compaction 频繁

### 类别3：读取与查询性能（Requirements 7.3）
- **数据源**：读取/点查作业 metrics（本测试暂无专门读作业）
- **指标**：查询延迟、QPS
- **占位**：`metrics_read_query_perf` 视图为空，待后续添加读作业时补全

### 类别4：资源消耗与 Compaction 开销（Requirements 7.4）
- **数据源**：YARN/HDFS 资源采集器（`source='YARN'/'HDFS'`）+ Paimon 元数据（`source='PAIMON_METADATA'`）
- **指标**：YARN CPU/内存利用率、HDFS 存储利用率、Paimon 文件数/Level 分布
- **说明**：小文件数过多（>5000）或 Compaction 频繁（>50%）触发 COMPACTION 瓶颈

## SLA 达标判定（Requirements 3.5 / 8.2）

**阈值**：
- 吞吐 ≥ 20000 条/秒
- 端到端延迟 ≤ 180 秒（3 分钟）

**判定逻辑**：
```sql
CASE
  WHEN throughput_rps >= 20000 AND e2e_latency_sec <= 180 THEN 'PASS'
  ELSE 'FAIL'
END
```

**数据来源**：
- 吞吐：从 Flink metrics `numRecordsOut`（类别1）计算
- 延迟：从延迟探针指标 `ingest.e2e_latency_ms`（任务 5.6，`source='PAIMON_METADATA'`）计算

## 基线对比（Requirements 8.1）

**流程**：
1. 阶段1 完成后，将聚合指标插入 `baseline_metrics` 表：
   ```sql
   INSERT INTO baseline_metrics VALUES
     ('ingest_perf', 'throughput_rps', 25000.0, 'rps'),
     ('resource', 'yarn_allocated_vcores', 50.0, 'cores'),
     ('compaction', 'paimon_file_count', 1000.0, 'count');
   ```

2. 阶段2 运行时，查询 `baseline_compare` 视图实时对比：
   - **绝对差**：`current_value - baseline_value`
   - **比率**：`current_value / baseline_value`（baseline≠0）
   - **优劣趋势**：吞吐类"高为优"、延迟/文件数类"低为优"

## 瓶颈定位（Requirements 8.3）

**瓶颈类别**（按优先级）：
1. **NONE**：SLA 达标，无瓶颈
2. **RESOURCE_CPU**：YARN CPU 利用率 > 80%
3. **COMPACTION**：Paimon 文件数 > 5000 或 Compaction 占比 > 50%
4. **WRITE_CONCURRENCY**：吞吐低但资源/Compaction 正常（可能并发度不足）
5. **READ_QUERY**：延迟高但吞吐正常（本测试暂无读作业，归 COMPACTION）
6. **UNKNOWN**：四类指标均正常但 SLA 仍未达标（需排查网络/Kafka 等外部因素）

**阈值可调**：
- YARN CPU > 80%
- Paimon 文件数 > 5000
- Compaction 占比 > 50%

查询 `bottleneck_identify` 视图，`bottleneck_detail` 字段给出具体数值便于复核。

## 可测性说明

所有核心分析逻辑（时段分桶、SLA 判定、基线对比、瓶颈定位）均配有 `_test.sql`，
插入固定样本数据并断言结果，满足任务要求的"可测函数/固定数据集断言"。

测试 SQL 在 StarRocks 上手动执行即可验证逻辑正确性（本地无法运行 StarRocks，
属性测试标 `*` 表示需真实环境）。

## 依赖前置

- 既有表 `RDW_ODS_FLINK_METRICS`（`source` / `metric_name` / `metric_value` / `collect_ts_millis` / `tags`）
- 元数据采集器（任务 5）与资源采集器（任务 6）已运行并写入指标
- 延迟探针（任务 5.6）已实现并产出 `ingest.e2e_latency_ms` 指标

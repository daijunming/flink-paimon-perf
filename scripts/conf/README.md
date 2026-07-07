# scripts/conf（配置模板目录）

三个 Java 组件（data-generator / metadata-collector / resource-collector）的配置文件模板，
用 `.template` 后缀表示需在部署时填入真实环境值。

## 使用方式

部署前复制模板并替换占位符：

```bash
# 示例：metadata-collector 配置
cp scripts/conf/metadata-collector.properties.template \
   /path/to/deploy/metadata-collector.properties

# 编辑并替换占位符 ${PAIMON_WAREHOUSE} / ${KAFKA_BOOTSTRAP_SERVERS} / ${KAFKA_METRICS_TOPIC}
```

或由编排脚本（任务 9）自动化替换占位符并注入。

## 占位符清单

| 占位符 | 含义 | 用于哪些组件 |
|--------|------|--------------|
| `${KAFKA_BOOTSTRAP_SERVERS}` | Kafka 地址（如 `kafka1:9092,kafka2:9092`） | 全部三个组件 |
| `${KAFKA_TOPIC}` | 测试数据 topic（生成器写入 + 入湖作业读取） | data-generator |
| `${KAFKA_METRICS_TOPIC}` | 既有 metrics topic（`RDW_ODS_FLINK_METRICS_TOPIC`） | metadata-collector, resource-collector |
| `${PAIMON_WAREHOUSE}` | Paimon 仓库 HDFS 路径（如 `hdfs:///warehouse/paimon_perf`） | metadata-collector, 入湖 SQL 脚本 |
| `${YARN_RM_URL}` | YARN ResourceManager 基地址（如 `http://rm-host:8088`） | resource-collector |
| `${HDFS_NN_URL}` | HDFS NameNode 基地址（如 `http://nn-host:9870`） | resource-collector |

## 配置文件清单

| 文件 | 组件 | 说明 |
|------|------|------|
| `data-generator.properties.template` | 数据生成器 | account.total / update.ratio / rate.limit.* / kafka.* |
| `metadata-collector.properties.template` | 元数据采集器 | warehouse / database / table / kafka.* / collect.interval.seconds |
| `resource-collector.properties.template` | 资源采集器 | yarn.rm.url / hdfs.nn.url / kafka.* / collect.interval.seconds |

## 阶段化配置建议

### 阶段1（极限压测）
- **data-generator**：`rate.limit.enabled=false`（不限速探上限）
- **metadata-collector / resource-collector**：`collect.interval.seconds=30`（频繁采样观测峰值）

### 阶段2（生产模拟 SLA 验证）
- **data-generator**：`rate.limit.enabled=true` + `rate.limit.rps=20000`（贴合目标负载）
- **metadata-collector / resource-collector**：`collect.interval.seconds=60`（降低采集开销，配合 5-7 天连跑）

> 入湖作业的阶段化参数在 `scripts/sql/init_phase{1,2}.sql` 中配置。

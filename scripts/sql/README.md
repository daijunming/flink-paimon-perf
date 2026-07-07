# scripts/sql（入湖 Flink SQL 脚本目录）

入湖作业（组件 b）的交付形态：独立 `.sql` 脚本，用 `flink sql-client -i/-f` 提交，不打 jar。

## 脚本清单

| 文件 | 内容 | 提交方式 |
|------|------|----------|
| `01_catalog.sql` | 创建 Paimon Hadoop Catalog + database | `-f`（preflight 一次性） |
| `02_sink_paimon.sql` | 创建 Paimon 100 列主键宽表（+ event_time，deduplicate 主键去重） | `-f`（preflight 一次性） |
| `03_source_kafka.sql` | Kafka source 临时表（100 列 + event_time，**format=canal-json 支持 CDC DELETE**） | `-i`（入湖会话内建临时表） |
| `05_ingest_insert.sql` | 入湖 INSERT（透传全部列 + event_time，**支持 INSERT/UPDATE/DELETE**） | `-f` 主脚本 |
| `06_point_lookup.sql` | **Flink 点查作业**（Lookup Join 模拟实时特征查询，Requirements 7.3） | `-f`（独立作业，可选） |
| `07_olap_scan.sql` | **OLAP 全表扫描作业**（批读聚合模拟 BI 报表，Requirements 7.3） | `-f`（独立作业，可选） |
| `init_phase1.sql` | 阶段1 极限压测参数（SET + 变量） | `-i` |
| `init_phase2.sql` | 阶段2 生产模拟参数（SET + 变量） | `-i` |

> **06/07 为读取作业**，验证 Requirements 7.3（读取与查询性能）；可在阶段2与入湖作业并发运行，观测读写冲突下的性能表现。

## 提交方式

建表（一次性，preflight 阶段）：

```bash
flink sql-client -f scripts/sql/01_catalog.sql
flink sql-client -f scripts/sql/02_sink_paimon.sql
```

> 注意：`02_sink_paimon.sql` 含变量 `${BUCKET_NUM}`，建表时需先经阶段脚本注入，或在 preflight 用相应阶段的 bucket 值执行。

启动入湖（按阶段）：

```bash
flink sql-client \
  -i scripts/sql/init_${PHASE}.sql \    # 阶段参数 + 变量（BUCKET_NUM / SCAN_STARTUP_MODE）
  -i scripts/sql/03_source_kafka.sql \  # 建 Kafka source 临时表（会话级）
  -f scripts/sql/05_ingest_insert.sql   # 执行入湖 INSERT
```

`PHASE` 取 `phase1` 或 `phase2`，由编排脚本（任务 9）的环境变量决定。

## 占位符（运行环境注入，仓库内不填真值）

| 占位符 | 含义 | 注入来源 |
|--------|------|----------|
| `${PAIMON_WAREHOUSE}` | Paimon 仓库 HDFS 路径 | 运行环境 |
| `${KAFKA_BOOTSTRAP_SERVERS}` | Kafka 地址 | 运行环境 |
| `${KAFKA_TOPIC}` | 测试数据 topic | 运行环境 |
| `${BUCKET_NUM}` | Paimon 表 bucket 数 | 阶段脚本（phase1=64 / phase2=16） |
| `${SCAN_STARTUP_MODE}` | Kafka 起始位移 | 阶段脚本（phase1=earliest / phase2=latest） |

## 关键设计决策（与设计文档的对齐说明）

1. **列名/类型严格对齐数据生成器实际输出**（`WideRecord.toJson()`）：
   - `c1_bigint..c20_bigint`（BIGINT）、`c21_decimal..c40_decimal`（DECIMAL(20,4)）、
     `c41_string..c89_string`（STRING）、`c90_ts..c99_ts`（BIGINT）。
   - **时间列与 `event_time` 用 BIGINT（epoch 毫秒），非 TIMESTAMP**——生成器用 `getTime()`
     写毫秒数字，若建成 TIMESTAMP 则 JSON format 解析会失败。这是对设计文档示意（TIMESTAMP）的
     有意偏离，以匹配生成器真实输出。

2. **OGG-JSON 格式支持 DELETE**（Requirements 7.2）：
   - 生成器产出 OGG-JSON 格式：`{"op_type":"I/U/D", "pk":123, ...}`
   - Kafka source 用 `format=ogg-json` 自动解析 op_type 字段：
     - `op_type=I` → INSERT
     - `op_type=U` → UPDATE（主键存在则覆盖）
     - `op_type=D` → DELETE（主键存在则删除）
   - DELETE 记录只含 pk + op_type + event_time，无业务列数据（节省带宽）
   - 配置项 `delete.ratio`（默认 0.1）控制 DELETE 占比，与 `update.ratio` 之和不超过 1.0

3. **去掉 WATERMARK**：入湖是直通 INSERT，不做窗口/事件时间运算，watermark 无用武之地。
   端到端延迟由探针查 `MAX(event_time)` 算 `now - max`（BIGINT 毫秒直接相减），不依赖 watermark。

4. **merge-engine=deduplicate + sequence.field=event_time**：高频更新时同一 pk 按 event_time
   毫秒"新值胜出"，即 LSM 主键去重语义；`changelog-producer=input` 产出变更日志供下游观测。

5. **降写入开销**：`upsert-materialize=NONE` + `not-null-enforcer=DROP`（参考既有 PK 入湖脚本经验）。

6. **读取作业覆盖 Requirements 7.3**：
   - **06_point_lookup.sql**：Flink Lookup Join 模拟实时特征查询（如风控/推荐点查 pk），
     验证主键表点查延迟、并发读写冲突下的查询性能
   - **07_olap_scan.sql**：批读全表聚合模拟 BI 报表（如每 5 分钟统计总记录数/avg/sum），
     验证扫描吞吐、Compaction 对读放大的影响
   - 两者可在阶段2与入湖作业**并发运行**，观测读写相互影响（吞吐下降/延迟上升）

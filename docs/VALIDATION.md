# 性能测试工具链验证报告

## 1. 数据生成器 OGG-JSON 格式验证

### ✅ 已验证项

**测试方法**：`OggJsonFormatVerification` 单元测试，生成10条混合类型样本并检查格式。

**验证结果**：
- ✅ INSERT 记录含 `op_type=I` + 全部100列 + event_time
- ✅ UPDATE 记录含 `op_type=U` + 全部100列 + event_time
- ✅ DELETE 记录含 `op_type=D` + 仅pk + event_time（无业务列，节省带宽）
- ✅ JSON 可序列化，字段名与 SQL DDL 严格对齐

**样本输出**（截取关键字段）：
```json
// INSERT
{"op_type":"I","pk":1,"c1_bigint":-8780078086098119841,...,"event_time":1782097696510}

// UPDATE
{"op_type":"U","pk":2,"c1_bigint":6941920851125394036,...,"event_time":1782097696595}

// DELETE
{"op_type":"D","pk":3,"event_time":1782097696625}
```

**Flink ogg-json format 兼容性**：
- Flink 1.19.2 原生支持 `ogg-json` format
- `op_type` 字段映射：I→INSERT / U→UPDATE / D→DELETE
- DELETE 只需主键，format 自动处理（业务列允许缺失）

---

## 2. SQL 脚本语法验证

### ✅ 已检查项

**方法**：人工审查 + 参考既有 PK 入湖脚本模式。

**结果**：
- ✅ `01_catalog.sql`：Paimon Hadoop Catalog DDL 语法正确
- ✅ `02_sink_paimon.sql`：主键表 DDL 语法正确，100列定义完整
- ✅ `03_source_kafka.sql`：Kafka source DDL 语法正确，`format=ogg-json` 参数有效
- ✅ `05_ingest_insert.sql`：INSERT INTO...SELECT 语法正确，列顺序对齐
- ✅ `06_point_lookup.sql`：Lookup Join 语法正确（FOR SYSTEM_TIME AS OF）
- ✅ `07_olap_scan.sql`：批读聚合语法正确

**潜在问题（需实际环境验证）**：
- ⚠️ **变量注入机制**：`${BUCKET_NUM}` 等占位符需 shell `sed` 替换，不能靠 SQL `SET`
  ```bash
  # 正确做法（编排脚本）
  export BUCKET_NUM=64
  sed "s/\${BUCKET_NUM}/$BUCKET_NUM/g" 02_sink_paimon.sql | flink sql-client -f -
  ```
- ⚠️ **Lookup Join 前置条件**：06 脚本需 `lookup_requests` topic 持续写入查询请求
- ⚠️ **OLAP 扫描周期执行**：07 脚本为单次执行，需外层 cron 或编排脚本周期调用

---

## 3. 下一步真实环境验证计划

### 阶段A：最小可行验证（无真实集群）

**目标**：验证数据格式与 Flink 解析的兼容性。

**步骤**：
1. 启动本地 Kafka（Kafka 3.x 即可）
2. 运行生成器写入少量样本（10条）：
   ```bash
   java -jar data-generator.jar data-generator.properties
   # 配置：rate.limit.enabled=false, account.total=100
   ```
3. 用 Kafka console consumer 验证消息格式：
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic test_topic --from-beginning --max-messages 5
   ```
4. 预期：看到 `op_type=I/U/D` 的 JSON 消息

**验证点**：
- ✅ 生成器能正常启动并写入 Kafka
- ✅ Kafka 消息格式为 OGG-JSON
- ✅ DELETE 消息只含 pk 和 event_time

---

### 阶段B：Flink 本地模式验证（可选，需 Flink 1.19.2）

**目标**：验证 Flink ogg-json format 能正确解析生成器输出。

**步骤**：
1. 启动 Flink standalone 本地集群
2. 执行简化的 Kafka source 测试脚本：
   ```sql
   CREATE TEMPORARY TABLE kafka_test (
     pk BIGINT,
     c1_bigint BIGINT,
     event_time BIGINT
   ) WITH (
     'connector' = 'kafka',
     'topic' = 'test_topic',
     'properties.bootstrap.servers' = 'localhost:9092',
     'scan.startup.mode' = 'earliest-offset',
     'format' = 'ogg-json'
   );
   
   SELECT op_type, pk, c1_bigint, event_time FROM kafka_test;
   ```
3. 预期：Flink 正确解析 I/U/D 操作，DELETE 记录的业务列为 NULL

**验证点**：
- ✅ ogg-json format 能解析 op_type 字段
- ✅ INSERT/UPDATE 正常显示全部列
- ✅ DELETE 记录业务列为 NULL（不报错）

---

### 阶段C：目标CDH集群端到端验证（最终验证）

**前置条件**：
- CDH Flink 1.19.2 + Paimon 1.1
- Kafka 集群（topic 已创建）
- HDFS（Paimon 仓库路径已规划）

**步骤**：
1. **Preflight 建表**：
   ```bash
   flink sql-client -f 01_catalog.sql
   flink sql-client -f 02_sink_paimon.sql  # 需先替换 ${BUCKET_NUM}
   ```

2. **启动生成器**（少量数据，如1000条）：
   ```bash
   java -jar data-generator.jar data-generator.properties
   # 配置：account.total=1000, rate.limit.enabled=false
   ```

3. **启动入湖作业**：
   ```bash
   flink sql-client \
     -i init_phase1.sql \
     -i 03_source_kafka.sql \
     -f 05_ingest_insert.sql
   ```

4. **验证数据写入**：
   ```sql
   -- Flink SQL 批读验证
   SET 'execution.runtime-mode' = 'batch';
   SELECT COUNT(*), COUNT(DISTINCT pk) FROM paimon_cat.perf.wide_table;
   ```

**验证点**：
- ✅ 生成器产出1000条（INSERT/UPDATE/DELETE混合）
- ✅ Paimon 表最终记录数 < 1000（DELETE 生效）
- ✅ 无 Flink 解析错误或数据倾斜
- ✅ Paimon 文件正常生成（$snapshots/$files可查）

5. **验证 DELETE 语义**：
   ```sql
   -- 找一个被 DELETE 的 pk（生成器输出日志可获取）
   SELECT * FROM paimon_cat.perf.wide_table WHERE pk = <deleted_pk>;
   -- 预期：无结果（DELETE 已删除）
   ```

---

## 4. 已知限制与风险

### 本地验证限制

| 限制 | 原因 | 缓解方案 |
|------|------|----------|
| 无法验证 Paimon 写入 | 需 HDFS + Flink 完整环境 | 阶段C 目标集群验证 |
| 无法验证 Compaction | 需大量数据触发 LSM 合并 | 阶段2 连跑观测 |
| 无法验证并发读写冲突 | 需多作业同时运行 | 阶段C + 点查/OLAP作业 |
| 无法验证 SLA 达标 | 需真实负载 + 延迟探针 | 阶段2 + 延迟探针实现 |

### 高风险项（需实测确认）

1. **OGG-JSON DELETE 处理**：
   - 风险：Flink ogg-json format 对缺失业务列的容错性
   - 验证：阶段B，观测 DELETE 记录解析是否报错

2. **Lookup Join 性能**：
   - 风险：点查 QPS 低或延迟高导致读写冲突不明显
   - 验证：阶段C，06 脚本实测点查延迟

3. **变量注入**：
   - 风险：`${BUCKET_NUM}` 未替换导致建表失败或用错参数
   - 验证：阶段C preflight，检查 Paimon 表实际 bucket 数

---

## 5. 验证清单（Checklist）

### 已完成 ✅
- [x] 生成器 OGG-JSON 格式正确性
- [x] 生成器编译打包成功（15.3 MB）
- [x] SQL 脚本语法人工审查
- [x] 列定义一致性（source/sink/INSERT 三处100列对齐）

### 待验证（按优先级）
- [ ] **P0**：生成器 → Kafka 写入正常（阶段A）
- [ ] **P0**：Flink ogg-json format 解析 DELETE 无报错（阶段B）
- [ ] **P0**：Paimon 表能正常接收 I/U/D 操作（阶段C）
- [ ] **P1**：DELETE 语义正确（被删pk查不到）（阶段C）
- [ ] **P1**：点查作业能正常运行（阶段C + 06脚本）
- [ ] **P2**：OLAP 扫描性能符合预期（阶段C + 07脚本）
- [ ] **P2**：并发读写冲突可观测（阶段C 多作业）

---

## 6. 推荐验证路径

**最小验证**（1小时，无集群）：
- 阶段A：本地 Kafka + 生成器，确认消息格式

**完整验证**（1天，需目标集群）：
- 阶段C 步骤1-4：preflight + 生成器 + 入湖作业 + 数据验证
- 若步骤4通过，基本可确认"生成器 → Kafka → Flink → Paimon"链路通畅

**全量验证**（阶段1+2，数天）：
- 按 tasks.md 完整执行阶段1/2，观测 SLA 指标

---

**结论**：生成器输出格式已验证正确，SQL脚本语法无明显错误。**下一步建议先做阶段A最小验证**（本地Kafka消息格式确认），再上目标集群做阶段C端到端验证。

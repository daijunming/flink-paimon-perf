# scripts/meta-collect(Paimon 元数据周期批采集)

用 **crontab 每 3 分钟触发一次有界 Flink SQL Batch 作业**,把 Paimon 系统表的状态采到
Kafka,再由 StarRocks Routine Load 保存为**可回放、可关联、可做时间范围分析的元数据历史**。

> 为什么不是常驻 Streaming SQL:Paimon 1.1 的元表是"一次性有界读取"(
> [System Tables 文档](https://paimon.apache.org/docs/1.1/concepts/system-tables/) 明确
> "access system tables with batch queries"),读出一个确定时点状态后结束;
> 不能指望一个长期运行的 Streaming SQL 持续输出元表变化。

## 分层定位与简化版口径

`rdw_ods_paimon_meta_*` 表保存从 Paimon 系统表周期采集的**原始元数据事实**,用于还原
指定 Paimon 表在不同 Snapshot 或采集时点下的提交、文件组织、存储分布、消费进度和配置状态。
**ODS 层不直接判断表是否健康,不存储"压缩有效""存在倾斜"等结论**——这些判断由分析层
(视图/分析 SQL)计算。ODS 的职责是"保存可复核事实",分析层的职责是"解释事实并形成
监控指标",两者不要混在一起。

本实现是**简化版(趋势观测优先,无本地状态)**:

- `$snapshots`/`$statistics` 每轮**全量重采**未过期部分(retention 限定行数,通常几十行);
- `$files`/`$manifests`/partitions/buckets 每轮采集**当前最新 Snapshot**,
  `source_snapshot_id` 由作业内 `MAX(snapshot_id)` 打标;
- SR 全部 PRIMARY KEY 表,重复采集 = 主键覆盖,**天然幂等**;毫秒级的跨源误标下一轮自愈;
- 因此**没有本地游标、没有 HDFS 回读、没有重放流程**——`collect_once.sh` 只做
  "渲染模板 → 提交 2 个批作业 → 记 runs"。

取舍:每轮固定 2 个小型 YARN 批作业;`collect_runs` 不回填起止 snapshot_id
(可经 `collector_run_id` 关联 snapshots 表);文件级历史粒度 = 轮次(3 分钟),
不是逐 Snapshot 精确历史。若日后需要审计级精度,再演进到"游标 + hint 固定"版本,
SR 表结构无需变动。

与 Java metadata-collector(组件 c)的关系:**并存**。组件 c 产出聚合指标
(`paimon.*` → `RDW_ODS_FLINK_METRICS`,是 analysis-sql 现有视图的底座);
本目录产出行级元数据历史(独立 topic、独立 SR 表),回答聚合指标答不了的问题——
每次 compact 是否真实替换文件、L0/Level 如何变化、三个 Bucket 是否倾斜、
有效文件大小分布、Consumer 是否滞后、哪些 compact 是 no-op。

## 链路

```
crontab(*/3分钟)
  └─ bin/collect_once.sh(flock 防堆积)
       ├─ 10_collect_main      → Kafka:$snapshots/$statistics 全量重采
       │                          + $files/$manifests 当前态(作业内 max 打标)
       │                          + partitions/buckets 由 $files 聚合(单作业 6 条 INSERT)
       ├─ 20_collect_sampling   → Kafka:$consumers + $options 按时间采样
       └─ collect_runs 记录     → Kafka(kafka-console-producer)
                                     ↓
                        StarRocks Routine Load(03_routine_load.sql)
                                     ↓
              RDW_DATA: rdw_ods_paimon_meta_*(9 张 PRIMARY KEY 表)
                                     ↓
              分析层视图(02):level_stats / file_size_stats
```

**topic 名与 SR 表名一一对应**(`rdw_ods_paimon_meta_*`),按元表类型分、不按业务表分;
消息中带 catalog/database/table 标识,新增被监控表无需新建 topic 和 SR 表。

## ODS 表四类职责

| 类别 | 包含表 | 主要回答的问题 |
|------|--------|----------------|
| 提交历史事实 | `snapshots`、`statistics` | Paimon 表在什么时间提交了什么变化? |
| 存储组织事实 | `files`、`manifests`、`partitions`、`buckets` | 当前数据由哪些文件组成,分布在哪些 Bucket 和 Level,文件组织如何变化? |
| 消费运行状态 | `consumers` | 各消费作业消费到哪里,是否出现滞后或停滞? |
| 配置与采集依据 | `options`、`collect_runs` | 表使用什么配置,采集结果是否完整可信? |

各表详细职责见 `sr/01_ods_tables.sql` 的表级 COMMENT。

## 目录

```
scripts/meta-collect/
├─ sr/
│  ├─ 01_ods_tables.sql               # 9 张 PRIMARY KEY ODS 表(先执行)
│  ├─ 02_analysis_views.sql           # 分析层视图(查询时计算,不落存储)
│  └─ 03_routine_load.sql             # 9 条 Routine Load(执行前替换 ${...})
├─ flink-sql/
│  ├─ 10_collect_main.sql.tpl         # 主采集(单作业 6 条 INSERT)
│  └─ 20_collect_sampling.sql.tpl     # consumers + options 按时间采样
├─ conf/meta-collect.properties.template  # 配置模板(占位符见 scripts/README.md 约定)
├─ bin/collect_once.sh                # 单轮编排:渲染模板 → 提交 → 记 runs
└─ README.md(本文件)
```

## 部署步骤(离线集群)

1. StarRocks 侧:`SOURCE sr/01_ods_tables.sql;` → `SOURCE sr/02_analysis_views.sql;`
   → 替换占位符后执行 `sr/03_routine_load.sql`(若走既有 Flink 入库链路则跳过,按列名映射)。
2. Kafka 侧:建 9 个 topic(`rdw_ods_paimon_meta_*`),分区数 3 即可,数据量很小。
3. 复制 `conf/meta-collect.properties.template` 为 `meta-collect.properties` 并填值。
4. 冒烟一轮:`bash bin/collect_once.sh /path/to/meta-collect.properties`,
   看日志、SR 表行数、`SHOW ROUTINE LOAD` 状态。
5. 挂 crontab:`*/3 * * * * bash .../bin/collect_once.sh /path/to/meta-collect.properties >> .../meta-collect.log 2>&1`

## 幂等与历史语义

- **幂等**:SR 全部 PRIMARY KEY 表。每轮全量重采,重复采集/重叠轮次/Routine Load 重发
  全部表现为主键覆盖,无需任何去重或游标。
- **历史**:已过期 Snapshot 的行在 SR 中保留(Paimon 侧过期不影响已入库事实),
  形成超出 `snapshot.time-retained` 的长历史;`$files` 类当前态按 `source_snapshot_id`
  逐轮累积,相邻轮次差分即文件级变化。
- **误标自愈**:文件类记录的 `source_snapshot_id` 与真实文件状态存在毫秒级跨源偏差窗口;
  即便发生,下一轮按正确 `source_snapshot_id` 主键覆盖即修正。
- **重放不需要**:没有游标,任何一轮失败只需等下一轮——本来就是全量重采。

## 已知限制

- 文件级历史粒度 = 轮次(3 分钟),非逐 Snapshot;采样周期 < compact 周期(5 分钟),
  相邻轮次差分即可定位 compact 行为。
- `wide_table` 为非分区表,`partition_value` 固定空串,`rdw_ods_paimon_meta_partitions`
  恒单行(无分析价值,为统一多表口径保留);接入分区表需扩展 10_collect_main 的
  `partition` 列逻辑(GROUP BY `partition` 展开)。
- `$statistics` 的 `colstat` 为复合类型,第一阶段不采集;Paimon 未产出统计的表该 topic 为空。
- SR 主键字节数限制:files 表主键用 `file_path_md5`(CHAR(32))代理——file_path 含目录、
  长度不定,直接进主键装载时超限被整批过滤(2026-08-11 现场发生)。主键无法 ALTER,
  变更需 DROP 重建该表并重建其 Routine Load。
- Paimon 1.1 的 `$files` **没有 `file_source` 列**(后续版本才有),勿在采集 SQL 中引用。
- 环境前提:sql-client 能提交 batch 作业(或平台支持 batch SQL);cron 用户持有有效
  Kerberos ticket。每轮 2 个小型 YARN 批作业(单作业 10-30s 量级),
  若 3 分钟周期太紧,放宽到 5 分钟与 compact 周期对齐也不丢信息。

## 分析示例(分析层口径,ODS 只提供事实)

```sql
-- 每次 COMPACT 是否真实产生提交、增量多大
SELECT snapshot_id, commit_kind, commit_time, delta_record_count, changelog_record_count
FROM rdw_ods_paimon_meta_snapshots
WHERE table_name='wide_table' AND commit_kind='COMPACT' ORDER BY snapshot_id DESC LIMIT 20;

-- Bucket 倾斜事实(ODS 表直接查;"是否倾斜"的判断在分析层)
SELECT source_snapshot_id, bucket, file_count, record_count, file_size_in_bytes
FROM rdw_ods_paimon_meta_buckets
WHERE table_name='wide_table' ORDER BY source_snapshot_id DESC, bucket;

-- 文件大小分布 / Level 变化(分析层视图)
SELECT * FROM v_paimon_meta_file_size_stats WHERE table_name='wide_table'
ORDER BY source_snapshot_id DESC LIMIT 10;

-- Consumer 滞后(latest 用快照表最大值;是否算"停滞"由分析层定阈值)
SELECT c.collected_at, c.consumer_id, c.next_snapshot_id,
       (SELECT MAX(snapshot_id) FROM rdw_ods_paimon_meta_snapshots s
         WHERE s.table_name=c.table_name AND s.commit_time<=c.collected_at) - c.next_snapshot_id AS snapshot_lag
FROM rdw_ods_paimon_meta_consumers c WHERE c.table_name='wide_table'
ORDER BY c.collected_at DESC LIMIT 50;

-- 采集链路自检:最近 24h 是否有 FAILED 轮次 / 长时间无记录
SELECT status, COUNT(*) FROM rdw_ods_paimon_meta_collect_runs
WHERE start_time >= NOW() - INTERVAL 1 DAY GROUP BY status;
```

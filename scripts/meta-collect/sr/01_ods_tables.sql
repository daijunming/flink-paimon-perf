-- 01_ods_tables.sql —— Paimon 元数据 ODS 层表(StarRocks 侧)
--
-- 定位:rdw_ods_paimon_meta_* 表保存从 Paimon 系统表周期采集的原始元数据事实,
-- 用于还原指定 Paimon 表在不同 Snapshot 或采集时点下的提交、文件组织、存储分布、
-- 消费进度和配置状态。ODS 层不直接判断表是否健康,不存储"压缩有效""存在倾斜"等结论,
-- 这些判断由分析层(视图/分析 SQL)计算。ODS 的职责是"保存可复核事实",
-- 分析层的职责是"解释事实并形成监控指标",两者不要混在一起。
--
-- 执行方式:StarRocks 客户端一次性执行(在建 Routine Load 之前)。
--   SOURCE 01_ods_tables.sql;
-- 注:默认落入 RDW_DATA 库;若现场数仓另有 ODS 库分层规划,改 USE 即可。
--
-- 设计要点:
--   * 全部 PRIMARY KEY 模型:同一主键重复装载即覆盖。采集侧为无状态简化版——
--     每轮全量重采,重复/重发/重叠轮次全部靠主键覆盖兜底,天然幂等。
--   * 主键均含 catalog/database/table:多张被监控表共用同一组表,新增被监控表无需建新表。
--   * 源元表字段保持原名(复合类型除外,见各表注释);额外增加采集上下文字段
--     collector_run_id / collected_at / source_snapshot_id。
--   * 时间列统一 DATETIME:采集侧已用 DATE_FORMAT 渲染成 'yyyy-MM-dd HH:mm:ss' 字符串,
--     装载时由 StarRocks 隐式转换,兼容无毫秒精度的老版本。
--
-- 四类职责划分:
--   提交历史事实:  snapshots, statistics
--   存储组织事实:  files, manifests, partitions, buckets
--   消费运行状态:  consumers
--   配置与采集依据: options, collect_runs

USE RDW_DATA;

-- ==================== 提交历史事实 ====================

-- 1) Snapshot 历史:监控 Paimon 表每次提交产生的 Snapshot 记录。
--    用于还原写入提交历史,识别 APPEND/COMPACT/OVERWRITE 何时发生,
--    分析数据增长速度、提交频率、Compaction 是否产生实际提交、Changelog 是否生成。
--    每轮全量重采未过期快照;已过期的行在本表中保留,形成超出 Paimon retention 的长历史。
CREATE TABLE IF NOT EXISTS rdw_ods_paimon_meta_snapshots (
  catalog_name              VARCHAR(128)  NOT NULL COMMENT 'Paimon catalog 名',
  database_name             VARCHAR(128)  NOT NULL COMMENT 'Paimon database 名',
  table_name                VARCHAR(128)  NOT NULL COMMENT 'Paimon 表名',
  snapshot_id               BIGINT        NOT NULL COMMENT 'Paimon Snapshot ID',
  collector_run_id          VARCHAR(64)   NULL     COMMENT '采集批次号(最近一次覆盖写入的批次)',
  collected_at              DATETIME      NULL     COMMENT '本条记录最近一次采集时间',
  schema_id                 BIGINT        NULL,
  commit_user               VARCHAR(128)  NULL,
  commit_identifier         BIGINT        NULL,
  commit_kind               VARCHAR(32)   NULL     COMMENT 'APPEND / COMPACT / OVERWRITE / ...',
  commit_time               DATETIME      NULL,
  base_manifest_list        VARCHAR(1024) NULL,
  delta_manifest_list       VARCHAR(1024) NULL,
  changelog_manifest_list   VARCHAR(1024) NULL,
  total_record_count        BIGINT        NULL,
  delta_record_count        BIGINT        NULL,
  changelog_record_count    BIGINT        NULL,
  watermark                 BIGINT        NULL
) PRIMARY KEY(catalog_name, database_name, table_name, snapshot_id)
COMMENT 'Paimon $snapshots 历史(每轮全量重采未过期部分)'
DISTRIBUTED BY HASH(table_name, snapshot_id) BUCKETS 3
PROPERTIES ("replication_num" = "3");  -- 按集群 BE 规模调整

-- 2) Snapshot 维度的表统计信息(Paimon 提交时产出;未产出统计的表该表为空,属正常)。
--    不记录 Flink 作业吞吐/Checkpoint 耗时/Compaction 耗时,不能独立作为读写性能指标。
--    colstat 为复合类型,第一阶段不采集。
CREATE TABLE IF NOT EXISTS rdw_ods_paimon_meta_statistics (
  catalog_name              VARCHAR(128)  NOT NULL,
  database_name             VARCHAR(128)  NOT NULL,
  table_name                VARCHAR(128)  NOT NULL,
  snapshot_id               BIGINT        NOT NULL,
  collector_run_id          VARCHAR(64)   NULL,
  collected_at              DATETIME      NULL,
  schema_id                 BIGINT        NULL,
  merged_record_count       BIGINT        NULL COMMENT '源列 mergedRecordCount',
  merged_record_size        BIGINT        NULL COMMENT '源列 mergedRecordSize'
) PRIMARY KEY(catalog_name, database_name, table_name, snapshot_id)
COMMENT 'Paimon $statistics 历史(随 $snapshots 每轮全量重采)'
DISTRIBUTED BY HASH(table_name, snapshot_id) BUCKETS 3
PROPERTIES ("replication_num" = "3");

-- ==================== 存储组织事实 ====================

-- 3) 文件明细:监控各 Snapshot 当前引用的数据文件。
--    用于分析有效数据规模、小文件数量、L0 积压、LSM 层级分布、Bucket 间文件分布,
--    以及 Compaction 前后哪些旧文件被替换、哪些新文件生成。
--    说明:wide_table 为非分区表,partition_value 固定空串;接入分区表时需扩展采集 SQL。
--    source_snapshot_id 由采集作业内 MAX(snapshot_id) 打标,极小概率跨源误标,
--    下一轮按正确 snapshot_id 主键覆盖即自愈。
--    主键用 file_path_md5 而非 file_path(2026-08-11 现场踩坑):SR 限制主键总字节数
--    (按声明或按内容计,因版本而异),file_path 含 bucket 目录、长度不定,直接进主键
--    装载时超限被整批过滤;MD5 定长 32 字节且确定,幂等覆盖语义不变,
--    file_path 保留为普通列供复核。名称列声明同步收紧(16/24/32),
--    保证按声明长度计时也不超限。
CREATE TABLE IF NOT EXISTS rdw_ods_paimon_meta_files (
  catalog_name              VARCHAR(16)   NOT NULL,
  database_name             VARCHAR(24)   NOT NULL,
  table_name                VARCHAR(32)   NOT NULL,
  source_snapshot_id        BIGINT        NOT NULL COMMENT '本行状态所属的 Snapshot ID',
  file_path_md5             CHAR(32)      NOT NULL COMMENT 'file_path 的 MD5(hex),代理主键',
  file_path                 VARCHAR(512)  NULL     COMMENT 'Paimon 数据文件相对路径(含 bucket 目录)',
  collector_run_id          VARCHAR(64)   NULL,
  collected_at              DATETIME      NULL,
  partition_value           VARCHAR(512)  NULL     COMMENT '分区值(非分区表为空串)',
  bucket                    INT           NULL,
  file_format               VARCHAR(32)   NULL,
  schema_id                 BIGINT        NULL,
  level                     INT           NULL     COMMENT 'LSM Level',
  record_count              BIGINT        NULL,
  file_size_in_bytes        BIGINT        NULL,
  min_sequence_number       BIGINT        NULL,
  max_sequence_number       BIGINT        NULL,
  creation_time             DATETIME      NULL
) PRIMARY KEY(catalog_name, database_name, table_name, source_snapshot_id, file_path_md5)
COMMENT 'Paimon $files 当前态(每轮采集最新 Snapshot)'
DISTRIBUTED BY HASH(table_name, source_snapshot_id) BUCKETS 3
PROPERTIES ("replication_num" = "3");

-- 4) Manifest 明细:监控各 Snapshot 引用的 Manifest 文件及其文件变更统计。
--    用于分析元数据规模是否持续增长、一次提交实际增删了多少文件、元数据扫描开销。
CREATE TABLE IF NOT EXISTS rdw_ods_paimon_meta_manifests (
  catalog_name              VARCHAR(128)  NOT NULL,
  database_name             VARCHAR(128)  NOT NULL,
  table_name                VARCHAR(128)  NOT NULL,
  source_snapshot_id        BIGINT        NOT NULL,
  file_name                 VARCHAR(256)  NOT NULL COMMENT 'manifest 文件名',
  collector_run_id          VARCHAR(64)   NULL,
  collected_at              DATETIME      NULL,
  file_size                 BIGINT        NULL,
  num_added_files           INT           NULL,
  num_deleted_files         INT           NULL,
  schema_id                 BIGINT        NULL
) PRIMARY KEY(catalog_name, database_name, table_name, source_snapshot_id, file_name)
COMMENT 'Paimon $manifests 当前态(每轮采集最新 Snapshot)'
DISTRIBUTED BY HASH(table_name, source_snapshot_id) BUCKETS 3
PROPERTIES ("replication_num" = "3");

-- 5) 分区汇总:监控各分区在采集时点的汇总状态(由当前 $files 聚合生成,
--    与 files/manifests 同一时间边界;非分区表恒为单行 partition_value='',
--    对当前无分区表没有实际分析价值,为统一多表口径保留)。
CREATE TABLE IF NOT EXISTS rdw_ods_paimon_meta_partitions (
  catalog_name              VARCHAR(128)  NOT NULL,
  database_name             VARCHAR(128)  NOT NULL,
  table_name                VARCHAR(128)  NOT NULL,
  source_snapshot_id        BIGINT        NOT NULL,
  partition_value           VARCHAR(512)  NOT NULL COMMENT '分区值(非分区表为空串)',
  collector_run_id          VARCHAR(64)   NULL,
  collected_at              DATETIME      NULL,
  record_count              BIGINT        NULL,
  file_count                BIGINT        NULL,
  file_size_in_bytes        BIGINT        NULL,
  last_update_time          DATETIME      NULL COMMENT '分区内最新文件创建时间'
) PRIMARY KEY(catalog_name, database_name, table_name, source_snapshot_id, partition_value)
COMMENT '分区汇总(由 $files 当前态聚合)'
DISTRIBUTED BY HASH(table_name, source_snapshot_id) BUCKETS 3
PROPERTIES ("replication_num" = "3");

-- 6) Bucket 汇总:监控每个分区下各 Bucket 的数据和文件汇总状态(同由 $files 聚合)。
--    用于比较 Bucket 间数据量/文件量,识别 Hash 分布不均、Bucket 倾斜、
--    单 Bucket 过大、Compaction 并行度受限等问题。
CREATE TABLE IF NOT EXISTS rdw_ods_paimon_meta_buckets (
  catalog_name              VARCHAR(128)  NOT NULL,
  database_name             VARCHAR(128)  NOT NULL,
  table_name                VARCHAR(128)  NOT NULL,
  source_snapshot_id        BIGINT        NOT NULL,
  partition_value           VARCHAR(512)  NOT NULL,
  bucket                    INT           NOT NULL,
  collector_run_id          VARCHAR(64)   NULL,
  collected_at              DATETIME      NULL,
  record_count              BIGINT        NULL,
  file_count                BIGINT        NULL,
  file_size_in_bytes        BIGINT        NULL,
  last_update_time          DATETIME      NULL
) PRIMARY KEY(catalog_name, database_name, table_name, source_snapshot_id, partition_value, bucket)
COMMENT 'Bucket 汇总(由 $files 当前态聚合)'
DISTRIBUTED BY HASH(table_name, source_snapshot_id) BUCKETS 3
PROPERTIES ("replication_num" = "3");

-- ==================== 消费运行状态 ====================

-- 7) Consumer 进度:监控各 Paimon Consumer 的消费进度(按采集时间采样)。
--    用于跟踪消费到哪个 Snapshot、计算 Snapshot 滞后和时间滞后、识别长期停滞的
--    Consumer、判断某 Consumer 是否可能阻止历史 Snapshot 和文件清理。
CREATE TABLE IF NOT EXISTS rdw_ods_paimon_meta_consumers (
  catalog_name              VARCHAR(128)  NOT NULL,
  database_name             VARCHAR(128)  NOT NULL,
  table_name                VARCHAR(128)  NOT NULL,
  collected_at              DATETIME      NOT NULL COMMENT '采样时间',
  consumer_id               VARCHAR(128)  NOT NULL,
  collector_run_id          VARCHAR(64)   NULL,
  next_snapshot_id          BIGINT        NULL     COMMENT '该 consumer 待消费的下一个 Snapshot'
) PRIMARY KEY(catalog_name, database_name, table_name, collected_at, consumer_id)
COMMENT 'Paimon $consumers 按时间采样'
DISTRIBUTED BY HASH(table_name) BUCKETS 3
PROPERTIES ("replication_num" = "3");

-- ==================== 配置与采集依据 ====================

-- 8) 表属性:记录 Paimon 表显式配置(DDL WITH 项)及其变化(每轮采样,变化可溯源)。
--    不代表所有默认配置均已落地($options 只含显式配置项)。
CREATE TABLE IF NOT EXISTS rdw_ods_paimon_meta_options (
  catalog_name              VARCHAR(128)  NOT NULL,
  database_name             VARCHAR(128)  NOT NULL,
  table_name                VARCHAR(128)  NOT NULL,
  collected_at              DATETIME      NOT NULL COMMENT '采样时间',
  option_key                VARCHAR(256)  NOT NULL COMMENT '源列 key(改名避免保留字)',
  option_value              VARCHAR(4096) NULL     COMMENT '源列 value',
  collector_run_id          VARCHAR(64)   NULL
) PRIMARY KEY(catalog_name, database_name, table_name, collected_at, option_key)
COMMENT 'Paimon $options 按时间采样'
DISTRIBUTED BY HASH(table_name) BUCKETS 3
PROPERTIES ("replication_num" = "3");

-- 9) 采集运行记录:监控每次周期采集任务自身的执行结果。
--    用于区分"Paimon 表没有变化"和"采集任务失败",检查 crontab 漏执行、
--    Flink SQL 失败、Kafka 未写入,并作为所有元数据记录的采集批次依据。
CREATE TABLE IF NOT EXISTS rdw_ods_paimon_meta_collect_runs (
  collector_run_id          VARCHAR(64)   NOT NULL COMMENT '采集批次号(yyyyMMddHHmmss-pid)',
  catalog_name              VARCHAR(128)  NULL,
  database_name             VARCHAR(128)  NULL,
  table_name                VARCHAR(128)  NULL,
  scheduled_time            DATETIME      NULL COMMENT '计划时间(cron 触发时间≈计划时间)',
  start_time                DATETIME      NULL,
  end_time                  DATETIME      NULL,
  previous_snapshot_id      BIGINT        NULL COMMENT '简化版不回填,恒 NULL(可经 collector_run_id 关联 snapshots 表)',
  latest_snapshot_id        BIGINT        NULL COMMENT '简化版不回填,恒 NULL(可经 collector_run_id 关联 snapshots 表)',
  output_record_count       BIGINT        NULL COMMENT '第一阶段未回填,恒 NULL',
  status                    VARCHAR(32)   NULL COMMENT 'OK / FAILED_MAIN_JOB',
  error_message             VARCHAR(4096) NULL
) PRIMARY KEY(collector_run_id)
COMMENT '元数据采集批次运行记录'
DISTRIBUTED BY HASH(collector_run_id) BUCKETS 3
PROPERTIES ("replication_num" = "3");

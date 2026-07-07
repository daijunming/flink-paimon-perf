-- 02_sink_paimon.sql —— 创建 Paimon 100 列主键宽表
-- 基于真实环境：paimon_obs.paimon_database.wide_table
-- 列名/类型严格对齐数据生成器 WideRecord.toJson() 的实际输出：
--   pk                     BIGINT          主键
--   c1_bigint..c20_bigint  BIGINT          20 列
--   c21_decimal..c40_decimal DECIMAL(20,4) 20 列（生成器写 JSON 数字，format 自动转 DECIMAL）
--   c41_string..c89_string STRING          49 列
--   c90_ts..c99_ts         BIGINT          10 列（生成器写 epoch 毫秒，故为 BIGINT 非 TIMESTAMP）
--   event_time             BIGINT          端到端延迟源时间锚（epoch 毫秒）
-- 合计 1+20+20+49+10 = 100 列业务字段 + event_time。
--
-- merge-engine=deduplicate + sequence.field=event_time：高频更新时按 event_time 毫秒"新值胜出"，
-- 正是 LSM 主键去重所需语义（同一 pk 的较新 update 覆盖旧值）。
-- bucket 由阶段初始化脚本注入变量 ${BUCKET_NUM}（阶段1 探上限取大、阶段2 贴合目标）。
-- 提交方式：preflight 阶段一次性执行。

CREATE TABLE IF NOT EXISTS paimon_obs.paimon_database.wide_table (
  pk BIGINT,
  c1_bigint BIGINT, c2_bigint BIGINT, c3_bigint BIGINT, c4_bigint BIGINT, c5_bigint BIGINT,
  c6_bigint BIGINT, c7_bigint BIGINT, c8_bigint BIGINT, c9_bigint BIGINT, c10_bigint BIGINT,
  c11_bigint BIGINT, c12_bigint BIGINT, c13_bigint BIGINT, c14_bigint BIGINT, c15_bigint BIGINT,
  c16_bigint BIGINT, c17_bigint BIGINT, c18_bigint BIGINT, c19_bigint BIGINT, c20_bigint BIGINT,
  c21_decimal DECIMAL(20,4), c22_decimal DECIMAL(20,4), c23_decimal DECIMAL(20,4), c24_decimal DECIMAL(20,4),
  c25_decimal DECIMAL(20,4), c26_decimal DECIMAL(20,4), c27_decimal DECIMAL(20,4), c28_decimal DECIMAL(20,4),
  c29_decimal DECIMAL(20,4), c30_decimal DECIMAL(20,4), c31_decimal DECIMAL(20,4), c32_decimal DECIMAL(20,4),
  c33_decimal DECIMAL(20,4), c34_decimal DECIMAL(20,4), c35_decimal DECIMAL(20,4), c36_decimal DECIMAL(20,4),
  c37_decimal DECIMAL(20,4), c38_decimal DECIMAL(20,4), c39_decimal DECIMAL(20,4), c40_decimal DECIMAL(20,4),
  c41_string STRING, c42_string STRING, c43_string STRING, c44_string STRING, c45_string STRING,
  c46_string STRING, c47_string STRING, c48_string STRING, c49_string STRING, c50_string STRING,
  c51_string STRING, c52_string STRING, c53_string STRING, c54_string STRING, c55_string STRING,
  c56_string STRING, c57_string STRING, c58_string STRING, c59_string STRING, c60_string STRING,
  c61_string STRING, c62_string STRING, c63_string STRING, c64_string STRING, c65_string STRING,
  c66_string STRING, c67_string STRING, c68_string STRING, c69_string STRING, c70_string STRING,
  c71_string STRING, c72_string STRING, c73_string STRING, c74_string STRING, c75_string STRING,
  c76_string STRING, c77_string STRING, c78_string STRING, c79_string STRING, c80_string STRING,
  c81_string STRING, c82_string STRING, c83_string STRING, c84_string STRING, c85_string STRING,
  c86_string STRING, c87_string STRING, c88_string STRING, c89_string STRING,
  c90_ts BIGINT, c91_ts BIGINT, c92_ts BIGINT, c93_ts BIGINT, c94_ts BIGINT,
  c95_ts BIGINT, c96_ts BIGINT, c97_ts BIGINT, c98_ts BIGINT, c99_ts BIGINT,
  event_time BIGINT,
  PRIMARY KEY (pk) NOT ENFORCED
) WITH (
  'bucket' = '${BUCKET_NUM}',
  'merge-engine' = 'deduplicate',
  'sequence.field' = 'event_time',
  'changelog-producer' = 'input',
  'snapshot.num-retained.min' = '10',

  -- Compaction 内存控制：防止 rewrite 阶段 OOM
  -- 1. 允许 write buffer 溢写磁盘（最重要，避免纯内存积压）
  'write-buffer-spillable' = 'true',
  -- 2. 减小 write buffer 大小（默认 256MB，降到 64MB 约束单 TM 内存占用）
  'write-buffer-size' = '67108864',
  -- 3. 限制单次 compaction 合并的文件数（默认 50，减少单次 rewrite 内存峰值）
  'num-sorted-run.compaction-trigger' = '3',
  'write.merge-max-file-num' = '6'
);

-- 01_catalog.sql —— 创建 Paimon Hadoop Catalog
-- 基于真实环境配置：catalog=paimon_obs, warehouse=hdfs:///user/rtp_stream_usr/paimon
-- 提交方式：preflight 阶段一次性执行（与 02_sink_paimon.sql 一起建表）。

CREATE CATALOG IF NOT EXISTS paimon_obs WITH (
  'type' = 'paimon',
  'warehouse' = 'hdfs:///user/rtp_stream_usr/paimon'
);

CREATE DATABASE IF NOT EXISTS paimon_obs.paimon_database;

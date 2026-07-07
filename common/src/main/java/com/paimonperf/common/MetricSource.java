package com.paimonperf.common;

/**
 * 指标来源枚举，对齐既有 RDW_ODS_FLINK_METRICS 表的 source 字段。
 * <p>
 * 需求来源：Requirements 6.3（source 非空且属枚举值）、6.4（collectTsMillis 为合法时间戳）、
 * Design 中 MetricEnvelope 定义（5.1节"统一指标信封"）。
 */
public enum MetricSource {
    /**
     * Paimon 元数据指标（文件数、快照、Level 大小、compaction 信息）
     */
    PAIMON_METADATA,

    /**
     * YARN 资源指标（CPU/内存使用率、队列）
     */
    YARN,

    /**
     * HDFS 资源指标（磁盘用量、副本健康度）
     */
    HDFS
}

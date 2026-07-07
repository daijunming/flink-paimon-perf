package com.paimonperf.common;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 统一指标信封：所有采集器（Paimon 元数据、YARN、HDFS）产出的指标都封装为该结构，
 * 序列化为 JSON 后写入既有 metrics topic，由既有 Flink 链路落到 StarRocks 的
 * {@code RDW_ODS_FLINK_METRICS} 表。
 *
 * <p>对应真实表结构（已验证2024-01-01）：
 * <pre>
 * CREATE TABLE RDW_ODS_FLINK_METRICS (
 *   etl_dt date NOT NULL,
 *   metric_id varchar(65533) NOT NULL,
 *   job_name varchar(65533),
 *   app_id varchar(65533),
 *   job_id varchar(65533),
 *   host_name varchar(65533),
 *   container_id varchar(65533),
 *   container_rule varchar(65533),
 *   metric_name varchar(65533),
 *   metric_type varchar(65533),  -- 用于区分来源：PAIMON_METADATA / YARN / HDFS
 *   metric_value varchar(65533),
 *   metric_ts varchar(65533)     -- 毫秒时间戳字符串
 * );
 * </pre>
 *
 * <p>字段映射策略：
 * <ul>
 *   <li>{@code source} (内部枚举) → {@code metric_type} (表字段)：PAIMON_METADATA / YARN / HDFS</li>
 *   <li>{@code metricName}      → {@code metric_name}</li>
 *   <li>{@code metricValue}     → {@code metric_value} (double转String)</li>
 *   <li>{@code collectTsMillis} → {@code metric_ts} (long转String)</li>
 *   <li>{@code job_name} = 被监测的表名（从 tags 的 "table" 读取，支持多表区分）</li>
 *   <li>{@code app_id} = 固定 "paimon_table_mornit"（本监测应用标识）</li>
 *   <li>自动生成 {@code metric_id} = metricType_jobName_metricName_collectTs</li>
 *   <li>自动生成 {@code etl_dt} = 从collectTsMillis提取日期</li>
 * </ul>
 */
public final class MetricEnvelope {

    private final MetricSource source;      // 内部枚举，映射到metricType
    private final String metricName;
    private final double metricValue;
    private final long collectTsMillis;
    private final Map<String, String> tags; // 保留用于扩展，当前未直接映射到表字段
    
    // 新增字段对齐真实表结构
    private final String metricType;        // metric_type: PAIMON_METADATA / YARN / HDFS
    private final String jobName;           // job_name: paimon-perf-test
    private final String appId;             // app_id: 被监测的表名（多表区分）
    private final String metricId;          // metric_id: 主键，自动生成
    private final String etlDt;             // etl_dt: 分区键，自动生成
    private final String jobId;             // job_id: 可选
    private final String hostName;          // host_name: 可选
    private final String containerId;       // container_id: 可选
    private final String containerRule;     // container_rule: 可选

    /**
     * @param source          指标来源，必须非空
     * @param metricName      指标名，必须非空非空白
     * @param metricValue     指标值
     * @param collectTsMillis 采集时间戳（毫秒），必须为非负的合法时间戳
     * @param tags            维度标签，允许为 null（视为空 map），内部存不可变副本
     * @throws IllegalArgumentException 当 source/metricName 非法或 collectTsMillis 为负
     */
    public MetricEnvelope(MetricSource source,
                          String metricName,
                          double metricValue,
                          long collectTsMillis,
                          Map<String, String> tags) {
        if (source == null) {
            throw new IllegalArgumentException("source 不能为空（必须属于 MetricSource 枚举）");
        }
        if (metricName == null || metricName.trim().isEmpty()) {
            throw new IllegalArgumentException("metricName 不能为空或空白");
        }
        if (collectTsMillis < 0) {
            throw new IllegalArgumentException(
                    "collectTsMillis 必须为合法时间戳（非负毫秒），实际为: " + collectTsMillis);
        }
        this.source = source;
        this.metricName = metricName;
        this.metricValue = metricValue;
        this.collectTsMillis = collectTsMillis;
        // 存不可变副本，避免外部修改破坏不变量；保留插入顺序便于稳定序列化
        Map<String, String> copy = new LinkedHashMap<>();
        if (tags != null) {
            copy.putAll(tags);
        }
        this.tags = Collections.unmodifiableMap(copy);
        
        // 自动填充新字段（对齐真实表结构）
        this.metricType = source.name();  // PAIMON_METADATA / YARN / HDFS
        // job_name 取被监测的表名（多表场景下区分来源），从 tags 的 "table" 读取；
        // 缺失时回退到 "unknown"
        this.jobName = tags != null ? tags.getOrDefault("table", "unknown") : "unknown";
        // app_id 为本监测应用的固定标识
        this.appId = "paimon_table_mornit";
        this.metricId = generateMetricId();
        this.etlDt = extractEtlDt();
        this.jobId = tags != null ? tags.getOrDefault("job_id", "") : "";
        this.hostName = tags != null ? tags.getOrDefault("host_name", "") : "";
        this.containerId = tags != null ? tags.getOrDefault("container_id", "") : "";
        this.containerRule = tags != null ? tags.getOrDefault("container_rule", "") : "";
    }

    /**
     * 生成 metric_id（主键）：metricType + "_" + jobName + "_" + metricName + "_" + collectTsMillis。
     * 加入 jobName（表名）保证多表场景下主键唯一。
     * 示例：PAIMON_METADATA_wide_table_paimon.file.count_1704099600000
     */
    private String generateMetricId() {
        return metricType + "_" + jobName + "_" + metricName + "_" + collectTsMillis;
    }

    /**
     * 从 collectTsMillis 提取 etl_dt（分区键）：yyyy-MM-dd 格式。
     * 示例：1704099600000 → 2024-01-01
     */
    private String extractEtlDt() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date(collectTsMillis));
    }

    public MetricSource getSource() {
        return source;
    }

    public String getMetricName() {
        return metricName;
    }

    public double getMetricValue() {
        return metricValue;
    }

    public long getCollectTsMillis() {
        return collectTsMillis;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    // 新增getter方法（对齐真实表字段）
    public String getMetricType() {
        return metricType;
    }

    public String getJobName() {
        return jobName;
    }

    public String getAppId() {
        return appId;
    }

    public String getMetricId() {
        return metricId;
    }

    public String getEtlDt() {
        return etlDt;
    }

    public String getJobId() {
        return jobId;
    }

    public String getHostName() {
        return hostName;
    }

    public String getContainerId() {
        return containerId;
    }

    public String getContainerRule() {
        return containerRule;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetricEnvelope)) {
            return false;
        }
        MetricEnvelope that = (MetricEnvelope) o;
        return Double.compare(that.metricValue, metricValue) == 0
                && collectTsMillis == that.collectTsMillis
                && source == that.source
                && metricName.equals(that.metricName)
                && tags.equals(that.tags)
                && metricType.equals(that.metricType)
                && metricId.equals(that.metricId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, metricName, metricValue, collectTsMillis, tags, metricType, metricId);
    }

    @Override
    public String toString() {
        return "MetricEnvelope{source=" + source
                + ", metricType='" + metricType + '\''
                + ", metricName='" + metricName + '\''
                + ", metricValue=" + metricValue
                + ", collectTsMillis=" + collectTsMillis
                + ", metricId='" + metricId + '\''
                + ", jobName='" + jobName + '\''
                + ", appId='" + appId + '\''
                + ", etlDt='" + etlDt + '\''
                + ", tags=" + tags + '}';
    }
}

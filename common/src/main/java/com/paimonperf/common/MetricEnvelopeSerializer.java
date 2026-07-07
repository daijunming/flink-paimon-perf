package com.paimonperf.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;

/**
 * 指标信封 JSON 序列化器：将 {@link MetricEnvelope} 序列化为对齐真实
 * {@code RDW_ODS_FLINK_METRICS} 表结构的 JSON 字符串（12字段），写入 metrics topic。
 *
 * <p>输出字段对齐真实表DDL（已验证2024-01-01）：
 * <pre>
 * {
 *   "etl_dt": "2024-01-01",
 *   "metric_id": "PAIMON_METADATA_wide_table_paimon.file.count_1704099600000",
 *   "job_name": "paimon-perf-test",
 *   "app_id": "wide_table",
 *   "job_id": "",
 *   "host_name": "",
 *   "container_id": "",
 *   "container_rule": "",
 *   "metric_name": "paimon.file.count",
 *   "metric_type": "PAIMON_METADATA",
 *   "metric_value": "12345.0",
 *   "metric_ts": "1704099600000"
 * }
 * </pre>
 *
 * <p>{@link ObjectMapper} 线程安全，复用单实例。
 */
public final class MetricEnvelopeSerializer {

    private final ObjectMapper mapper;

    public MetricEnvelopeSerializer() {
        this(new ObjectMapper());
    }

    public MetricEnvelopeSerializer(ObjectMapper mapper) {
        if (mapper == null) {
            throw new IllegalArgumentException("ObjectMapper 不能为空");
        }
        this.mapper = mapper;
    }

    /**
     * 序列化为 JSON 字符串（对齐真实表12字段）。
     *
     * @param envelope 指标信封，必须非空
     * @return 对齐真实表的 JSON 字符串
     * @throws IllegalArgumentException 当 envelope 为空
     * @throws RuntimeException         当序列化失败
     */
    public String toJson(MetricEnvelope envelope) {
        if (envelope == null) {
            throw new IllegalArgumentException("envelope 不能为空");
        }
        ObjectNode node = mapper.createObjectNode();
        
        // 对齐真实表12个字段（按DDL顺序）
        node.put("etl_dt", envelope.getEtlDt());
        node.put("metric_id", envelope.getMetricId());
        node.put("job_name", envelope.getJobName());
        node.put("app_id", envelope.getAppId());
        node.put("job_id", envelope.getJobId());
        node.put("host_name", envelope.getHostName());
        node.put("container_id", envelope.getContainerId());
        node.put("container_rule", envelope.getContainerRule());
        node.put("metric_name", envelope.getMetricName());
        node.put("metric_type", envelope.getMetricType());
        node.put("metric_value", String.valueOf(envelope.getMetricValue()));  // double→String
        node.put("metric_ts", String.valueOf(envelope.getCollectTsMillis())); // long→String
        
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("指标信封 JSON 序列化失败: " + envelope, ex);
        }
    }
}

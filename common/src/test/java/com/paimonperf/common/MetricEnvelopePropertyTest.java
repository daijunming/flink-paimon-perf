package com.paimonperf.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.LongRange;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Feature: paimon-perf-test, Property 6: 指标信封完整性
 *
 * <p>对任意由采集器生成的指标信封，其 source 字段非空且取值属于约定枚举
 * （PAIMON_METADATA / YARN / HDFS），且 collect_ts_millis 字段存在并为合法时间戳。
 *
 * <p>Validates: Requirements 6.3、6.4
 */
class MetricEnvelopePropertyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final MetricEnvelopeSerializer SERIALIZER = new MetricEnvelopeSerializer();

    private static final Set<String> VALID_SOURCES = new HashSet<>(Arrays.asList(
            "PAIMON_METADATA", "YARN", "HDFS"));

    @Provide
    Arbitrary<MetricSource> sources() {
        return Arbitraries.of(MetricSource.class);
    }

    @Provide
    Arbitrary<String> metricNames() {
        // 非空非空白的指标名
        return Arbitraries.strings().alpha().numeric().withChars('.', '_').ofMinLength(1).ofMaxLength(40);
    }

    @Provide
    Arbitrary<Map<String, String>> tagMaps() {
        Arbitrary<String> keys = Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10);
        Arbitrary<String> values = Arbitraries.strings().alpha().numeric().ofMaxLength(10);
        return Arbitraries.maps(keys, values).ofMaxSize(5);
    }

    /**
     * 属性：任意合法输入构造出的信封，source 非空且属枚举、collectTsMillis 为合法（非负）时间戳。
     */
    @Property(tries = 200)
    void envelopeHasValidSourceAndTimestamp(
            @ForAll("sources") MetricSource source,
            @ForAll("metricNames") String metricName,
            @ForAll double metricValue,
            @ForAll @LongRange(min = 0, max = 4102444800000L) long collectTsMillis,
            @ForAll("tagMaps") Map<String, String> tags) {

        MetricEnvelope envelope = new MetricEnvelope(source, metricName, metricValue, collectTsMillis, tags);

        // source 非空且属约定枚举
        assertNotNull(envelope.getSource(), "source 不能为空");
        assertTrue(VALID_SOURCES.contains(envelope.getSource().name()),
                "source 必须属于约定枚举: " + envelope.getSource());

        // collectTsMillis 存在且为合法时间戳（非负）
        assertTrue(envelope.getCollectTsMillis() >= 0,
                "collectTsMillis 必须为合法时间戳（非负）: " + envelope.getCollectTsMillis());
    }

    /**
     * 属性：序列化后的 JSON 携带合法的 metric_type（属枚举）与 metric_ts（合法时间戳字符串），
     * 保证写入既有 RDW_ODS_FLINK_METRICS 管道的载荷满足完整性。
     */
    @Property(tries = 200)
    void serializedJsonPreservesSourceAndTimestamp(
            @ForAll("sources") MetricSource source,
            @ForAll("metricNames") String metricName,
            @ForAll double metricValue,
            @ForAll @LongRange(min = 0, max = 4102444800000L) long collectTsMillis,
            @ForAll("tagMaps") Map<String, String> tags) throws Exception {

        MetricEnvelope envelope = new MetricEnvelope(source, metricName, metricValue, collectTsMillis, tags);
        String json = SERIALIZER.toJson(envelope);
        JsonNode node = MAPPER.readTree(json);

        // metric_type 对应来源枚举（真实表字段）
        assertTrue(node.has("metric_type"), "JSON 必须含 metric_type 字段");
        assertTrue(VALID_SOURCES.contains(node.get("metric_type").asText()),
                "JSON metric_type 必须属约定枚举: " + node.get("metric_type").asText());

        // metric_ts 为毫秒时间戳字符串（真实表 varchar 字段）
        assertTrue(node.has("metric_ts"), "JSON 必须含 metric_ts 字段");
        assertTrue(Long.parseLong(node.get("metric_ts").asText()) >= 0,
                "JSON metric_ts 必须为合法时间戳（非负）");

        // 往返一致性
        assertEquals(source.name(), node.get("metric_type").asText());
        assertEquals(collectTsMillis, Long.parseLong(node.get("metric_ts").asText()));
    }
}

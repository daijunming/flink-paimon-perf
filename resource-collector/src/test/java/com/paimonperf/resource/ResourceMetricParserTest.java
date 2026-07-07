package com.paimonperf.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paimonperf.common.MetricEnvelope;
import com.paimonperf.common.MetricSource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 资源指标解析单测（任务 6.3）：用固定 YARN/HDFS REST 响应样本验证解析逻辑正确性。
 *
 * <p>不依赖真实 YARN/HDFS 集群，纯数据驱动，覆盖 Requirements 5.1、5.2 的解析内容。
 */
class ResourceMetricParserTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void parsesYarnMetrics() throws Exception {
        // YARN /ws/v1/cluster/metrics 典型响应样本
        String yarnJson = "{\n" +
                "  \"clusterMetrics\": {\n" +
                "    \"allocatedVirtualCores\": 50,\n" +
                "    \"availableVirtualCores\": 80,\n" +
                "    \"allocatedMB\": 102400,\n" +
                "    \"availableMB\": 204800\n" +
                "  }\n" +
                "}";
        JsonNode node = MAPPER.readTree(yarnJson);
        long ts = 1_700_000_000_000L;

        List<MetricEnvelope> metrics = ResourceMetricParser.parseYarn(node, ts);

        assertEquals(4, metrics.size(), "YARN 应产出 4 条指标");
        Map<String, Double> byName = metrics.stream()
                .collect(Collectors.toMap(MetricEnvelope::getMetricName, MetricEnvelope::getMetricValue));

        assertEquals(50.0, byName.get("yarn.allocated.vcores"));
        assertEquals(80.0, byName.get("yarn.available.vcores"));
        assertEquals(102400.0, byName.get("yarn.allocated.memory.mb"));
        assertEquals(204800.0, byName.get("yarn.available.memory.mb"));

        // 所有指标 source=YARN、collectTsMillis 一致
        for (MetricEnvelope m : metrics) {
            assertEquals(MetricSource.YARN, m.getSource());
            assertEquals(ts, m.getCollectTsMillis());
        }
    }

    @Test
    void parsesHdfsMetrics() throws Exception {
        // HDFS /jmx 响应样本（简化，只保留 FSNamesystem bean）
        String hdfsJson = "{\n" +
                "  \"beans\": [\n" +
                "    {\n" +
                "      \"name\": \"Hadoop:service=NameNode,name=FSNamesystem\",\n" +
                "      \"CapacityTotal\": 10995116277760,\n" +
                "      \"CapacityUsed\": 5497558138880,\n" +
                "      \"CapacityRemaining\": 5497558138880\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        JsonNode node = MAPPER.readTree(hdfsJson);
        long ts = 1_700_000_001_000L;

        List<MetricEnvelope> metrics = ResourceMetricParser.parseHdfs(node, ts);

        assertEquals(3, metrics.size(), "HDFS 应产出 3 条指标");
        Map<String, Double> byName = metrics.stream()
                .collect(Collectors.toMap(MetricEnvelope::getMetricName, MetricEnvelope::getMetricValue));

        assertEquals(10995116277760.0, byName.get("hdfs.capacity.total.bytes"));
        assertEquals(5497558138880.0, byName.get("hdfs.capacity.used.bytes"));
        assertEquals(5497558138880.0, byName.get("hdfs.capacity.remaining.bytes"));

        for (MetricEnvelope m : metrics) {
            assertEquals(MetricSource.HDFS, m.getSource());
            assertEquals(ts, m.getCollectTsMillis());
        }
    }

    @Test
    void rejectsIllegalYarnJson() throws Exception {
        JsonNode missing = MAPPER.readTree("{}");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ResourceMetricParser.parseYarn(missing, 1L));
        assertTrue(ex.getMessage().contains("clusterMetrics"),
                "缺少必要节点时应指明");
    }

    @Test
    void rejectsIllegalHdfsJson() throws Exception {
        // beans 数组非空但不含 FSNamesystem bean
        JsonNode noFsBean = MAPPER.readTree("{\"beans\":[{\"name\":\"Other:service=Foo\"}]}");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ResourceMetricParser.parseHdfs(noFsBean, 1L));
        assertTrue(ex.getMessage().contains("FSNamesystem"),
                "未找到 FSNamesystem bean 时应指明");
    }

    @Test
    void rejectsNullOrNegativeTimestamp() throws Exception {
        JsonNode dummy = MAPPER.readTree("{\"clusterMetrics\":{}}");
        assertThrows(IllegalArgumentException.class,
                () -> ResourceMetricParser.parseYarn(null, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> ResourceMetricParser.parseYarn(dummy, -1L));
    }
}

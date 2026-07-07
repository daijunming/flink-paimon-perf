package com.paimonperf.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paimonperf.common.MetricEnvelope;
import com.paimonperf.common.MetricSource;
import com.paimonperf.common.MetricsSink;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 资源采集器集成测试（任务 6.5）：覆盖一次采集 → 解析 → Kafka 投递的端到端流程。
 * 用 mock {@link RestClient} 与 {@link MetricsSink} 避免依赖真实 YARN/HDFS 与 Kafka。
 *
 * <p>验证：
 * <ul>
 *   <li>正常路径：YARN+HDFS 均成功时投递两侧合并指标（Requirements 5.1、5.2、5.3）</li>
 *   <li>容错路径：一侧 REST 失败时仍投递另一侧指标，整体不抛异常（验证 5.5）</li>
 * </ul>
 */
class ResourceCollectorIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String YARN_JSON =
            "{\"clusterMetrics\":{\"allocatedVirtualCores\":10,\"availableVirtualCores\":20,"
                    + "\"allocatedMB\":1024,\"availableMB\":2048}}";
    private static final String HDFS_JSON =
            "{\"beans\":[{\"name\":\"Hadoop:service=NameNode,name=FSNamesystem\","
                    + "\"CapacityTotal\":1000,\"CapacityUsed\":400,\"CapacityRemaining\":600}]}";

    @Test
    void collectOnceCollectsBothYarnAndHdfs() {
        StubRestClient rest = new StubRestClient();
        rest.responses.put("http://rm:8088" + ResourceCollectorMain.YARN_METRICS_PATH, YARN_JSON);
        rest.responses.put("http://nn:9870" + ResourceCollectorMain.HDFS_JMX_PATH, HDFS_JSON);
        RecordingSink sink = new RecordingSink();

        ResourceCollectorMain.collectOnce(rest, sink, "http://rm:8088", "http://nn:9870");

        assertEquals(1, sink.emitted.size(), "应调用一次 emit");
        List<MetricEnvelope> metrics = sink.emitted.get(0);
        // YARN 4 条 + HDFS 3 条
        assertEquals(7, metrics.size(), "应合并 YARN(4) + HDFS(3) 共 7 条指标");

        long yarnCount = metrics.stream().filter(m -> m.getSource() == MetricSource.YARN).count();
        long hdfsCount = metrics.stream().filter(m -> m.getSource() == MetricSource.HDFS).count();
        assertEquals(4, yarnCount);
        assertEquals(3, hdfsCount);
    }

    @Test
    void yarnFailureDoesNotBlockHdfsCollection() {
        StubRestClient rest = new StubRestClient();
        // YARN 不配置响应 → get 抛 RestException；HDFS 正常
        rest.responses.put("http://nn:9870" + ResourceCollectorMain.HDFS_JMX_PATH, HDFS_JSON);
        RecordingSink sink = new RecordingSink();

        // 整体不抛异常
        ResourceCollectorMain.collectOnce(rest, sink, "http://rm:8088", "http://nn:9870");

        assertEquals(1, sink.emitted.size());
        List<MetricEnvelope> metrics = sink.emitted.get(0);
        // 仅 HDFS 指标被投递
        assertEquals(3, metrics.size(), "YARN 失败时仍应投递 HDFS 的 3 条指标");
        assertTrue(metrics.stream().allMatch(m -> m.getSource() == MetricSource.HDFS));
    }

    @Test
    void bothFailureEmitsEmptyAndDoesNotThrow() {
        StubRestClient rest = new StubRestClient(); // 两侧都无响应配置 → 都抛异常
        RecordingSink sink = new RecordingSink();

        ResourceCollectorMain.collectOnce(rest, sink, "http://rm:8088", "http://nn:9870");

        assertEquals(1, sink.emitted.size(), "仍调用一次 emit（空列表）");
        assertEquals(0, sink.emitted.get(0).size(), "两侧失败时投递空集");
    }

    /** 桩 RestClient：按 URL 返回预置 JSON；未配置的 URL 抛 RestException。 */
    private static final class StubRestClient implements RestClient {
        final Map<String, String> responses = new HashMap<>();

        @Override
        public JsonNode get(String url) throws RestException {
            String body = responses.get(url);
            if (body == null) {
                throw new RestException("无预置响应: " + url);
            }
            try {
                return MAPPER.readTree(body);
            } catch (Exception e) {
                throw new RestException("解析失败", e);
            }
        }
    }

    /** 记录型 MetricsSink。 */
    private static final class RecordingSink implements MetricsSink {
        final List<List<MetricEnvelope>> emitted = new ArrayList<>();

        @Override
        public void emit(List<MetricEnvelope> metrics) {
            emitted.add(new ArrayList<>(metrics));
        }

        @Override
        public void close() {
        }
    }
}

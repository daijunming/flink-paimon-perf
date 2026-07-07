package com.paimonperf.metadata;

import com.paimonperf.common.MetricEnvelope;
import com.paimonperf.common.MetricsSink;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 元数据采集器集成测试（任务 5.5）：覆盖一次采集 → 指标映射 → Kafka 投递的端到端流程。
 * 用 mock {@link MetadataReader} 与 {@link MetricsSink} 避免依赖真实 Paimon 仓库与 Kafka。
 *
 * <p>验证：
 * <ul>
 *   <li>正常路径：collectOnce 成功读取、映射并投递（Requirements 4.1、4.3）</li>
 *   <li>失败路径：reader 抛异常时 collectOnce 向上抛出（由调度器的 safeRunOnce 隔离，Property 5）</li>
 * </ul>
 */
class MetadataCollectorIntegrationTest {

    @Test
    void collectOnceReadsMapsSendsMetrics() {
        PaimonTableMetadata sampleMetadata = new PaimonTableMetadata(
                10L, 1_700_000_000_000L, 5L,
                Collections.singletonMap(0, 2048L),
                Collections.singletonMap(0, 5L),
                "APPEND");

        MetadataReader mockReader = new MetadataReader() {
            @Override
            public PaimonTableMetadata read() {
                return sampleMetadata;
            }

            @Override
            public void close() {
            }
        };
        RecordingSink sink = new RecordingSink();
        LatencyProbe mockProbe = new LatencyProbe(null, "perf", "test_table") {
            @Override
            public MetricEnvelope probe(long probeTimeMillis) {
                return null; // 集成测试不测延迟探针，返回null跳过
            }
        };

        // 执行一次采集
        MetadataCollectorMain.collectOnce(mockReader, sink, mockProbe, "test_table");

        // 验证：sink 收到映射后的指标信封列表
        assertEquals(1, sink.emitted.size(), "应调用一次 emit");
        List<MetricEnvelope> metrics = sink.emitted.get(0);
        assertTrue(metrics.size() > 0, "应产出至少一条指标");

        // 验证指标内容（快照号、文件总数）
        double snapshotIdMetric = metrics.stream()
                .filter(m -> m.getMetricName().equals("paimon.snapshot.id"))
                .findFirst().orElseThrow(AssertionError::new).getMetricValue();
        assertEquals(10.0, snapshotIdMetric, "快照号指标应与输入一致");

        double fileCountMetric = metrics.stream()
                .filter(m -> m.getMetricName().equals("paimon.file.count"))
                .findFirst().orElseThrow(AssertionError::new).getMetricValue();
        assertEquals(5.0, fileCountMetric, "文件总数指标应与输入一致");
    }

    @Test
    void collectOnceThrowsWhenReaderFails() {
        MetadataReader failingReader = new MetadataReader() {
            @Override
            public PaimonTableMetadata read() throws Exception {
                throw new RuntimeException("模拟 Paimon 读取失败");
            }

            @Override
            public void close() {
            }
        };
        RecordingSink sink = new RecordingSink();
        LatencyProbe mockProbe = new LatencyProbe(null, "perf", "test_table") {
            @Override
            public MetricEnvelope probe(long probeTimeMillis) {
                return null;
            }
        };

        // reader 抛异常时 collectOnce 向上抛出（RuntimeException 包装）
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> MetadataCollectorMain.collectOnce(failingReader, sink, mockProbe, "test_table"));
        assertTrue(ex.getMessage().contains("读取 Paimon 元数据失败"),
                "异常消息应指明失败原因");

        // sink 未被调用
        assertEquals(0, sink.emitted.size(), "reader 失败时 sink 不应被调用");
    }

    /** 记录型 MetricsSink：记录每次 emit 调用，用于测试验证。 */
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

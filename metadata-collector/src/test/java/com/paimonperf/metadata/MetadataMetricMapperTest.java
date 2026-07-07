package com.paimonperf.metadata;

import com.paimonperf.common.MetricEnvelope;
import com.paimonperf.common.MetricSource;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 纯映射逻辑单测（任务 5.3 映射部分 / 5.5）：验证 {@link MetadataMetricMapper} 把
 * {@link PaimonTableMetadata} 正确映射为指标信封列表，覆盖 Requirements 4.1 的采集内容。
 *
 * <p>不依赖真实 Paimon 仓库，纯数据驱动，可在本机稳定运行。
 */
class MetadataMetricMapperTest {

    private PaimonTableMetadata sampleMetadata() {
        Map<Integer, Long> levelSizes = new LinkedHashMap<>();
        levelSizes.put(0, 1024L);
        levelSizes.put(1, 4096L);
        Map<Integer, Long> levelFileCounts = new LinkedHashMap<>();
        levelFileCounts.put(0, 3L);
        levelFileCounts.put(1, 5L);
        return new PaimonTableMetadata(42L, 1_700_000_000_000L, 8L,
                levelSizes, levelFileCounts, "COMPACT");
    }

    @Test
    void mapsAllMetricCategories() {
        List<MetricEnvelope> metrics =
                MetadataMetricMapper.toMetrics(sampleMetadata(), "wide_table", 1_700_000_001_000L);

        Map<String, List<MetricEnvelope>> byName = metrics.stream()
                .collect(Collectors.groupingBy(MetricEnvelope::getMetricName));

        // 文件总数、快照号、快照时间、commit kind 各一条
        assertEquals(8.0, byName.get("paimon.file.count").get(0).getMetricValue());
        assertEquals(42.0, byName.get("paimon.snapshot.id").get(0).getMetricValue());
        assertEquals(1_700_000_000_000.0,
                byName.get("paimon.snapshot.time.millis").get(0).getMetricValue());
        // COMPACT 编码为 1.0
        assertEquals(1.0, byName.get("paimon.last.commit.kind").get(0).getMetricValue());

        // 各 Level 数据量/文件数：level 编码进指标名（L0/L1），各一条
        assertEquals(1, byName.get("paimon.level.size.bytes.L0").size());
        assertEquals(1, byName.get("paimon.level.size.bytes.L1").size());
        assertEquals(1, byName.get("paimon.level.file.count.L0").size());
        assertEquals(1, byName.get("paimon.level.file.count.L1").size());
    }

    @Test
    void allMetricsCarrySourceTableTagAndTimestamp() {
        long ts = 1_700_000_002_000L;
        List<MetricEnvelope> metrics =
                MetadataMetricMapper.toMetrics(sampleMetadata(), "wide_table", ts);

        for (MetricEnvelope m : metrics) {
            assertEquals(MetricSource.PAIMON_METADATA, m.getSource(), "source 必须为 PAIMON_METADATA");
            assertEquals(ts, m.getCollectTsMillis(), "collectTsMillis 必须为传入的采集时刻");
            assertEquals("wide_table", m.getTags().get("table"), "所有指标须携带 table tag");
        }
    }

    @Test
    void levelMetricsCarryLevelTag() {
        List<MetricEnvelope> metrics =
                MetadataMetricMapper.toMetrics(sampleMetadata(), "wide_table", 1L);

        List<MetricEnvelope> levelSizes = metrics.stream()
                .filter(m -> m.getMetricName().startsWith("paimon.level.size.bytes.L"))
                .collect(Collectors.toList());

        // 每条 level 指标须带 level tag，且 metric_name 含 level 编码
        for (MetricEnvelope m : levelSizes) {
            assertTrue(m.getTags().containsKey("level"), "level 指标须携带 level tag");
        }
        // 按 metric_name 的 level 后缀聚合，取值与输入一致
        Map<String, Double> byName = levelSizes.stream()
                .collect(Collectors.toMap(MetricEnvelope::getMetricName, MetricEnvelope::getMetricValue));
        assertEquals(1024.0, byName.get("paimon.level.size.bytes.L0"));
        assertEquals(4096.0, byName.get("paimon.level.size.bytes.L1"));
    }

    @Test
    void appendCommitKindEncodedAsZero() {
        Map<Integer, Long> empty = new LinkedHashMap<>();
        PaimonTableMetadata md = new PaimonTableMetadata(1L, 100L, 0L, empty, empty, "APPEND");
        List<MetricEnvelope> metrics = MetadataMetricMapper.toMetrics(md, "t", 1L);
        double kind = metrics.stream()
                .filter(m -> m.getMetricName().equals("paimon.last.commit.kind"))
                .findFirst().orElseThrow(AssertionError::new).getMetricValue();
        assertEquals(0.0, kind, "APPEND 应编码为 0.0");
    }

    @Test
    void rejectsIllegalArguments() {
        PaimonTableMetadata md = sampleMetadata();
        assertThrows(IllegalArgumentException.class,
                () -> MetadataMetricMapper.toMetrics(null, "t", 1L));
        assertThrows(IllegalArgumentException.class,
                () -> MetadataMetricMapper.toMetrics(md, " ", 1L));
        assertThrows(IllegalArgumentException.class,
                () -> MetadataMetricMapper.toMetrics(md, "t", -1L));
    }
}

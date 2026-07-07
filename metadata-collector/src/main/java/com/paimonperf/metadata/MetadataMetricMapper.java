package com.paimonperf.metadata;

import com.paimonperf.common.MetricEnvelope;
import com.paimonperf.common.MetricSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 纯映射逻辑：{@link PaimonTableMetadata} → {@code List<MetricEnvelope>}，
 * 与 Paimon API 解耦，便于单元测试在不依赖真实仓库的情况下验证指标映射正确性。
 *
 * <p>对应 Design 5.3 的指标映射部分，覆盖 Requirements 4.1 的采集内容：
 * <ul>
 *   <li>文件总数：{@code paimon.file.count}</li>
 *   <li>各 Level 数据量：{@code paimon.level.size.bytes.L{n}}（level 编码进指标名）</li>
 *   <li>各 Level 文件数：{@code paimon.level.file.count.L{n}}（level 编码进指标名）</li>
 *   <li>快照号与时间：{@code paimon.snapshot.id}、{@code paimon.snapshot.time.millis}</li>
 *   <li>Compaction 信息：{@code paimon.last.commit.kind}（APPEND/COMPACT/OVERWRITE）</li>
 * </ul>
 *
 * <p>所有指标 {@code source=PAIMON_METADATA}；{@code collectTsMillis} 为传入的采集时刻；
 * {@code tags} 携带表名、level 等维度标签。
 */
public final class MetadataMetricMapper {

    /**
     * 将 Paimon 表元数据中间模型映射为指标信封列表。
     *
     * @param metadata        Paimon 表元数据，必须非空
     * @param tableName       表名（用于 tags 维度），必须非空
     * @param collectTsMillis 本次采集时间戳（毫秒），必须为合法时间戳（非负）
     * @return 指标信封列表（不可变），按指标名排序
     * @throws IllegalArgumentException 当任一参数非法
     */
    public static List<MetricEnvelope> toMetrics(PaimonTableMetadata metadata,
                                                 String tableName,
                                                 long collectTsMillis) {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata 不能为空");
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("tableName 不能为空或空白");
        }
        if (collectTsMillis < 0) {
            throw new IllegalArgumentException("collectTsMillis 必须为合法时间戳（非负）");
        }

        List<MetricEnvelope> metrics = new ArrayList<>();

        // 公共 tags：表名（所有指标携带）
        Map<String, String> baseTagsMap = new HashMap<>();
        baseTagsMap.put("table", tableName);

        // 1. 文件总数
        metrics.add(new MetricEnvelope(
                MetricSource.PAIMON_METADATA,
                "paimon.file.count",
                metadata.getFileCount(),
                collectTsMillis,
                baseTagsMap));

        // 2. 快照号
        metrics.add(new MetricEnvelope(
                MetricSource.PAIMON_METADATA,
                "paimon.snapshot.id",
                metadata.getSnapshotId(),
                collectTsMillis,
                baseTagsMap));

        // 3. 快照时间（毫秒）
        metrics.add(new MetricEnvelope(
                MetricSource.PAIMON_METADATA,
                "paimon.snapshot.time.millis",
                metadata.getSnapshotTimeMillis(),
                collectTsMillis,
                baseTagsMap));

        // 4. 各 Level 数据量（字节）
        //    level 编码进 metric_name（paimon.level.size.bytes.L{n}），因为目标表 RDW_ODS_FLINK_METRICS
        //    无 tags/level 列，序列化只保留 12 个字段；不编码则落表后各 Level 无法区分。
        //    同时保留 level tag（内存态可用，便于单测与未来扩展）。
        for (Map.Entry<Integer, Long> e : metadata.getLevelSizes().entrySet()) {
            Map<String, String> levelTags = new HashMap<>(baseTagsMap);
            levelTags.put("level", String.valueOf(e.getKey()));
            metrics.add(new MetricEnvelope(
                    MetricSource.PAIMON_METADATA,
                    "paimon.level.size.bytes.L" + e.getKey(),
                    e.getValue(),
                    collectTsMillis,
                    levelTags));
        }

        // 5. 各 Level 文件数（同样把 level 编码进 metric_name）
        for (Map.Entry<Integer, Long> e : metadata.getLevelFileCounts().entrySet()) {
            Map<String, String> levelTags = new HashMap<>(baseTagsMap);
            levelTags.put("level", String.valueOf(e.getKey()));
            metrics.add(new MetricEnvelope(
                    MetricSource.PAIMON_METADATA,
                    "paimon.level.file.count.L" + e.getKey(),
                    e.getValue(),
                    collectTsMillis,
                    levelTags));
        }

        // 6. Compaction 信息：lastCommitKind 编码为枚举值，供下游 SQL 判定 compaction 触发
        //    （COMPACT=1.0、APPEND=0.0、其他=0.5）便于下游 CASE WHEN 聚合
        double commitKindValue = encodeCommitKind(metadata.getLastCommitKind());
        metrics.add(new MetricEnvelope(
                MetricSource.PAIMON_METADATA,
                "paimon.last.commit.kind",
                commitKindValue,
                collectTsMillis,
                baseTagsMap));

        // 返回不可变副本，按指标名排序保证稳定序列化
        metrics.sort((a, b) -> a.getMetricName().compareTo(b.getMetricName()));
        return Collections.unmodifiableList(metrics);
    }

    /**
     * 编码 commitKind 为数值：COMPACT=1.0（高 compaction 开销）、APPEND=0.0（无 compaction）、
     * 其他=0.5（中间状态），便于下游 SQL 按阈值聚合统计 compaction 触发频率。
     */
    private static double encodeCommitKind(String kind) {
        if (kind == null) {
            return 0.5;
        }
        switch (kind.toUpperCase()) {
            case "COMPACT":
                return 1.0;
            case "APPEND":
                return 0.0;
            default:
                return 0.5;
        }
    }
}

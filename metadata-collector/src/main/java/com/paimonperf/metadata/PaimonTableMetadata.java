package com.paimonperf.metadata;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Paimon 表元数据中间模型：{@link MetadataReader} 读取一次 Paimon 表后产出的纯数据快照，
 * 与 Paimon API 解耦，便于 {@link MetadataMetricMapper} 做可单测的纯映射。
 *
 * <p>对应 Design「Data Models / Paimon 表元数据（采集器读取的中间模型）」：
 * <ul>
 *   <li>{@code snapshotId}     当前快照号</li>
 *   <li>{@code snapshotTimeMillis} 快照提交时间（毫秒），用于关联时段</li>
 *   <li>{@code fileCount}      当前快照下数据文件总数</li>
 *   <li>{@code levelSizes}     各 LSM Level 的数据量（map&lt;level,bytes&gt;）</li>
 *   <li>{@code levelFileCounts} 各 LSM Level 的文件数（map&lt;level,count&gt;），用于观测 compaction 压力</li>
 *   <li>{@code lastCommitKind} 最近一次快照的提交类型（如 APPEND/COMPACT/OVERWRITE），反映 compaction 触发</li>
 * </ul>
 *
 * <p>不可变（immutable）：map 字段存不可变副本，避免外部修改破坏一致性。
 */
public final class PaimonTableMetadata {

    private final long snapshotId;
    private final long snapshotTimeMillis;
    private final long fileCount;
    private final Map<Integer, Long> levelSizes;
    private final Map<Integer, Long> levelFileCounts;
    private final String lastCommitKind;

    public PaimonTableMetadata(long snapshotId,
                               long snapshotTimeMillis,
                               long fileCount,
                               Map<Integer, Long> levelSizes,
                               Map<Integer, Long> levelFileCounts,
                               String lastCommitKind) {
        this.snapshotId = snapshotId;
        this.snapshotTimeMillis = snapshotTimeMillis;
        this.fileCount = fileCount;
        // TreeMap 不可变副本：保留 level 升序，序列化/遍历稳定
        this.levelSizes = Collections.unmodifiableMap(
                new TreeMap<>(levelSizes == null ? Collections.emptyMap() : levelSizes));
        this.levelFileCounts = Collections.unmodifiableMap(
                new TreeMap<>(levelFileCounts == null ? Collections.emptyMap() : levelFileCounts));
        this.lastCommitKind = lastCommitKind == null ? "UNKNOWN" : lastCommitKind;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getSnapshotTimeMillis() {
        return snapshotTimeMillis;
    }

    public long getFileCount() {
        return fileCount;
    }

    public Map<Integer, Long> getLevelSizes() {
        return levelSizes;
    }

    public Map<Integer, Long> getLevelFileCounts() {
        return levelFileCounts;
    }

    public String getLastCommitKind() {
        return lastCommitKind;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PaimonTableMetadata)) {
            return false;
        }
        PaimonTableMetadata that = (PaimonTableMetadata) o;
        return snapshotId == that.snapshotId
                && snapshotTimeMillis == that.snapshotTimeMillis
                && fileCount == that.fileCount
                && levelSizes.equals(that.levelSizes)
                && levelFileCounts.equals(that.levelFileCounts)
                && lastCommitKind.equals(that.lastCommitKind);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, snapshotTimeMillis, fileCount,
                levelSizes, levelFileCounts, lastCommitKind);
    }

    @Override
    public String toString() {
        return "PaimonTableMetadata{snapshotId=" + snapshotId
                + ", snapshotTimeMillis=" + snapshotTimeMillis
                + ", fileCount=" + fileCount
                + ", levelSizes=" + levelSizes
                + ", levelFileCounts=" + levelFileCounts
                + ", lastCommitKind='" + lastCommitKind + '\'' + '}';
    }
}

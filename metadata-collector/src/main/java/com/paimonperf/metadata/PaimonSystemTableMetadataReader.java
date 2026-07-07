package com.paimonperf.metadata;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 基于 Paimon 系统表的 {@link MetadataReader} 实现：通过 Hadoop Catalog 打开目标表的
 * 系统表 {@code <table>$snapshots} 与 {@code <table>$files}，扫描后聚合为
 * {@link PaimonTableMetadata}。把 Paimon 特定 API 调用全部隔离在本类，使纯映射逻辑
 * （{@link MetadataMetricMapper}）可在无真实仓库下单测。
 *
 * <p>读取内容（覆盖 Requirements 4.1）：
 * <ul>
 *   <li>最新快照号与提交时间、提交类型 ← {@code $snapshots} 系统表的最大 snapshot_id 行</li>
 *   <li>文件总数、各 Level 文件数与数据量 ← {@code $files} 系统表逐行聚合</li>
 * </ul>
 *
 * <p>注意：系统表的列名以 Paimon 1.1 的 schema 为准，本实现按列名读取，列名变动时在此集中调整。
 */
public final class PaimonSystemTableMetadataReader implements MetadataReader {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonSystemTableMetadataReader.class);

    private final Catalog catalog;
    private final String database;
    private final String table;

    /**
     * @param warehouse Paimon 仓库路径（如 hdfs:///warehouse/paimon_perf），非空
     * @param database  数据库名，非空
     * @param table     表名（业务宽表，不带 $ 后缀），非空
     */
    public PaimonSystemTableMetadataReader(String warehouse, String database, String table) {
        this(warehouse, database, table, null, null, null);
    }

    /**
     * 带 Kerberos 认证的构造：在创建 catalog 前完成 keytab 登录，并把 Kerberos 配置
     * 注入 catalog Options，使 catalog 内部的 Hadoop FileSystem 走同一认证上下文。
     *
     * @param warehouse Paimon 仓库路径（如 hdfs:///warehouse/paimon_perf），非空
     * @param database  数据库名，非空
     * @param table     表名（业务宽表，不带 $ 后缀），非空
     * @param principal Kerberos principal，为空则不启用 Kerberos
     * @param keytab    keytab 路径，为空则不启用 Kerberos
     * @param krb5Conf  krb5.conf 路径，可为空（用系统默认）
     */
    public PaimonSystemTableMetadataReader(String warehouse, String database, String table,
                                           String principal, String keytab, String krb5Conf) {
        if (warehouse == null || warehouse.trim().isEmpty()) {
            throw new IllegalArgumentException("warehouse 不能为空或空白");
        }
        if (database == null || database.trim().isEmpty()) {
            throw new IllegalArgumentException("database 不能为空或空白");
        }
        if (table == null || table.trim().isEmpty()) {
            throw new IllegalArgumentException("table 不能为空或空白");
        }

        // 1. 先完成 Kerberos 登录（未配置时为 no-op，幂等）
        KerberosAuthenticator.authenticate(principal, keytab, krb5Conf);

        // 2. 构造 catalog Options：warehouse 必填，Kerberos 配置按需注入
        Options options = new Options();
        options.set("warehouse", warehouse);
        boolean kerberosEnabled = isNotBlank(principal) && isNotBlank(keytab);
        if (kerberosEnabled) {
            options.set("security.kerberos.login.keytab", keytab);
            options.set("security.kerberos.login.principal", principal);
            options.set("security.kerberos.login.use-ticket-cache", "false");
            options.set("hadoop.security.authentication", "Kerberos");
            LOG.info("Paimon catalog 启用 Kerberos: principal={}", principal);
        }

        this.catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        this.database = database;
        this.table = table;
    }

    private static boolean isNotBlank(String s) {
        return s != null && !s.trim().isEmpty();
    }

    @Override
    public PaimonTableMetadata read() throws Exception {
        SnapshotInfo snapshot = readLatestSnapshot();
        FileAggregates files = readFileAggregates();
        return new PaimonTableMetadata(
                snapshot.snapshotId,
                snapshot.commitTimeMillis,
                files.totalFileCount,
                files.levelSizes,
                files.levelFileCounts,
                snapshot.commitKind);
    }

    /**
     * 获取 Paimon catalog 实例，供延迟探针复用连接（任务 5.6）。
     */
    public Catalog getCatalog() {
        return catalog;
    }

    /** 读取 {@code <table>$snapshots} 系统表，取最大 snapshot_id 对应的行。 */
    private SnapshotInfo readLatestSnapshot() throws Exception {
        Identifier id = Identifier.create(database, table + "$snapshots");
        Table sysTable = catalog.getTable(id);
        List<String> fieldNames = sysTable.rowType().getFieldNames();
        int idxSnapshotId = fieldNames.indexOf("snapshot_id");
        int idxCommitTime = fieldNames.indexOf("commit_time");
        int idxCommitKind = fieldNames.indexOf("commit_kind");

        SnapshotInfo latest = new SnapshotInfo();
        forEachRow(sysTable, row -> {
            long sid = row.getLong(idxSnapshotId);
            if (sid >= latest.snapshotId) {
                latest.snapshotId = sid;
                // commit_time 为 TIMESTAMP，统一取毫秒；列缺失时记 0
                latest.commitTimeMillis = idxCommitTime >= 0 && !row.isNullAt(idxCommitTime)
                        ? row.getTimestamp(idxCommitTime, 3).getMillisecond()
                        : 0L;
                latest.commitKind = idxCommitKind >= 0 && !row.isNullAt(idxCommitKind)
                        ? row.getString(idxCommitKind).toString()
                        : "UNKNOWN";
            }
        });
        return latest;
    }

    /** 读取 {@code <table>$files} 系统表，按 level 聚合文件数与数据量。 */
    private FileAggregates readFileAggregates() throws Exception {
        Identifier id = Identifier.create(database, table + "$files");
        Table sysTable = catalog.getTable(id);
        List<String> fieldNames = sysTable.rowType().getFieldNames();
        int idxLevel = fieldNames.indexOf("level");
        int idxFileSize = fieldNames.indexOf("file_size_in_bytes");

        FileAggregates agg = new FileAggregates();
        forEachRow(sysTable, row -> {
            agg.totalFileCount++;
            int level = idxLevel >= 0 && !row.isNullAt(idxLevel) ? row.getInt(idxLevel) : -1;
            long size = idxFileSize >= 0 && !row.isNullAt(idxFileSize) ? row.getLong(idxFileSize) : 0L;
            agg.levelFileCounts.merge(level, 1L, Long::sum);
            agg.levelSizes.merge(level, size, Long::sum);
        });
        return agg;
    }

    /** 全表扫描系统表，对每行执行 consumer。系统表数据量小（快照/文件元信息），一次性读取可接受。 */
    private void forEachRow(Table sysTable, RowConsumer consumer) throws Exception {
        ReadBuilder readBuilder = sysTable.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(splits)) {
            reader.forEachRemaining(row -> {
                try {
                    consumer.accept(row);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Override
    public void close() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @FunctionalInterface
    private interface RowConsumer {
        void accept(InternalRow row) throws Exception;
    }

    /** 最新快照信息的可变累加器。 */
    private static final class SnapshotInfo {
        long snapshotId = -1L;
        long commitTimeMillis = 0L;
        String commitKind = "UNKNOWN";
    }

    /** 文件级聚合的可变累加器。 */
    private static final class FileAggregates {
        long totalFileCount = 0L;
        final Map<Integer, Long> levelSizes = new TreeMap<>();
        final Map<Integer, Long> levelFileCounts = new TreeMap<>();
    }
}

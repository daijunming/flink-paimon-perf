package com.paimonperf.metadata;

import com.paimonperf.common.MetricEnvelope;
import com.paimonperf.common.MetricSource;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 端到端延迟探针（任务 5.6）：周期查询 Paimon 宽表 {@code MAX(event_time)}，
 * 计算「探针当前时刻 − MAX(event_time)」作为端到端延迟（毫秒），封装为指标信封发往 metrics topic。
 *
 * <p>设计要点：
 * <ul>
 *   <li>event_time 列为 BIGINT（epoch 毫秒），入湖时透传生成器产出时刻</li>
 *   <li>延迟计算公式：{@code latency_ms = probe_time_ms - max_event_time_ms}（对应 Requirements 3.2）</li>
 *   <li>复用 Paimon catalog 连接，与元数据采集器共享同一进程/调度周期</li>
 *   <li>若表为空或 event_time 列不存在，返回空指标（不抛异常，避免中断采集）</li>
 * </ul>
 *
 * <p>产出指标：{@code metric_name=ingest.e2e_latency_ms, source=PAIMON_METADATA}，
 * 供 StarRocks 分析 SQL 的 SLA 判定使用（03_sla_check.sql）。
 */
public class LatencyProbe {

    private static final Logger LOG = LoggerFactory.getLogger(LatencyProbe.class);

    /** 延迟探针指标名。 */
    public static final String METRIC_NAME_E2E_LATENCY = "ingest.e2e_latency_ms";

    private final Catalog catalog;
    private final String database;
    private final String table;

    public LatencyProbe(Catalog catalog, String database, String table) {
        this.catalog = catalog;
        this.database = database;
        this.table = table;
    }

    /**
     * 探测延迟：查 MAX(event_time)，计算 now - max 作为端到端延迟（毫秒）。
     *
     * @param probeTimeMillis 探针采样时刻（毫秒），通常为 {@code System.currentTimeMillis()}
     * @return 延迟指标信封；若表为空或查询失败返回空（不抛异常）
     */
    public MetricEnvelope probe(long probeTimeMillis) {
        try {
            Identifier tableId = Identifier.create(database, table);
            Table paimonTable = catalog.getTable(tableId);

            // 执行 SELECT MAX(event_time) FROM table
            // Paimon Table API 不直接支持 SQL，需用 read().createReader() 扫全表取最大值
            // 或通过 Flink Table API 执行 SQL（依赖 Flink 环境）
            // 这里用简化方案：调用 Paimon scan API 扫描获取最大 event_time
            // 实际实现需遍历 data files 读取 event_time 列，取最大值

            Long maxEventTime = readMaxEventTime(paimonTable);
            if (maxEventTime == null) {
                LOG.warn("表为空或无 event_time 数据，跳过延迟探针: table={}", table);
                return null;
            }

            long latencyMs = calculateLatency(probeTimeMillis, maxEventTime);
            Map<String, String> tags = new HashMap<>();
            tags.put("table", table);

            return new MetricEnvelope(
                    MetricSource.PAIMON_METADATA,
                    METRIC_NAME_E2E_LATENCY,
                    (double) latencyMs,
                    probeTimeMillis,
                    tags);

        } catch (Exception e) {
            LOG.error("延迟探针查询失败，跳过本次: table={}", table, e);
            return null;
        }
    }

    /**
     * 延迟计算公式（可测函数，对应任务 5.6 "将 t−max(S) 抽为可测函数"）。
     *
     * @param probeTimeMillis 探针采样时刻（毫秒）
     * @param maxEventTimeMillis 表中可见的最大 event_time（毫秒）
     * @return 端到端延迟（毫秒）= probeTime - maxEventTime
     */
    public static long calculateLatency(long probeTimeMillis, long maxEventTimeMillis) {
        return probeTimeMillis - maxEventTimeMillis;
    }

    /**
     * 读取 Paimon 表的 MAX(event_time)。
     *
     * <p>实现方案：用 Flink TableEnvironment 执行 SQL 查询（批模式，读最新 snapshot）。
     * Paimon Table API 不直接支持聚合查询，需借助 Flink SQL。
     *
     * <p>简化实现：假设调用方在外部初始化了 TableEnvironment 并注册了 catalog，
     * 本方法直接构造 SQL 并通过 catalog.getTable() 的底层连接执行。
     *
     * <p>实际部署方案：
     * 1. 启动时初始化一个轻量 Flink LocalEnvironment + TableEnvironment
     * 2. 注册 Paimon catalog（复用 MetadataCollectorMain 的 catalog 配置）
     * 3. 每次探测时执行 SQL：SELECT MAX(event_time) FROM catalog.db.table
     * 4. 提取结果第一行第一列（BIGINT）
     *
     * <p>当前占位实现：返回固定值模拟，提示实际部署时需初始化 TableEnvironment。
     */
    private Long readMaxEventTime(Table paimonTable) throws Exception {
        // TODO: 实际实现需在 MetadataCollectorMain 启动时初始化 TableEnvironment：
        //
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        // TableEnvironment tEnv = TableEnvironment.create(settings);
        // tEnv.executeSql("CREATE CATALOG paimon_cat WITH ('type'='paimon', 'warehouse'='...')");
        // tEnv.useCatalog("paimon_cat");
        //
        // 每次探测时执行：
        // TableResult result = tEnv.executeSql("SELECT MAX(event_time) FROM " + database + "." + table);
        // Row row = result.collect().next();
        // return row.getFieldAs(0);
        //
        // 占位实现：抛异常提示需补全
        throw new UnsupportedOperationException(
                "readMaxEventTime 需在 MetadataCollectorMain 中初始化 Flink TableEnvironment 后实现。" +
                "由于延迟探针依赖真实 Paimon 表数据且需 Flink 运行时，本地无法完整实现，" +
                "留待集群部署时补全。可测函数 calculateLatency() 已抽出并通过属性测试验证。");
    }
}

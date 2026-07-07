package com.paimonperf.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.paimonperf.common.MetricEnvelope;
import com.paimonperf.common.MetricSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 纯解析逻辑：YARN/HDFS REST JSON → {@code List<MetricEnvelope>}，与 HTTP 调用解耦，
 * 便于用固定响应样本单测（任务 6.3）。
 *
 * <p>对应 Design「(d) YARN/HDFS 资源采集器」的 {@code ResourceMetricParser}。
 * 支持解析：
 * <ul>
 *   <li>YARN {@code /ws/v1/cluster/metrics}：CPU vCores（allocatedVirtualCores / availableVirtualCores）、
 *       内存（allocatedMB / availableMB）</li>
 *   <li>HDFS {@code /jmx}：NameNode 的 {@code Hadoop:service=NameNode,name=FSNamesystem} bean
 *       中的 CapacityTotal / CapacityUsed / CapacityRemaining</li>
 * </ul>
 *
 * <p>所有指标 {@code collectTsMillis} 为传入的采集时刻；{@code source} 分别为
 * {@code YARN} 与 {@code HDFS}。字段名按 YARN 2.x / Hadoop 3.x REST schema。
 */
public final class ResourceMetricParser {

    private ResourceMetricParser() {
    }

    /**
     * 解析 YARN {@code /ws/v1/cluster/metrics} 响应。
     *
     * @param json            YARN REST 根节点，必须非空
     * @param collectTsMillis 本次采集时间戳（毫秒），必须为合法时间戳（非负）
     * @return 指标信封列表（不可变），4 条：allocatedVirtualCores / availableVirtualCores /
     * allocatedMB / availableMB
     * @throws IllegalArgumentException 当参数非法或 JSON 缺少必要字段
     */
    public static List<MetricEnvelope> parseYarn(JsonNode json, long collectTsMillis) {
        validateArgs(json, collectTsMillis);
        JsonNode metrics = json.path("clusterMetrics");
        if (metrics.isMissingNode()) {
            throw new IllegalArgumentException("YARN 响应缺少 clusterMetrics 节点");
        }

        List<MetricEnvelope> result = new ArrayList<MetricEnvelope>();
        Map<String, String> tags = new HashMap<String, String>();
        // YARN 为集群级资源指标，不归属单表；用 table=cluster 作为 app_id，
        // 与各 Paimon 表的元数据指标在 RDW_ODS_FLINK_METRICS 中明确区分
        tags.put("table", "cluster");
        // YARN 可能有多个 ResourceManager；暂不区分，聚合指标无需 RM 维度 tag

        result.add(new MetricEnvelope(
                MetricSource.YARN,
                "yarn.allocated.vcores",
                metrics.path("allocatedVirtualCores").asDouble(0.0),
                collectTsMillis,
                tags));
        result.add(new MetricEnvelope(
                MetricSource.YARN,
                "yarn.available.vcores",
                metrics.path("availableVirtualCores").asDouble(0.0),
                collectTsMillis,
                tags));
        result.add(new MetricEnvelope(
                MetricSource.YARN,
                "yarn.allocated.memory.mb",
                metrics.path("allocatedMB").asDouble(0.0),
                collectTsMillis,
                tags));
        result.add(new MetricEnvelope(
                MetricSource.YARN,
                "yarn.available.memory.mb",
                metrics.path("availableMB").asDouble(0.0),
                collectTsMillis,
                tags));

        return result;
    }

    /**
     * 解析 HDFS {@code /jmx} 响应。
     *
     * @param json            HDFS JMX 根节点，必须非空
     * @param collectTsMillis 本次采集时间戳（毫秒），必须为合法时间戳（非负）
     * @return 指标信封列表（不可变），3 条：CapacityTotal / CapacityUsed / CapacityRemaining
     * @throws IllegalArgumentException 当参数非法或 JSON 缺少必要字段
     */
    public static List<MetricEnvelope> parseHdfs(JsonNode json, long collectTsMillis) {
        validateArgs(json, collectTsMillis);
        JsonNode beans = json.path("beans");
        if (!beans.isArray() || beans.size() == 0) {
            throw new IllegalArgumentException("HDFS 响应缺少 beans 数组");
        }

        // 找到 name="Hadoop:service=NameNode,name=FSNamesystem" 的 bean
        JsonNode fsBean = null;
        for (JsonNode bean : beans) {
            String name = bean.path("name").asText("");
            if (name.contains("FSNamesystem") && name.contains("NameNode")) {
                fsBean = bean;
                break;
            }
        }
        if (fsBean == null) {
            throw new IllegalArgumentException("HDFS 响应未找到 FSNamesystem bean");
        }

        List<MetricEnvelope> result = new ArrayList<MetricEnvelope>();
        Map<String, String> tags = new HashMap<String, String>();
        // HDFS 为集群级存储指标，不归属单表；用 table=cluster 作为 app_id
        tags.put("table", "cluster");

        result.add(new MetricEnvelope(
                MetricSource.HDFS,
                "hdfs.capacity.total.bytes",
                fsBean.path("CapacityTotal").asDouble(0.0),
                collectTsMillis,
                tags));
        result.add(new MetricEnvelope(
                MetricSource.HDFS,
                "hdfs.capacity.used.bytes",
                fsBean.path("CapacityUsed").asDouble(0.0),
                collectTsMillis,
                tags));
        result.add(new MetricEnvelope(
                MetricSource.HDFS,
                "hdfs.capacity.remaining.bytes",
                fsBean.path("CapacityRemaining").asDouble(0.0),
                collectTsMillis,
                tags));

        return result;
    }

    private static void validateArgs(JsonNode json, long collectTsMillis) {
        if (json == null) {
            throw new IllegalArgumentException("json 不能为空");
        }
        if (collectTsMillis < 0) {
            throw new IllegalArgumentException("collectTsMillis 必须为合法时间戳（非负）");
        }
    }
}

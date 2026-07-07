package com.paimonperf.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.paimonperf.common.KafkaMetricsSink;
import com.paimonperf.common.MetricEnvelope;
import com.paimonperf.common.MetricsSink;
import com.paimonperf.common.ScheduledCollectorScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * YARN/HDFS 资源采集器主入口（组件 d）。
 *
 * <p>读取配置 → 构造 {@link RestClient}（HTTP）与 {@link MetricsSink}（Kafka）→
 * 用 {@link ScheduledCollectorScheduler} 按周期采集：每周期分别调 YARN 与 HDFS REST、
 * 解析为指标信封、经 MetricsSink 发往既有 metrics topic。单次 REST 失败被调度器隔离，
 * 不中断后续周期（Property 5，验证 5.5）。
 *
 * <p>配置项（系统属性 -D 或 properties 文件，离线注入）：
 * <ul>
 *   <li>{@code yarn.rm.url}              YARN ResourceManager 基地址（如 http://rm:8088，必填）</li>
 *   <li>{@code hdfs.nn.url}              HDFS NameNode 基地址（如 http://nn:9870，必填）</li>
 *   <li>{@code collect.interval.seconds} 采集周期秒（默认 30）</li>
 *   <li>{@code kafka.bootstrap}          Kafka 地址（必填）</li>
 *   <li>{@code kafka.metrics.topic}      metrics topic（必填）</li>
 * </ul>
 *
 * <p>用法：{@code java -jar resource-collector.jar [config.properties]}
 */
public final class ResourceCollectorMain {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceCollectorMain.class);

    /** YARN 集群指标 REST 路径。 */
    static final String YARN_METRICS_PATH = "/ws/v1/cluster/metrics";
    /** HDFS NameNode JMX REST 路径。 */
    static final String HDFS_JMX_PATH = "/jmx";

    private ResourceCollectorMain() {
    }

    public static void main(String[] args) throws Exception {
        CollectorConfig cfg = CollectorConfig.load(args);
        LOG.info("启动 YARN/HDFS 资源采集器: {}", cfg);

        RestClient restClient = new HttpRestClient();
        MetricsSink sink = new KafkaMetricsSink(cfg.kafkaBootstrap, cfg.kafkaMetricsTopic);
        ScheduledCollectorScheduler scheduler = new ScheduledCollectorScheduler();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("收到关闭信号，停止采集并释放资源");
            scheduler.shutdown(10);
            closeQuietly(sink);
        }, "resource-collector-shutdown"));

        scheduler.runPeriodically(
                () -> collectOnce(restClient, sink, cfg.yarnRmUrl, cfg.hdfsNnUrl),
                cfg.collectIntervalSeconds);

        Thread.currentThread().join();
    }

    /**
     * 单次采集：分别调 YARN 与 HDFS REST、解析、合并后投递。
     *
     * <p>容错策略：YARN 与 HDFS 各自独立 try/catch——一侧 REST 失败仅记录该侧错误，
     * 不影响另一侧采集；两侧都失败时本周期投递空集（emit 空列表为 no-op）。整体不抛异常，
     * 与「单次失败不中断后续周期」一致（验证 5.5）。
     */
    static void collectOnce(RestClient restClient, MetricsSink sink,
                            String yarnRmUrl, String hdfsNnUrl) {
        long collectTs = System.currentTimeMillis();
        List<MetricEnvelope> metrics = new ArrayList<>();

        // YARN：失败仅记录，不影响 HDFS 采集
        try {
            JsonNode yarnResp = restClient.get(yarnRmUrl + YARN_METRICS_PATH);
            metrics.addAll(ResourceMetricParser.parseYarn(yarnResp, collectTs));
        } catch (Exception e) {
            LOG.error("YARN 资源采集失败，本周期跳过 YARN: {}", e.toString(), e);
        }

        // HDFS：失败仅记录，不影响已采集的 YARN 指标投递
        try {
            JsonNode hdfsResp = restClient.get(hdfsNnUrl + HDFS_JMX_PATH);
            metrics.addAll(ResourceMetricParser.parseHdfs(hdfsResp, collectTs));
        } catch (Exception e) {
            LOG.error("HDFS 资源采集失败，本周期跳过 HDFS: {}", e.toString(), e);
        }

        sink.emit(metrics);
        LOG.info("资源采集完成: metrics={}", metrics.size());
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception e) {
            LOG.warn("关闭资源失败: {}", c.getClass().getSimpleName(), e);
        }
    }

    /** 采集器配置，带必填校验。 */
    static final class CollectorConfig {
        final String yarnRmUrl;
        final String hdfsNnUrl;
        final int collectIntervalSeconds;
        final String kafkaBootstrap;
        final String kafkaMetricsTopic;

        private CollectorConfig(String yarnRmUrl, String hdfsNnUrl, int collectIntervalSeconds,
                                String kafkaBootstrap, String kafkaMetricsTopic) {
            this.yarnRmUrl = yarnRmUrl;
            this.hdfsNnUrl = hdfsNnUrl;
            this.collectIntervalSeconds = collectIntervalSeconds;
            this.kafkaBootstrap = kafkaBootstrap;
            this.kafkaMetricsTopic = kafkaMetricsTopic;
        }

        /** 从 properties 文件（args[0]）或系统属性加载，缺必填项即终止并指明缺失参数名。 */
        static CollectorConfig load(String[] args) throws Exception {
            Properties p = new Properties();
            if (args != null && args.length > 0) {
                try (java.io.InputStream in = new java.io.FileInputStream(args[0])) {
                    p.load(in);
                }
            } else {
                p.putAll(System.getProperties());
            }
            String yarnRmUrl = require(p, "yarn.rm.url");
            String hdfsNnUrl = require(p, "hdfs.nn.url");
            String kafkaBootstrap = require(p, "kafka.bootstrap");
            String kafkaMetricsTopic = require(p, "kafka.metrics.topic");
            int interval = Integer.parseInt(p.getProperty("collect.interval.seconds", "30"));
            if (interval <= 0) {
                throw new IllegalArgumentException("collect.interval.seconds 必须为正，实际为: " + interval);
            }
            return new CollectorConfig(yarnRmUrl, hdfsNnUrl, interval,
                    kafkaBootstrap, kafkaMetricsTopic);
        }

        private static String require(Properties p, String key) {
            String v = p.getProperty(key);
            if (v == null || v.trim().isEmpty()) {
                throw new IllegalArgumentException("缺少必填配置项: " + key);
            }
            return v.trim();
        }

        @Override
        public String toString() {
            return "CollectorConfig{yarnRmUrl='" + yarnRmUrl + "', hdfsNnUrl='" + hdfsNnUrl
                    + "', collectIntervalSeconds=" + collectIntervalSeconds
                    + ", kafkaBootstrap='" + kafkaBootstrap
                    + "', kafkaMetricsTopic='" + kafkaMetricsTopic + "'}";
        }
    }
}

package com.paimonperf.metadata;

import com.paimonperf.common.KafkaMetricsSink;
import com.paimonperf.common.MetricEnvelope;
import com.paimonperf.common.MetricsSink;
import com.paimonperf.common.ScheduledCollectorScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Paimon 元数据采集器主入口（组件 c）。
 *
 * <p>读取配置 → 构造 {@link MetadataReader}（Paimon 系统表）与 {@link MetricsSink}（Kafka）
 * → 用 {@link ScheduledCollectorScheduler} 按周期采集：每周期读取一次元数据、映射为指标信封、
 * 经 MetricsSink 发往既有 metrics topic。单次采集失败被调度器隔离，不中断后续周期（Property 5）。
 *
 * <p>配置项（系统属性 -D 或 properties 文件，离线注入）：
 * <ul>
 *   <li>{@code warehouse}                Paimon 仓库路径（必填）</li>
 *   <li>{@code database}                 数据库名（必填）</li>
 *   <li>{@code table}                    业务宽表名（必填）</li>
 *   <li>{@code collect.interval.seconds} 采集周期秒（默认 30）</li>
 *   <li>{@code kafka.bootstrap}          Kafka 地址（必填）</li>
 *   <li>{@code kafka.metrics.topic}      metrics topic（必填）</li>
 *   <li>{@code kerberos.principal}       Kerberos principal（可选，与 keytab 成对）</li>
 *   <li>{@code kerberos.keytab}          keytab 文件路径（可选，与 principal 成对）</li>
 *   <li>{@code kerberos.krb5.conf}       krb5.conf 路径（可选，默认用系统配置）</li>
 * </ul>
 *
 * <p>用法：{@code java -jar metadata-collector.jar [config.properties]}
 * 配置加载优先级：显式文件路径 args[0] → 类路径 {@code collector.properties}
 * （支持 {@code -Xbootclasspath/a:conf} 或 {@code -cp conf} 挂载配置目录）→ JVM 系统属性。
 */
public final class MetadataCollectorMain {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataCollectorMain.class);

    private MetadataCollectorMain() {
    }

    public static void main(String[] args) throws Exception {
        CollectorConfig cfg = CollectorConfig.load(args);
        LOG.info("启动 Paimon 元数据采集器: {}", cfg);

        MetadataReader reader = new PaimonSystemTableMetadataReader(
                cfg.warehouse, cfg.database, cfg.table,
                cfg.kerberosPrincipal, cfg.kerberosKeytab, cfg.krb5Conf);
        MetricsSink sink = new KafkaMetricsSink(cfg.kafkaBootstrap, cfg.kafkaMetricsTopic);
        
        // 延迟探针：复用 reader 的 catalog 连接（任务 5.6）
        LatencyProbe latencyProbe = new LatencyProbe(
                ((PaimonSystemTableMetadataReader) reader).getCatalog(),
                cfg.database, cfg.table);
        
        ScheduledCollectorScheduler scheduler = new ScheduledCollectorScheduler();

        // 进程退出时优雅关闭：停止调度、flush/close Kafka、关闭 Paimon catalog
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("收到关闭信号，停止采集并释放资源");
            scheduler.shutdown(10);
            closeQuietly(sink);
            closeQuietly(reader);
        }, "metadata-collector-shutdown"));

        scheduler.runPeriodically(
                () -> collectOnce(reader, sink, latencyProbe, cfg.table),
                cfg.collectIntervalSeconds);

        // 主线程驻留，采集在调度线程持续运行
        Thread.currentThread().join();
    }

    /**
     * 单次采集：读元数据 + 延迟探针 → 映射指标 → 投递。异常向上抛给调度器的 safeRunOnce 隔离，
     * 保证本次失败不影响后续周期（Property 5，验证 4.5）。
     */
    static void collectOnce(MetadataReader reader, MetricsSink sink, LatencyProbe latencyProbe, String table) {
        long collectTs = System.currentTimeMillis();
        
        // 1. 采集 Paimon 元数据
        PaimonTableMetadata metadata;
        try {
            metadata = reader.read();
        } catch (Exception e) {
            // 转为 unchecked 抛给调度器隔离记录
            throw new RuntimeException("读取 Paimon 元数据失败: table=" + table, e);
        }
        List<MetricEnvelope> metrics = MetadataMetricMapper.toMetrics(metadata, table, collectTs);
        
        // 2. 延迟探针：查 MAX(event_time) 算端到端延迟（Requirements 3.2，任务 5.6）
        MetricEnvelope latencyMetric = latencyProbe.probe(collectTs);
        if (latencyMetric != null) {
            metrics.add(latencyMetric);
            LOG.info("延迟探针: {}ms (table={})", (long) latencyMetric.getMetricValue(), table);
        } else {
            LOG.warn("延迟探针跳过（表为空或查询失败）: table={}", table);
        }
        
        // 3. 投递指标
        sink.emit(metrics);
        LOG.info("元数据采集完成: table={} snapshot={} fileCount={} metrics={} (含延迟探针)",
                table, metadata.getSnapshotId(), metadata.getFileCount(), metrics.size());
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
        final String warehouse;
        final String database;
        final String table;
        final int collectIntervalSeconds;
        final String kafkaBootstrap;
        final String kafkaMetricsTopic;
        // Kerberos 配置（可选，为空则不启用）
        final String kerberosPrincipal;
        final String kerberosKeytab;
        final String krb5Conf;

        private CollectorConfig(String warehouse, String database, String table,
                                int collectIntervalSeconds, String kafkaBootstrap,
                                String kafkaMetricsTopic,
                                String kerberosPrincipal, String kerberosKeytab, String krb5Conf) {
            this.warehouse = warehouse;
            this.database = database;
            this.table = table;
            this.collectIntervalSeconds = collectIntervalSeconds;
            this.kafkaBootstrap = kafkaBootstrap;
            this.kafkaMetricsTopic = kafkaMetricsTopic;
            this.kerberosPrincipal = kerberosPrincipal;
            this.kerberosKeytab = kerberosKeytab;
            this.krb5Conf = krb5Conf;
        }

        /** 从 properties 文件（args[0]）、类路径 collector.properties 或系统属性加载，缺必填项即终止并指明缺失参数名。 */
        static CollectorConfig load(String[] args) throws Exception {
            Properties p = loadProperties(args);
            String warehouse = require(p, "warehouse");
            String database = require(p, "database");
            String table = require(p, "table");
            String kafkaBootstrap = require(p, "kafka.bootstrap");
            String kafkaMetricsTopic = require(p, "kafka.metrics.topic");
            int interval = Integer.parseInt(p.getProperty("collect.interval.seconds", "30"));
            if (interval <= 0) {
                throw new IllegalArgumentException("collect.interval.seconds 必须为正，实际为: " + interval);
            }
            // Kerberos 可选配置（未填则非 Kerberos 环境）
            String kerberosPrincipal = optional(p, "kerberos.principal");
            String kerberosKeytab = optional(p, "kerberos.keytab");
            String krb5Conf = optional(p, "kerberos.krb5.conf");
            // 校验：principal 与 keytab 必须成对出现
            boolean hasPrincipal = kerberosPrincipal != null;
            boolean hasKeytab = kerberosKeytab != null;
            if (hasPrincipal != hasKeytab) {
                throw new IllegalArgumentException(
                        "kerberos.principal 与 kerberos.keytab 必须同时配置或同时为空");
            }
            return new CollectorConfig(warehouse, database, table, interval,
                    kafkaBootstrap, kafkaMetricsTopic,
                    kerberosPrincipal, kerberosKeytab, krb5Conf);
        }

        private static String require(Properties p, String key) {
            String v = p.getProperty(key);
            if (v == null || v.trim().isEmpty()) {
                throw new IllegalArgumentException("缺少必填配置项: " + key);
            }
            return v.trim();
        }

        /** 读取可选配置项，缺失或空白返回 null。 */
        private static String optional(Properties p, String key) {
            String v = p.getProperty(key);
            return (v == null || v.trim().isEmpty()) ? null : v.trim();
        }

        /**
         * 按优先级加载配置：显式文件路径 → 类路径 collector.properties → JVM 系统属性。
         *
         * <p>类路径加载支持 {@code -Xbootclasspath/a:conf}（或常规 {@code -cp conf}）把配置目录
         * 挂到类路径，从而无需在命令行显式传文件路径即可读取 {@code collector.properties}。
         */
        private static Properties loadProperties(String[] args) throws Exception {
            Properties p = new Properties();

            // 1. 显式文件路径（args[0]）优先
            if (args != null && args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
                try (java.io.InputStream in = new java.io.FileInputStream(args[0].trim())) {
                    p.load(in);
                    return p;
                }
            }

            // 2. 类路径下的 collector.properties（-Xbootclasspath/a:conf 或 -cp conf）
            try (java.io.InputStream in =
                         CollectorConfig.class.getClassLoader().getResourceAsStream("collector.properties")) {
                if (in != null) {
                    p.load(in);
                    return p;
                }
            }

            // 3. 回退到 JVM 系统属性（-Dkey=value）
            p.putAll(System.getProperties());
            return p;
        }

        @Override
        public String toString() {
            return "CollectorConfig{warehouse='" + warehouse + "', database='" + database
                    + "', table='" + table + "', collectIntervalSeconds=" + collectIntervalSeconds
                    + ", kafkaBootstrap='" + kafkaBootstrap
                    + "', kafkaMetricsTopic='" + kafkaMetricsTopic + "'"
                    + ", kerberos=" + (kerberosPrincipal != null ? "enabled(principal=" + kerberosPrincipal + ")" : "disabled")
                    + "}";
        }
    }
}

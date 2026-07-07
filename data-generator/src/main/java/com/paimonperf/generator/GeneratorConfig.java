package com.paimonperf.generator;

import java.util.Properties;

/**
 * 数据生成器配置，带必填校验与取值边界检查（对应 Requirements 1.6）。
 *
 * <p>配置项（系统属性 -D 或 properties 文件，离线注入）：
 * <ul>
 *   <li>{@code account.total}        账户总数（主键上限），默认 30000000，必须为正</li>
 *   <li>{@code update.ratio}         UPDATE 比例，默认 0.4，必须属 [0,1]</li>
 *   <li>{@code delete.ratio}         DELETE 比例，默认 0.1，必须属 [0,1]</li>
 *   <li>注：INSERT 比例 = 1 - update.ratio - delete.ratio，三者之和必须≤1</li>
 *   <li>{@code rate.limit.enabled}   是否启用限速，默认 false</li>
 *   <li>{@code rate.limit.rps}       限速 RPS（rate.limit.enabled=true 时必填），必须为正</li>
 *   <li>{@code kafka.bootstrap}      Kafka 地址（必填）</li>
 *   <li>{@code kafka.topic}          测试数据 topic（必填）</li>
 *   <li>{@code column.count}         生成列数，默认 100（不可配，硬编码保证与入湖/Paimon sink 一致）</li>
 *   <li>{@code ogg.table}            OGG 信封 table 字段（表来源标识），默认 "db.WIDE_TABLE"</li>
 *   <li>{@code ogg.group.id}         OGG 信封 groupId（源端分片号），默认 "1"</li>
 *   <li>{@code ogg.cluster.name}     OGG 信封 clusterName（源端集群名），默认 "clusterID1"</li>
 * </ul>
 *
 * <p>配置文件加载顺序：
 * <ol>
 *   <li>若 {@code args[0]} 提供且文件存在 → 从该路径文件加载；</li>
 *   <li>否则尝试从<b>类路径</b>加载 {@code generator.properties}（支持
 *       {@code -Xbootclasspath/a:conf} 把配置目录挂到类路径，无需显式传文件路径）；</li>
 *   <li>都没有 → 回退到 JVM 系统属性（{@code -Dkey=value}）。</li>
 * </ol>
 *
 * <p>违规参数在启动时拒绝（Property 4，验证 1.6）：缺必填项或取值越界时抛出含违规参数名的异常。
 */
public final class GeneratorConfig {

    /** 账户总数（主键上限）。 */
    public final long accountTotal;
    /** UPDATE 比例，[0,1]。 */
    public final double updateRatio;
    /** DELETE 比例，[0,1]。 */
    public final double deleteRatio;
    /** 是否启用限速。 */
    public final boolean rateLimitEnabled;
    /** 限速 RPS（rateLimitEnabled=true 时生效）。 */
    public final int rateLimitRps;
    /** Kafka 地址。 */
    public final String kafkaBootstrap;
    /** 测试数据 topic。 */
    public final String kafkaTopic;
    /** 生成列数（固定 100，与入湖/Paimon sink 一致）。 */
    public final int columnCount;
    /** OGG 信封 table 字段（表来源标识）。 */
    public final String oggTable;
    /** OGG 信封 groupId（源端分片号）。 */
    public final String oggGroupId;
    /** OGG 信封 clusterName（源端集群名）。 */
    public final String oggClusterName;

    private GeneratorConfig(long accountTotal, double updateRatio, double deleteRatio,
                            boolean rateLimitEnabled, int rateLimitRps, String kafkaBootstrap,
                            String kafkaTopic, int columnCount,
                            String oggTable, String oggGroupId, String oggClusterName) {
        this.accountTotal = accountTotal;
        this.updateRatio = updateRatio;
        this.deleteRatio = deleteRatio;
        this.rateLimitEnabled = rateLimitEnabled;
        this.rateLimitRps = rateLimitRps;
        this.kafkaBootstrap = kafkaBootstrap;
        this.kafkaTopic = kafkaTopic;
        this.columnCount = columnCount;
        this.oggTable = oggTable;
        this.oggGroupId = oggGroupId;
        this.oggClusterName = oggClusterName;
    }

    /**
     * 从 properties 文件（args[0]）或系统属性加载，非法配置终止并指明违规参数名。
     *
     * @param args 命令行参数（args[0] 为 properties 文件路径，可选）
     * @return 验证通过的配置对象
     * @throws Exception 当配置非法时抛出，消息含违规参数名与具体值
     */
    public static GeneratorConfig load(String[] args) throws Exception {
        Properties p = loadProperties(args);

        // 必填项校验
        String kafkaBootstrap = require(p, "kafka.bootstrap");
        String kafkaTopic = require(p, "kafka.topic");

        // 有默认值的项
        long accountTotal = Long.parseLong(p.getProperty("account.total", "30000000"));
        double updateRatio = Double.parseDouble(p.getProperty("update.ratio", "0.4"));
        double deleteRatio = Double.parseDouble(p.getProperty("delete.ratio", "0.1"));
        boolean rateLimitEnabled = Boolean.parseBoolean(p.getProperty("rate.limit.enabled", "false"));
        int rateLimitRps = 0;
        if (rateLimitEnabled) {
            String rpsStr = p.getProperty("rate.limit.rps");
            if (rpsStr == null || rpsStr.trim().isEmpty()) {
                throw new IllegalArgumentException("rate.limit.enabled=true 时 rate.limit.rps 为必填");
            }
            rateLimitRps = Integer.parseInt(rpsStr);
        }
        int columnCount = Integer.parseInt(p.getProperty("column.count", "100"));

        // OGG 信封元数据（有默认值）
        String oggTable = p.getProperty("ogg.table", "db.WIDE_TABLE").trim();
        String oggGroupId = p.getProperty("ogg.group.id", "1").trim();
        String oggClusterName = p.getProperty("ogg.cluster.name", "clusterID1").trim();

        // 取值边界校验（对应 Property 4）
        if (accountTotal <= 0) {
            throw new IllegalArgumentException("account.total 必须为正，实际为: " + accountTotal);
        }
        if (updateRatio < 0.0 || updateRatio > 1.0) {
            throw new IllegalArgumentException("update.ratio 必须属 [0,1]，实际为: " + updateRatio);
        }
        if (deleteRatio < 0.0 || deleteRatio > 1.0) {
            throw new IllegalArgumentException("delete.ratio 必须属 [0,1]，实际为: " + deleteRatio);
        }
        if (updateRatio + deleteRatio > 1.0) {
            throw new IllegalArgumentException(
                    "update.ratio + delete.ratio 不能超过 1.0，实际为: " + (updateRatio + deleteRatio));
        }
        if (rateLimitEnabled && rateLimitRps <= 0) {
            throw new IllegalArgumentException("rate.limit.rps 必须为正，实际为: " + rateLimitRps);
        }
        if (columnCount != 100) {
            throw new IllegalArgumentException("column.count 必须为 100（与入湖 schema 一致），实际为: " + columnCount);
        }

        return new GeneratorConfig(accountTotal, updateRatio, deleteRatio, rateLimitEnabled,
                rateLimitRps, kafkaBootstrap, kafkaTopic, columnCount,
                oggTable, oggGroupId, oggClusterName);
    }

    /**
     * 按优先级加载配置：显式文件路径 → 类路径 generator.properties → JVM 系统属性。
     *
     * <p>类路径加载支持 {@code -Xbootclasspath/a:conf}（或常规 {@code -cp conf}）把配置目录
     * 挂到类路径，从而无需在命令行显式传文件路径即可读取 {@code generator.properties}。
     *
     * @param args 命令行参数，args[0] 为可选的配置文件路径
     * @return 加载到的配置属性（找不到任何文件时回退到系统属性）
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

        // 2. 类路径下的 generator.properties（-Xbootclasspath/a:conf 或 -cp conf）
        try (java.io.InputStream in =
                     GeneratorConfig.class.getClassLoader().getResourceAsStream("generator.properties")) {
            if (in != null) {
                p.load(in);
                return p;
            }
        }

        // 3. 回退到 JVM 系统属性（-Dkey=value）
        p.putAll(System.getProperties());
        return p;
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
        return "GeneratorConfig{accountTotal=" + accountTotal
                + ", updateRatio=" + updateRatio
                + ", deleteRatio=" + deleteRatio
                + ", rateLimitEnabled=" + rateLimitEnabled
                + ", rateLimitRps=" + rateLimitRps
                + ", kafkaBootstrap='" + kafkaBootstrap + '\''
                + ", kafkaTopic='" + kafkaTopic + '\''
                + ", columnCount=" + columnCount
                + ", oggTable='" + oggTable + '\''
                + ", oggGroupId='" + oggGroupId + '\''
                + ", oggClusterName='" + oggClusterName + '\'' + '}';
    }
}

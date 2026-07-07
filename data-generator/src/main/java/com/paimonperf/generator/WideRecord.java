package com.paimonperf.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * 100 列宽表记录（对应 Design「100 列宽表 schema」）。
 *
 * <p>列分布（需与 Kafka source DDL、Paimon sink DDL 三处保持一致）：
 * <ul>
 *   <li>1 BIGINT 主键：{@code pk}</li>
 *   <li>20 BIGINT：{@code c1_bigint .. c20_bigint}</li>
 *   <li>20 DECIMAL(20,4)：{@code c21_decimal .. c40_decimal}</li>
 *   <li>49 STRING：{@code c41_string .. c89_string}</li>
 *   <li>10 TIMESTAMP(3)：{@code c90_ts .. c99_ts}</li>
 * </ul>
 *
 * <p>外加 {@code event_time} 列（记录产出时刻），随 JSON 写入 Kafka，作为端到端延迟测量的源时间锚
 * （Requirements 3.2，入湖作业需透传该列到 Paimon，不重新赋值）。
 *
 * <p>序列化为 JSON 供 Kafka Producer 写入、Kafka source 读取、Flink 映射到 Paimon sink。
 */
public final class WideRecord {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Random RAND = new Random();

    /** 主键。 */
    public final long pk;
    /** 20 BIGINT 列。 */
    public final long[] bigintCols;
    /** 20 DECIMAL(20,4) 列（用 BigDecimal 表示）。 */
    public final BigDecimal[] decimalCols;
    /** 49 STRING 列（随机 10-50 字符）。 */
    public final String[] stringCols;
    /** 10 TIMESTAMP(3) 列（用 Timestamp 表示）。 */
    public final Timestamp[] tsCols;
    /** 记录产出时刻（端到端延迟源时间锚）。 */
    public final Timestamp eventTime;
    /** 记录类型：INSERT 用新主键，UPDATE 复用历史主键。 */
    public final RecordType type;

    public WideRecord(long pk, Timestamp eventTime, RecordType type) {
        this.pk = pk;
        this.eventTime = eventTime;
        this.type = type;
        this.bigintCols = new long[20];
        this.decimalCols = new BigDecimal[20];
        this.stringCols = new String[49];
        this.tsCols = new Timestamp[10];

        // 填充随机数据
        for (int i = 0; i < 20; i++) {
            bigintCols[i] = RAND.nextLong();
        }
        for (int i = 0; i < 20; i++) {
            decimalCols[i] = BigDecimal.valueOf(RAND.nextDouble() * 1000000).setScale(4, RoundingMode.HALF_UP);
        }
        for (int i = 0; i < 49; i++) {
            stringCols[i] = randomString(10 + RAND.nextInt(41)); // 10-50 字符
        }
        long now = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            tsCols[i] = new Timestamp(now - RAND.nextInt(86400000)); // 最近 24h 内随机时间
        }
    }

    /**
     * 序列化为标准 OGG-JSON 信封（Flink {@code ogg-json} format 兼容）。
     *
     * <p>信封结构：
     * <pre>
     * {
     *   "table": "db.TABLE",
     *   "op_type": "I/U/D",
     *   "current_ts": "yyyy-MM-dd HH:mm:ss.SSS",  // 写入 Kafka 系统时间
     *   "op_ts": "yyyy-MM-dd HH:mm:ss.SSS",       // 源端提交时间（= event_time）
     *   "pos": "1042072462",                       // 队列位置，链路追踪
     *   "ddl": null,
     *   "groupId": "7",
     *   "clusterName": "clusterID1",
     *   "before": {...},   // 更新前镜像；INSERT 时为 null
     *   "after":  {...}    // 更新后镜像；DELETE 时为 null
     * }
     * </pre>
     *
     * <p>{@code ogg-json} 据 {@code op_type + before/after} 推导 Flink RowKind：
     * <ul>
     *   <li>I：before=null, after=data → INSERT</li>
     *   <li>U：before=data, after=data → UPDATE_BEFORE + UPDATE_AFTER</li>
     *   <li>D：before=data, after=null → DELETE</li>
     * </ul>
     * before/after 镜像均含相同主键 {@code pk}，保证 RowKind 变更落到同一行。
     *
     * @param table       表来源标识（如 db.TABLE）
     * @param groupId     源端分片号
     * @param clusterName 源端集群名
     * @param pos         队列位置（链路追踪用，单调递增序号）
     * @return OGG-JSON 信封字符串
     */
    public String toJson(String table, String groupId, String clusterName, long pos) {
        ObjectNode envelope = MAPPER.createObjectNode();
        envelope.put("table", table);
        envelope.put("op_type", oggOpType());
        envelope.put("current_ts", formatOggTs(System.currentTimeMillis())); // Kafka 写入时间
        envelope.put("op_ts", formatOggTs(eventTime.getTime()));             // 源端提交时间锚
        envelope.put("pos", String.valueOf(pos));
        envelope.putNull("ddl");
        envelope.put("groupId", groupId);
        envelope.put("clusterName", clusterName);

        // before/after 镜像：由 op_type 决定哪侧为 null，驱动 ogg-json 生成 RowKind
        switch (type) {
            case INSERT:
                envelope.putNull("before");
                envelope.set("after", buildImage());
                break;
            case UPDATE:
                envelope.set("before", buildImage()); // 更新前镜像（同主键）
                envelope.set("after", buildImage());  // 更新后镜像
                break;
            case DELETE:
                envelope.set("before", buildImage());
                envelope.putNull("after");
                break;
            default:
                throw new IllegalStateException("未知记录类型: " + type);
        }

        try {
            return MAPPER.writeValueAsString(envelope);
        } catch (Exception e) {
            throw new RuntimeException("WideRecord OGG-JSON 序列化失败", e);
        }
    }

    /** op_type 编码：INSERT→I、UPDATE→U、DELETE→D。 */
    private String oggOpType() {
        switch (type) {
            case INSERT: return "I";
            case UPDATE: return "U";
            case DELETE: return "D";
            default: throw new IllegalStateException("未知记录类型: " + type);
        }
    }

    /** OGG 时间戳格式：yyyy-MM-dd HH:mm:ss.SSS（毫秒精度）。SimpleDateFormat 非线程安全，每次新建。 */
    private static String formatOggTs(long millis) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(millis));
    }

    /**
     * 构建一份业务列镜像（100 列 + event_time），用于 before/after。
     * 主键 {@code pk} 始终一致，保证 UPDATE/DELETE 的 RowKind 落到同一行。
     */
    private ObjectNode buildImage() {
        ObjectNode data = MAPPER.createObjectNode();
        data.put("pk", pk);
        for (int i = 0; i < 20; i++) {
            data.put("c" + (i + 1) + "_bigint", bigintCols[i]);
        }
        for (int i = 0; i < 20; i++) {
            data.put("c" + (i + 21) + "_decimal", decimalCols[i].doubleValue());
        }
        for (int i = 0; i < 49; i++) {
            data.put("c" + (i + 41) + "_string", stringCols[i]);
        }
        for (int i = 0; i < 10; i++) {
            data.put("c" + (i + 90) + "_ts", tsCols[i].getTime());
        }
        data.put("event_time", eventTime.getTime()); // 端到端延迟源时间锚，透传到 Paimon
        return data;
    }

    private static String randomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + RAND.nextInt(26)));
        }
        return sb.toString();
    }
}

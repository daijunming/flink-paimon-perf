package com.paimonperf.generator;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Feature: paimon-perf-test 数据生成器属性测试，覆盖 Property 1/2/3。
 *
 * <ul>
 *   <li>Property 1: 生成记录的宽表结构（Validates 1.1）</li>
 *   <li>Property 2: update/insert 比例符合配置（Validates 1.3）</li>
 *   <li>Property 3: update 记录复用历史主键（Validates 1.2、1.5）</li>
 * </ul>
 */
class RecordFactoryPropertyTest {

    /**
     * Feature: paimon-perf-test, Property 1: 生成记录的宽表结构
     *
     * <p>任意生成记录恰有 100 列（1 主键 + 20 BIGINT + 20 DECIMAL + 49 STRING + 10 TIMESTAMP），
     * 且主键存在且非空、event_time 存在、type 存在。JSON 含 op 字段（CDC 格式）。
     *
     * <p>Validates: Requirements 1.1, 7.2
     */
    @Property(tries = 200)
    void generatedRecordHasWideTableStructure(
            @ForAll @LongRange(min = 1, max = 1000000) long accountTotal,
            @ForAll @DoubleRange(min = 0.0, max = 0.5) double updateRatio,
            @ForAll @DoubleRange(min = 0.0, max = 0.3) double deleteRatio) {

        if (updateRatio + deleteRatio > 1.0) {
            return; // 跳过非法组合
        }

        RecordFactory factory = new RecordFactory(accountTotal, updateRatio, deleteRatio);
        WideRecord r = factory.next();

        // 列数检查：各类型列数量符合 schema
        assertEquals(20, r.bigintCols.length, "应有 20 个 BIGINT 列");
        assertEquals(20, r.decimalCols.length, "应有 20 个 DECIMAL 列");
        assertEquals(49, r.stringCols.length, "应有 49 个 STRING 列");
        assertEquals(10, r.tsCols.length, "应有 10 个 TIMESTAMP 列");
        // 1 主键 + 20 + 20 + 49 + 10 = 100 列
        assertEquals(100, 1 + r.bigintCols.length + r.decimalCols.length
                + r.stringCols.length + r.tsCols.length, "总列数应为 100");

        // 主键存在且为正、event_time 存在
        assertTrue(r.pk >= 1, "主键应存在且为正");
        assertNotNull(r.eventTime, "event_time 应存在");
        assertNotNull(r.type, "记录类型应存在");

        // JSON 可序列化且为 OGG 信封（含 table/op_type/before/after，业务列在镜像内含 pk、event_time）
        String json = r.toJson("db.WIDE_TABLE", "1", "clusterID1", 1L);
        assertTrue(json.contains("\"table\""), "JSON 应含 table 字段（OGG 信封）");
        assertTrue(json.contains("\"op_type\""), "JSON 应含 op_type 字段（OGG 信封）");
        assertTrue(json.contains("\"pk\""), "JSON 镜像应含 pk 字段");
        assertTrue(json.contains("\"event_time\""), "JSON 镜像应含 event_time 字段");
        // before/after 至少有一侧存在（INSERT→after、DELETE→before、UPDATE→两者）
        assertTrue(json.contains("\"before\"") && json.contains("\"after\""),
                "OGG 信封应含 before 与 after 字段");
    }

    /**
     * Feature: paimon-perf-test, Property 2: update/insert/delete 比例符合配置
     *
     * <p>足够大样本下，UPDATE/DELETE 占比落在配置 updateRatio/deleteRatio 的统计容差内。
     * 注意：初期主键集合为空时 UPDATE/DELETE 会降级为 INSERT，故用较大样本 + 充足预热摊薄初期偏差。
     *
     * <p>Validates: Requirements 1.3, 7.2
     */
    @Property(tries = 20)
    void updateInsertDeleteRatioMatchesConfig(
            @ForAll @DoubleRange(min = 0.1, max = 0.4) double updateRatio,
            @ForAll @DoubleRange(min = 0.05, max = 0.2) double deleteRatio) {

        // 大 accountTotal 保证不触顶；大样本摊薄统计噪声
        RecordFactory factory = new RecordFactory(100_000_000L, updateRatio, deleteRatio);
        int sample = 50_000;
        int updates = 0;
        int deletes = 0;
        for (int i = 0; i < sample; i++) {
            RecordType type = factory.next().type;
            if (type == RecordType.UPDATE) {
                updates++;
            } else if (type == RecordType.DELETE) {
                deletes++;
            }
        }
        double actualUpdate = (double) updates / sample;
        double actualDelete = (double) deletes / sample;
        // 容差 0.03：初期降级（集合空时 UPDATE/DELETE→INSERT）只影响极少量样本，5万样本下可忽略
        assertTrue(Math.abs(actualUpdate - updateRatio) < 0.03,
                "UPDATE 占比应接近配置: 期望≈" + updateRatio + " 实际=" + actualUpdate);
        assertTrue(Math.abs(actualDelete - deleteRatio) < 0.03,
                "DELETE 占比应接近配置: 期望≈" + deleteRatio + " 实际=" + actualDelete);
    }

    /**
     * Feature: paimon-perf-test, Property 3: update/delete 记录复用历史主键
     *
     * <p>任意 UPDATE/DELETE 记录的主键必属于此前已发出过的主键区间 [1, 当前已发主键数]；
     * INSERT 记录主键不超过 accountTotal 上界。
     *
     * <p>Validates: Requirements 1.2、1.5、7.2
     */
    @Property(tries = 100)
    void updateDeleteReusesHistoricalPkInsertWithinBound(
            @ForAll @LongRange(min = 10, max = 100000) long accountTotal,
            @ForAll @DoubleRange(min = 0.0, max = 0.5) double updateRatio,
            @ForAll @DoubleRange(min = 0.0, max = 0.3) double deleteRatio,
            @ForAll @IntRange(min = 1, max = 2000) int steps) {

        if (updateRatio + deleteRatio > 1.0) {
            return; // 跳过非法组合
        }

        RecordFactory factory = new RecordFactory(accountTotal, updateRatio, deleteRatio);
        long maxEmittedSoFar = 0; // 此前已发出的最大主键（= 已发主键数，因连续递增）

        for (int i = 0; i < steps; i++) {
            WideRecord r = factory.next();
            if (r.type == RecordType.UPDATE || r.type == RecordType.DELETE) {
                // UPDATE/DELETE 主键必属于此前已发出区间 [1, maxEmittedSoFar]
                assertTrue(maxEmittedSoFar >= 1, "出现 UPDATE/DELETE 时此前应已发出过主键");
                assertTrue(r.pk >= 1 && r.pk <= maxEmittedSoFar,
                        r.type + " 主键须属历史区间 [1," + maxEmittedSoFar + "]，实际=" + r.pk);
            } else {
                // INSERT 主键为新值，不超过 accountTotal
                assertTrue(r.pk >= 1 && r.pk <= accountTotal,
                        "INSERT 主键须属 [1," + accountTotal + "]，实际=" + r.pk);
                maxEmittedSoFar = Math.max(maxEmittedSoFar, r.pk);
            }
        }
    }
}

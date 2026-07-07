package com.paimonperf.generator;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.LongRange;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Feature: paimon-perf-test, Property 4: 非法配置在启动时被拒绝
 *
 * <p>对任意非法配置（缺必填项 / account.total≤0 / update.ratio∉[0,1]），
 * {@link GeneratorConfig#load} 必须抛出含违规参数名的异常并终止启动，不留孤立进程。
 *
 * <p>Validates: Requirements 1.6
 */
class GeneratorConfigPropertyTest {

    /**
     * 属性：account.total 非正时拒绝配置。
     */
    @Property(tries = 100)
    void rejectsNonPositiveAccountTotal(
            @ForAll @LongRange(min = Long.MIN_VALUE, max = 0) long accountTotal) throws Exception {

        Properties p = validBase();
        p.setProperty("account.total", String.valueOf(accountTotal));
        File tmp = writeTempProperties(p);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> GeneratorConfig.load(new String[]{tmp.getAbsolutePath()}));
        assertTrue(ex.getMessage().contains("account.total"),
                "异常消息应指明违规参数名 account.total");
        tmp.delete();
    }

    /**
     * 属性：update.ratio 越界 [0,1] 时拒绝配置。
     */
    @Property(tries = 100)
    void rejectsUpdateRatioOutOfRange(
            @ForAll @DoubleRange(min = -100.0, max = 100.0) double ratio) throws Exception {

        if (ratio >= 0.0 && ratio <= 1.0) {
            return; // 合法范围，不测
        }
        Properties p = validBase();
        p.setProperty("update.ratio", String.valueOf(ratio));
        File tmp = writeTempProperties(p);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> GeneratorConfig.load(new String[]{tmp.getAbsolutePath()}));
        assertTrue(ex.getMessage().contains("update.ratio"),
                "异常消息应指明违规参数名 update.ratio");
        tmp.delete();
    }

    /**
     * 属性：缺必填项 kafka.bootstrap / kafka.topic 时拒绝配置。
     */
    @Property(tries = 50)
    void rejectsMissingRequiredFields() throws Exception {
        // 缺 kafka.bootstrap
        Properties p1 = new Properties();
        p1.setProperty("kafka.topic", "test_topic");
        File tmp1 = writeTempProperties(p1);
        IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class,
                () -> GeneratorConfig.load(new String[]{tmp1.getAbsolutePath()}));
        assertTrue(ex1.getMessage().contains("kafka.bootstrap"),
                "缺 kafka.bootstrap 时应指明");
        tmp1.delete();

        // 缺 kafka.topic
        Properties p2 = new Properties();
        p2.setProperty("kafka.bootstrap", "localhost:9092");
        File tmp2 = writeTempProperties(p2);
        IllegalArgumentException ex2 = assertThrows(IllegalArgumentException.class,
                () -> GeneratorConfig.load(new String[]{tmp2.getAbsolutePath()}));
        assertTrue(ex2.getMessage().contains("kafka.topic"),
                "缺 kafka.topic 时应指明");
        tmp2.delete();
    }

    /**
     * 属性：合法配置成功加载。
     */
    @Property(tries = 100)
    void acceptsValidConfig(
            @ForAll @LongRange(min = 1, max = 1000000) long accountTotal,
            @ForAll @DoubleRange(min = 0.0, max = 1.0) double updateRatio) throws Exception {

        Properties p = validBase();
        p.setProperty("account.total", String.valueOf(accountTotal));
        p.setProperty("update.ratio", String.valueOf(updateRatio));
        // 显式置 delete.ratio=0，避免与默认值 0.1 叠加触发 update+delete>1.0 校验
        // （本属性仅验证 update.ratio 在 [0,1] 内的配置可加载）
        p.setProperty("delete.ratio", "0.0");
        File tmp = writeTempProperties(p);

        GeneratorConfig cfg = GeneratorConfig.load(new String[]{tmp.getAbsolutePath()});
        assertNotNull(cfg);
        tmp.delete();
    }

    private static Properties validBase() {
        Properties p = new Properties();
        p.setProperty("kafka.bootstrap", "localhost:9092");
        p.setProperty("kafka.topic", "test_topic");
        return p;
    }

    private static File writeTempProperties(Properties p) throws Exception {
        File tmp = File.createTempFile("gen-cfg-", ".properties");
        try (FileOutputStream out = new FileOutputStream(tmp)) {
            p.store(out, null);
        }
        return tmp;
    }
}

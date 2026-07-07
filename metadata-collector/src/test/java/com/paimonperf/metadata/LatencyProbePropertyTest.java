package com.paimonperf.metadata;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Feature: paimon-perf-test, Property 14: 端到端延迟探针计算正确
 *
 * <p>对任意探针采样时刻 t 与可见 event_time 集合 S（非空），断言输出延迟等于 t − max(S)。
 *
 * <p>Validates: Requirements 3.2
 */
class LatencyProbePropertyTest {

    /**
     * 属性：延迟计算等于 probeTime - maxEventTime。
     *
     * <p>对任意合法时间戳（probeTime >= maxEventTime，因 max 是历史最大值），
     * 延迟必等于两者差值。
     */
    @Property(tries = 200)
    void latencyEqualsProbeTimeMinusMaxEventTime(
            @ForAll @LongRange(min = 1_600_000_000_000L, max = 1_800_000_000_000L) long maxEventTime,
            @ForAll @LongRange(min = 0, max = 3_600_000L) long deltaMs) {

        // probeTime = maxEventTime + delta（保证 probeTime >= maxEventTime）
        long probeTime = maxEventTime + deltaMs;

        long latency = LatencyProbe.calculateLatency(probeTime, maxEventTime);

        // 断言：latency == probeTime - maxEventTime == deltaMs
        assertEquals(deltaMs, latency,
                "延迟应等于 probeTime − maxEventTime");
    }

    /**
     * 边界：probeTime == maxEventTime 时延迟为 0（理想情况：探针时刻恰好有数据写入完成可见）。
     */
    @Property(tries = 50)
    void zeroLatencyWhenProbeTimeEqualsMaxEventTime(
            @ForAll @LongRange(min = 1_600_000_000_000L, max = 1_800_000_000_000L) long timestamp) {

        long latency = LatencyProbe.calculateLatency(timestamp, timestamp);

        assertEquals(0L, latency, "probeTime == maxEventTime 时延迟应为 0");
    }

    /**
     * 边界：probeTime < maxEventTime 时延迟为负（异常场景：时钟回拨或数据乱序，
     * 计算公式仍正确返回负值，由上层决定如何处理）。
     */
    @Property(tries = 50)
    void negativeLatencyWhenProbeTimeBeforeMaxEventTime(
            @ForAll @LongRange(min = 1_600_000_000_000L, max = 1_800_000_000_000L) long maxEventTime,
            @ForAll @LongRange(min = 1_000L, max = 3_600_000L) long backwardMs) {

        long probeTime = maxEventTime - backwardMs;

        long latency = LatencyProbe.calculateLatency(probeTime, maxEventTime);

        assertEquals(-backwardMs, latency,
                "probeTime < maxEventTime 时延迟应为负（异常但计算公式仍正确）");
    }
}

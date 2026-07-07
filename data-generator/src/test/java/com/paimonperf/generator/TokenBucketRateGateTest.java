package com.paimonperf.generator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 限速器单元测试（任务 2.9 的限速边界部分，覆盖 Requirements 1.4）。
 */
class TokenBucketRateGateTest {

    @Test
    void rejectsNonPositiveRps() {
        assertThrows(IllegalArgumentException.class, () -> new TokenBucketRateGate(0));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucketRateGate(-1));
    }

    @Test
    void limitsThroughputToConfiguredRps() {
        // 100 RPS：取 50 个许可应耗时约 0.5s（首个许可立即放行，故约 49 个间隔）
        int rps = 100;
        RateGate gate = new TokenBucketRateGate(rps);
        int permits = 50;

        long start = System.nanoTime();
        for (int i = 0; i < permits; i++) {
            gate.acquire();
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        // 期望约 (permits-1)/rps 秒 = 490ms；放宽下界到 400ms 容纳调度抖动，确实起到了限速
        assertTrue(elapsedMs >= 400,
                "100 RPS 取 50 个许可应至少耗时约 0.4s，实际=" + elapsedMs + "ms");
    }

    @Test
    void unlimitedGateDoesNotBlock() {
        long start = System.nanoTime();
        for (int i = 0; i < 100000; i++) {
            RateGate.UNLIMITED.acquire();
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        // 不限速：10万次 no-op 应在极短时间内完成
        assertTrue(elapsedMs < 1000, "UNLIMITED 不应阻塞，实际=" + elapsedMs + "ms");
    }
}

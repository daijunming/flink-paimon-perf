package com.paimonperf.generator;

/**
 * 令牌桶限速器：以固定速率发放许可，平滑控制记录产出速率（对应 Requirements 1.4）。
 *
 * <p>用 JDK 原生实现（不引入 Guava），保持依赖轻、离线单 jar 简单。算法：按 RPS 计算
 * 每个许可的时间间隔，{@link #acquire()} 计算下一许可的应发时刻并 sleep 到该时刻；
 * 允许突发不超过 1 秒的令牌积累，避免长期偏离目标速率。
 *
 * <p>该类线程安全（acquire 同步），但本采集器为单生产线程，竞争开销可忽略。
 */
public final class TokenBucketRateGate implements RateGate {

    private final long intervalNanos;
    private long nextPermitNanos;

    /**
     * @param rps 目标每秒许可数，必须为正
     */
    public TokenBucketRateGate(int rps) {
        if (rps <= 0) {
            throw new IllegalArgumentException("rps 必须为正，实际为: " + rps);
        }
        this.intervalNanos = 1_000_000_000L / rps;
        this.nextPermitNanos = System.nanoTime();
    }

    @Override
    public synchronized void acquire() {
        long now = System.nanoTime();
        if (now < nextPermitNanos) {
            long sleepNanos = nextPermitNanos - now;
            try {
                long millis = sleepNanos / 1_000_000L;
                int nanos = (int) (sleepNanos % 1_000_000L);
                Thread.sleep(millis, nanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            nextPermitNanos += intervalNanos;
        } else {
            // 已落后于计划：以当前时刻为基准，避免无限追赶导致瞬时突发
            nextPermitNanos = now + intervalNanos;
        }
    }
}

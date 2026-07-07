package com.paimonperf.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 基于 {@link ScheduledExecutorService} 的周期调度器实现。
 *
 * <p>容错关键（Property 5，验证 4.5/5.5）：周期任务在 {@link #safeRunOnce(Runnable, Logger)}
 * 中以 try/catch 包裹执行，单次异常被捕获并记录后吞掉，<b>绝不向调度线程抛出</b>——
 * 因为 {@code ScheduledExecutorService.scheduleAtFixedRate} 一旦任务抛出异常便会
 * 取消后续调度。把异常隔离在 {@code safeRunOnce} 内，即可保证后续周期持续被调度。
 *
 * <p>{@link #safeRunOnce(Runnable, Logger)} 抽为静态可测方法：纯粹的"执行一次并隔离异常"
 * 逻辑，供属性测试（Property 5）在不启动真实定时器的前提下大量随机迭代验证。
 */
public final class ScheduledCollectorScheduler implements CollectorScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledCollectorScheduler.class);

    private final ScheduledExecutorService executor;

    public ScheduledCollectorScheduler() {
        this(Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "collector-scheduler");
            t.setDaemon(true);
            return t;
        }));
    }

    public ScheduledCollectorScheduler(ScheduledExecutorService executor) {
        if (executor == null) {
            throw new IllegalArgumentException("executor 不能为空");
        }
        this.executor = executor;
    }

    @Override
    public void runPeriodically(Runnable task, int intervalSeconds) {
        if (task == null) {
            throw new IllegalArgumentException("task 不能为空");
        }
        if (intervalSeconds <= 0) {
            throw new IllegalArgumentException("intervalSeconds 必须为正，实际为: " + intervalSeconds);
        }
        // 用 safeRunOnce 包裹，确保单次异常不冒泡到调度器、不取消后续周期
        executor.scheduleAtFixedRate(
                () -> safeRunOnce(task, LOG),
                0L,
                intervalSeconds,
                TimeUnit.SECONDS);
    }

    /**
     * 优雅关闭调度器，等待至多 {@code timeoutSeconds} 秒。
     */
    public void shutdown(int timeoutSeconds) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    /**
     * 执行一次任务并隔离异常：任务抛出的任何 {@link Throwable}（含 {@link RuntimeException}、
     * {@link Error}）都被捕获并通过日志记录，方法本身正常返回，绝不向调用方抛出。
     *
     * <p>这是"单次采集失败不中断后续周期"的核心不变量载体（Property 5）。抽为静态方法以便
     * 属性测试在不依赖真实定时器的情况下验证：无论任务在哪些周期抛异常，每个周期都会被执行
     * 且调度循环不被打断。
     *
     * @param task 待执行任务，非空
     * @param log  日志器，非空
     * @return {@code true} 表示本次执行未抛异常；{@code false} 表示捕获到异常（已记录）
     */
    public static boolean safeRunOnce(Runnable task, Logger log) {
        try {
            task.run();
            return true;
        } catch (Throwable t) {
            // 单次失败被隔离：记录错误，吞掉异常，保留后续周期调度
            log.error("采集周期执行失败，已记录并保留后续周期调度: {}", t.toString(), t);
            return false;
        }
    }
}

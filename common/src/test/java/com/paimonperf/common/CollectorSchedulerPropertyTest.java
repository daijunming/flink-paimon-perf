package com.paimonperf.common;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Feature: paimon-perf-test, Property 5: 单次采集失败不中断后续周期
 *
 * <p>对任意在某一采集周期抛出异常的采集任务（元数据或资源采集器），该异常应被捕获并记录，
 * 且后续采集周期仍按配置周期继续被调度。
 *
 * <p>Validates: Requirements 4.5、5.5
 *
 * <p>验证策略：{@link ScheduledCollectorScheduler#safeRunOnce} 是"执行一次并隔离异常"的
 * 不变量载体。用一个布尔序列模拟"每个周期任务是否抛异常"，依次喂给 safeRunOnce，断言：
 * (1) 任一周期抛异常时 safeRunOnce 不向外抛出（返回 false）；
 * (2) 每个周期都被执行（执行计数 == 周期数），即异常不会中断后续周期的调度循环。
 */
class CollectorSchedulerPropertyTest {

    /**
     * 属性：无论失败模式如何，所有周期都被执行，且失败周期被隔离（safeRunOnce 不向外抛）。
     */
    @Property(tries = 200)
    void failureInOneCycleDoesNotStopSubsequentCycles(
            @ForAll @Size(min = 1, max = 50) List<Boolean> failurePattern) {

        AtomicInteger executed = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);

        // 模拟调度循环：依次执行每个周期任务，单次异常须被 safeRunOnce 隔离，循环不被打断
        for (Boolean flag : failurePattern) {
            boolean shouldThrow = Boolean.TRUE.equals(flag);
            Runnable task = () -> {
                executed.incrementAndGet();
                if (shouldThrow) {
                    throw new RuntimeException("模拟采集失败");
                }
            };
            // safeRunOnce 绝不向外抛出；否则此处会中断 for 循环
            boolean ok = ScheduledCollectorScheduler.safeRunOnce(
                    task, LoggerFactory.getLogger(CollectorSchedulerPropertyTest.class));
            if (shouldThrow) {
                assertFalse(ok, "抛异常的周期应返回 false（已被捕获记录）");
                failed.incrementAndGet();
            } else {
                assertTrue(ok, "正常周期应返回 true");
            }
        }

        // 每个周期都被执行 —— 失败周期没有中断后续调度
        assertEquals(failurePattern.size(), executed.get(),
                "所有周期都应被执行，失败不得中断后续周期");
        long expectedFailures = failurePattern.stream().filter(Boolean.TRUE::equals).count();
        assertEquals(expectedFailures, failed.get(), "失败次数应与失败模式一致");
    }

    /**
     * 属性：即便任务抛出的是 Error（非 Exception），调度循环仍不被中断。
     */
    @Property(tries = 100)
    void errorThrowingTaskIsAlsoIsolated(@ForAll @IntRange(min = 1, max = 30) int cycles) {
        AtomicInteger executed = new AtomicInteger(0);
        for (int i = 0; i < cycles; i++) {
            Runnable task = () -> {
                executed.incrementAndGet();
                throw new AssertionError("模拟严重错误");
            };
            boolean ok = ScheduledCollectorScheduler.safeRunOnce(
                    task, LoggerFactory.getLogger(CollectorSchedulerPropertyTest.class));
            assertFalse(ok, "抛 Error 的周期应被捕获并返回 false");
        }
        assertEquals(cycles, executed.get(), "抛 Error 也不应中断后续周期");
    }
}

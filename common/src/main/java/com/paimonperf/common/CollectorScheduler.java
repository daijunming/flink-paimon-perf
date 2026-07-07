package com.paimonperf.common;

/**
 * 周期调度器：按固定周期重复执行采集任务。被元数据采集器、资源采集器、延迟探针共用。
 *
 * <p>核心容错语义（Property 5，验证 4.5/5.5）：任一采集周期内任务抛出的异常
 * 必须被捕获并记录，<b>不得中断后续周期的调度</b>——即单次失败被隔离，调度器持续运行。
 *
 * <p>对应 Design「(c) Paimon 元数据采集器」中的 {@code CollectorScheduler} 接口。
 */
public interface CollectorScheduler {

    /**
     * 按 {@code intervalSeconds} 周期重复执行 {@code task}。
     * 单次执行抛出的异常被捕获并记录，后续周期仍按周期继续被调度。
     *
     * @param task            周期任务，必须非空
     * @param intervalSeconds 采集周期（秒），必须为正
     */
    void runPeriodically(Runnable task, int intervalSeconds);
}

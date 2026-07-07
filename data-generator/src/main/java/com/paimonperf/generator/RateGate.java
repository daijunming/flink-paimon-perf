package com.paimonperf.generator;

/**
 * 速率闸门：限制记录产出速率，使阶段2 能稳定复现目标负载（对应 Requirements 1.4）。
 *
 * <p>{@link #acquire()} 在每条记录发送前调用，放行速率不超过配置 RPS。
 */
public interface RateGate {

    /** 获取一个放行许可；超过目标速率时阻塞等待。 */
    void acquire();

    /**
     * 不限速的空实现：{@code rate.limit.enabled=false} 时使用（阶段1 探吞吐上限）。
     */
    RateGate UNLIMITED = new RateGate() {
        @Override
        public void acquire() {
            // no-op，不限速
        }
    };
}

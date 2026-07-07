package com.paimonperf.common;

import java.util.List;

/**
 * 指标投递接口：将指标信封列表发往既有 Kafka metrics topic。
 * 被元数据采集器、资源采集器、延迟探针共用（Design「(c)/(d)」）。
 *
 * <p>需求来源：4.3、5.3、6.1（写既有指标管道所用的同一 Kafka 集群）。
 *
 * <p>实现 {@link AutoCloseable}，便于以 try-with-resources 管理 Kafka Producer 生命周期，
 * 保证进程退出时正确 flush/close。
 */
public interface MetricsSink extends AutoCloseable {

    /**
     * 将一批指标信封发往 metrics topic。允许传入空列表（视为 no-op）。
     *
     * @param metrics 待发送的指标信封列表，非空（可为空列表）
     */
    void emit(List<MetricEnvelope> metrics);
}

package com.paimonperf.resource;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * REST 调用接口：GET 一个端点并返回解析后的 JSON 根节点。
 *
 * <p>对应 Design「(d) YARN/HDFS 资源采集器」的 {@code RestClient} 接口。把 HTTP 细节
 * 隔离在实现类（{@link HttpRestClient}），使解析逻辑（{@link ResourceMetricParser}）与
 * 采集主流程可在不发真实网络请求的前提下单测。
 */
public interface RestClient {

    /**
     * GET 指定 URL 并返回 JSON 根节点。
     *
     * @param url 完整 REST 端点 URL，非空
     * @return 响应体解析后的 JSON 根节点
     * @throws RestException HTTP 调用失败或响应非 JSON 时抛出（由调用方的周期调度器捕获隔离）
     */
    JsonNode get(String url) throws RestException;
}

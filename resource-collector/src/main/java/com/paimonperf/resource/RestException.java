package com.paimonperf.resource;

/**
 * REST 调用异常：HTTP 请求失败、非 2xx 响应或响应体非法 JSON 时抛出。
 * 由资源采集器主流程向上传播，最终被 CollectorScheduler 的 safeRunOnce 捕获隔离，
 * 保证单次 REST 失败不中断后续采集周期（Property 5，验证 5.5）。
 */
public class RestException extends Exception {

    public RestException(String message) {
        super(message);
    }

    public RestException(String message, Throwable cause) {
        super(message, cause);
    }
}

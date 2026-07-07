package com.paimonperf.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * 基于 JDK 内置 {@link HttpURLConnection} 的 {@link RestClient} 实现。
 *
 * <p>选用内置 HTTP 客户端而非引入第三方库：YARN/HDFS REST 调用简单（GET + JSON），
 * 内置实现零额外依赖、兼容 JDK 8、利于离线单 jar 交付。
 *
 * <p>连接与读取超时可配，避免某次 REST 阻塞拖死采集周期；非 2xx 响应或读取失败抛
 * {@link RestException}，由上层周期调度器隔离。
 */
public final class HttpRestClient implements RestClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;

    public HttpRestClient() {
        this(5000, 10000);
    }

    /**
     * @param connectTimeoutMillis 连接超时（毫秒），必须为正
     * @param readTimeoutMillis    读取超时（毫秒），必须为正
     */
    public HttpRestClient(int connectTimeoutMillis, int readTimeoutMillis) {
        if (connectTimeoutMillis <= 0 || readTimeoutMillis <= 0) {
            throw new IllegalArgumentException("超时必须为正");
        }
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
    }

    @Override
    public JsonNode get(String url) throws RestException {
        if (url == null || url.trim().isEmpty()) {
            throw new RestException("url 不能为空或空白");
        }
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(connectTimeoutMillis);
            conn.setReadTimeout(readTimeoutMillis);
            conn.setRequestProperty("Accept", "application/json");

            int code = conn.getResponseCode();
            if (code < 200 || code >= 300) {
                throw new RestException("REST 调用返回非 2xx 状态: " + code + " url=" + url);
            }
            String body = readBody(conn.getInputStream());
            return MAPPER.readTree(body);
        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            throw new RestException("REST 调用失败: url=" + url, e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private static String readBody(InputStream in) throws Exception {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(in, StandardCharsets.UTF_8))) {
            char[] buf = new char[4096];
            int n;
            while ((n = reader.read(buf)) != -1) {
                sb.append(buf, 0, n);
            }
        }
        return sb.toString();
    }
}

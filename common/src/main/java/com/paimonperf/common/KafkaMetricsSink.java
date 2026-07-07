package com.paimonperf.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * 基于 Kafka Producer 的 {@link MetricsSink} 实现：将指标信封序列化为 JSON
 * 写入既有 metrics topic。与 Kafka 2.x broker 兼容（使用 kafka-clients 2.8.1）。
 *
 * <p>分区键采用 {@code metricId}（每条指标唯一：metricType_metricName_timestamp），
 * 使指标均匀分布到各分区，避免固定值导致数据倾斜。Producer 配置启用 {@code acks=all}
 * 与重试，保证投递可靠性；{@link #close()} 中 flush 并关闭 Producer，避免进程退出时丢数据。
 */
public final class KafkaMetricsSink implements MetricsSink {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsSink.class);

    private final Producer<String, String> producer;
    private final String topic;
    private final MetricEnvelopeSerializer serializer;

    /**
     * 用 bootstrap 与 topic 构造，内部创建默认配置的 Kafka Producer。
     *
     * @param bootstrapServers Kafka 地址，非空
     * @param topic            metrics topic 名，非空
     */
    public KafkaMetricsSink(String bootstrapServers, String topic) {
        this(createProducer(bootstrapServers), topic, new MetricEnvelopeSerializer());
    }

    /**
     * 注入式构造，便于单元/集成测试传入 MockProducer。
     *
     * @param producer   Kafka Producer，非空
     * @param topic      metrics topic 名，非空
     * @param serializer 信封序列化器，非空
     */
    public KafkaMetricsSink(Producer<String, String> producer,
                            String topic,
                            MetricEnvelopeSerializer serializer) {
        if (producer == null) {
            throw new IllegalArgumentException("producer 不能为空");
        }
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("topic 不能为空或空白");
        }
        if (serializer == null) {
            throw new IllegalArgumentException("serializer 不能为空");
        }
        this.producer = producer;
        this.topic = topic;
        this.serializer = serializer;
    }

    private static Producer<String, String> createProducer(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new IllegalArgumentException("bootstrapServers 不能为空或空白");
        }
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 可靠性：全副本确认 + 重试 + 幂等，避免指标丢失或重复
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaProducer<>(props);
    }

    @Override
    public void emit(List<MetricEnvelope> metrics) {
        if (metrics == null) {
            throw new IllegalArgumentException("metrics 不能为空（可传空列表）");
        }
        if (metrics.isEmpty()) {
            return;
        }
        for (MetricEnvelope m : metrics) {
            String json = serializer.toJson(m);
            // 用 metricId 作为分区键：每条指标唯一（metricType_metricName_timestamp），
            // 避免固定值（如 source）导致数据倾斜到少数分区
            String key = m.getMetricId();
            producer.send(new ProducerRecord<>(topic, key, json), (metadata, ex) -> {
                if (ex != null) {
                    LOG.error("指标投递失败 topic={} key={} payload={}", topic, key, json, ex);
                }
            });
        }
        // 每批发送后 flush，确保采集周期内的指标及时落盘到 broker
        producer.flush();
    }

    @Override
    public void close() {
        try {
            producer.flush();
        } finally {
            producer.close();
        }
    }
}

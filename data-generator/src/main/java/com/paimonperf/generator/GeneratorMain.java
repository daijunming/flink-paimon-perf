package com.paimonperf.generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 测试数据生成器主入口（组件 a）。
 *
 * <p>读取并校验配置（非法即终止，Property 4）→ 构造 {@link RecordFactory}（造数+主键复用）
 * 与 {@link RateGate}（可选限速）→ 持续生成 {@link WideRecord} 写入 Kafka test topic，
 * 直到收到停止信号。
 *
 * <p>配置项见 {@link GeneratorConfig}。用法：
 * {@code java -jar data-generator.jar [config.properties]}
 */
public final class GeneratorMain {

    private static final Logger LOG = LoggerFactory.getLogger(GeneratorMain.class);

    private GeneratorMain() {
    }

    public static void main(String[] args) throws Exception {
        // 启动前先校验配置；非法配置在此终止并打印违规参数名（Property 4，验证 1.6）
        GeneratorConfig cfg = GeneratorConfig.load(args);
        LOG.info("启动数据生成器: {}", cfg);

        RecordFactory factory = new RecordFactory(cfg.accountTotal, cfg.updateRatio, cfg.deleteRatio);
        RateGate rateGate = cfg.rateLimitEnabled
                ? new TokenBucketRateGate(cfg.rateLimitRps)
                : RateGate.UNLIMITED;
        Producer<String, String> producer = createProducer(cfg.kafkaBootstrap);

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong sent = new AtomicLong(0);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("收到关闭信号，停止生成并 flush Kafka，已发送 {} 条", sent.get());
            running.set(false);
            try {
                producer.flush();
            } finally {
                producer.close();
            }
        }, "data-generator-shutdown"));

        // 主生成循环：限速 → 造数 → 投递（主键作分区键，使同一主键进同分区保序）
        while (running.get()) {
            rateGate.acquire();
            WideRecord record = factory.next();
            String key = String.valueOf(record.pk);
            // pos 用已发送序号（单调递增），对齐 OGG 队列位置语义，供链路追踪
            long pos = sent.get() + 1;
            String value = record.toJson(cfg.oggTable, cfg.oggGroupId, cfg.oggClusterName, pos);
            producer.send(new ProducerRecord<>(cfg.kafkaTopic, key, value), (md, ex) -> {
                if (ex != null) {
                    LOG.error("记录投递失败 pk={}", key, ex);
                }
            });
            long n = sent.incrementAndGet();
            if (n % 100000 == 0) {
                LOG.info("已发送 {} 条（已发出唯一主键 {}）", n, factory.getEmittedPkCount());
            }
        }
    }

    private static Producer<String, String> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 高吞吐写入：批量 + 压缩 + 较大缓冲；acks=1 兼顾吞吐与可靠（测试数据可容忍极少丢失）
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return new KafkaProducer<>(props);
    }
}

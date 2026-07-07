package com.paimonperf.metadata;

/**
 * Paimon 表元数据读取接口：读取一次目标 Paimon 表的元数据快照，返回与 Paimon API 解耦的
 * {@link PaimonTableMetadata} 中间模型。
 *
 * <p>对应 Design「(c) Paimon 元数据采集器」的 {@code MetadataReader} 接口。把 Paimon 特定
 * 调用隔离在实现类（{@link PaimonSystemTableMetadataReader}），使采集主流程与映射逻辑
 * （{@link MetadataMetricMapper}）可在无真实仓库下单测。
 */
public interface MetadataReader extends AutoCloseable {

    /**
     * 读取一次 Paimon 表元数据快照。
     *
     * @return 当前表的元数据中间模型
     * @throws Exception 读取失败时抛出（由调用方的周期调度器捕获隔离，不中断后续周期）
     */
    PaimonTableMetadata read() throws Exception;
}

package com.paimonperf.generator;

import java.sql.Timestamp;
import java.util.Random;

/**
 * 宽表记录工厂：按 {@code updateRatio/deleteRatio} 决定生成 INSERT/UPDATE/DELETE，
 * UPDATE/DELETE 复用历史主键（对应 Requirements 1.2、1.3、1.5、7.2）。
 *
 * <p>主键分配策略：
 * <ul>
 *   <li><b>INSERT</b>：递增分配新主键（从 1 起），不超过 {@code accountTotal} 上界</li>
 *   <li><b>UPDATE</b>：从已发出主键中随机采样（若尚无已发主键则降级为 INSERT）</li>
 *   <li><b>DELETE</b>：从已发出主键中随机采样（若尚无已发主键则降级为 INSERT）</li>
 * </ul>
 *
 * <p>比例分配（三者之和≤1）：
 * <ul>
 *   <li>随机数 r ∈ [0,1)</li>
 *   <li>r < deleteRatio → DELETE</li>
 *   <li>deleteRatio ≤ r < (deleteRatio + updateRatio) → UPDATE</li>
 *   <li>r ≥ (deleteRatio + updateRatio) → INSERT</li>
 * </ul>
 *
 * <p><b>关键设计</b>：INSERT 分配的是<b>连续递增</b>主键，故"已发出主键集合"恒等于连续区间
 * {@code [1, nextInsertPk)}。UPDATE/DELETE 采样即在该区间随机取一个，O(1) 时间、零额外内存——
 * 无需用 HashSet 保存全部主键（千万级主键下那会是吞吐瓶颈与内存负担）。
 *
 * <p>该类非线程安全（单生产线程调用）。
 */
public final class RecordFactory {

    private final long accountTotal;
    private final double updateRatio;
    private final double deleteRatio;
    private final Random rand;
    /** 下一个待分配的 INSERT 主键；已发出主键恒为连续区间 [1, nextInsertPk)。 */
    private long nextInsertPk;

    /**
     * @param accountTotal 账户总数（主键上限），必须为正
     * @param updateRatio  UPDATE 比例，必须属 [0,1]
     * @param deleteRatio  DELETE 比例，必须属 [0,1]，且 updateRatio + deleteRatio ≤ 1
     */
    public RecordFactory(long accountTotal, double updateRatio, double deleteRatio) {
        if (accountTotal <= 0) {
            throw new IllegalArgumentException("accountTotal 必须为正");
        }
        if (updateRatio < 0.0 || updateRatio > 1.0) {
            throw new IllegalArgumentException("updateRatio 必须属 [0,1]");
        }
        if (deleteRatio < 0.0 || deleteRatio > 1.0) {
            throw new IllegalArgumentException("deleteRatio 必须属 [0,1]");
        }
        if (updateRatio + deleteRatio > 1.0) {
            throw new IllegalArgumentException("updateRatio + deleteRatio 不能超过 1.0");
        }
        this.accountTotal = accountTotal;
        this.updateRatio = updateRatio;
        this.deleteRatio = deleteRatio;
        this.rand = new Random();
        this.nextInsertPk = 1L; // 主键从 1 开始
    }

    /**
     * 生成下一条记录。按 {@code deleteRatio/updateRatio} 决定类型：
     * DELETE/UPDATE 复用历史主键（若有），INSERT 分配新主键（不超过上界）。
     *
     * @return 宽表记录，带主键、event_time、类型
     */
    public WideRecord next() {
        double r = rand.nextDouble();
        boolean hasEmitted = nextInsertPk > 1L;          // 是否已发出过至少一个主键
        boolean atCapacity = nextInsertPk > accountTotal; // 是否已达主键上界

        RecordType type;
        long pk;

        // 三路分支：r < deleteRatio → DELETE; deleteRatio ≤ r < (deleteRatio+updateRatio) → UPDATE; 其余 → INSERT
        if (r < deleteRatio) {
            // DELETE：需要历史主键
            if (hasEmitted) {
                type = RecordType.DELETE;
                pk = sampleEmitted();
            } else {
                // 降级为 INSERT（无可删除主键）
                type = RecordType.INSERT;
                pk = nextInsertPk++;
            }
        } else if (r < deleteRatio + updateRatio) {
            // UPDATE：需要历史主键
            if (hasEmitted) {
                type = RecordType.UPDATE;
                pk = sampleEmitted();
            } else if (!atCapacity) {
                // 降级为 INSERT（无可更新主键）
                type = RecordType.INSERT;
                pk = nextInsertPk++;
            } else {
                throw new IllegalStateException("主键上界已达且无已发主键可复用");
            }
        } else {
            // INSERT：分配新主键
            if (!atCapacity) {
                type = RecordType.INSERT;
                pk = nextInsertPk++;
            } else if (hasEmitted) {
                // 降级为 UPDATE（主键上界已达）
                type = RecordType.UPDATE;
                pk = sampleEmitted();
            } else {
                throw new IllegalStateException("主键上界已达且无已发主键可复用");
            }
        }

        return new WideRecord(pk, new Timestamp(System.currentTimeMillis()), type);
    }

    /** 在已发出主键区间 [1, nextInsertPk) 随机采样一个主键。要求已有至少一个主键。 */
    private long sampleEmitted() {
        // nextInsertPk-1 个已发主键：值域 [1, nextInsertPk)
        long emittedCount = nextInsertPk - 1L;
        return 1L + (long) (rand.nextDouble() * emittedCount);
    }

    /** 已发出的唯一主键数（= nextInsertPk-1），用于测试验证。 */
    public long getEmittedPkCount() {
        return nextInsertPk - 1L;
    }

    /** 下一个待分配的 INSERT 主键，用于测试验证。 */
    public long getNextInsertPk() {
        return nextInsertPk;
    }
}

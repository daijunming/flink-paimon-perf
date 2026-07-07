package com.paimonperf.generator;

/**
 * 记录类型：决定主键来源与CDC操作类型。
 * <ul>
 *   <li>{@link #INSERT}：分配新主键（不超过 account.total 上界），CDC op=c（create）</li>
 *   <li>{@link #UPDATE}：复用已发出过的历史主键，触发对相同主键的更新，CDC op=u（update）</li>
 *   <li>{@link #DELETE}：复用已发出过的历史主键，触发对相同主键的删除，CDC op=d（delete）</li>
 * </ul>
 * 对应 Requirements 1.3（比例）、1.5（UPDATE 复用历史主键）、7.2（DELETE 效率）。
 */
public enum RecordType {
    INSERT,
    UPDATE,
    DELETE
}

#!/usr/bin/env bash
# 06_compaction_job.sh —— 独立 Compaction 作业（Paimon Flink Action，对齐真实环境）
#
# 真实拓扑：写入作业（DataStreamperf_paimon）为 write-only，只写不合并；
# 合并由本独立作业（job_name=compaction_job）通过 paimon-flink-action 的 compact 动作完成。
# 流模式提交时，作业持续监听表的新变更并按需 compaction（详见 Paimon 1.1 Dedicated Compaction 文档）。
#
# 语法核对：https://paimon.apache.org/docs/1.1/maintenance/dedicated-compaction/
# 说明：
#   * -D pipeline.name=compaction_job：让 Flink 作业名 = compaction_job，
#     分析侧（analysis-sql）正是按 job_name='compaction_job' 过滤其 Paimon Compaction Metrics。
#     若平台另有作业命名方式，请确保最终作业名与分析口径一致。
#   * 不传 -D execution.runtime-mode=batch → 默认流模式（持续 compaction）。
#   * HDFS warehouse 无需 --catalog_conf（对象存储才需 s3.* 等）。
#   * compaction 调优（compaction-trigger / merge-max-file-num / write-buffer-*）在这里通过
#     --table_conf 传入——因为真正执行合并的是本作业（写入作业 write-only 下这些参数不生效）。
#   * 同步 compaction 可能引起 checkpoint 超时；如遇到可考虑异步 compaction（见上文档 table_conf）。

set -euo pipefail

FLINK_HOME="${FLINK_HOME:-/opt/flink}"
PAIMON_ACTION_JAR="${PAIMON_ACTION_JAR:-/path/to/paimon-flink-action-1.1.1.jar}"

WAREHOUSE="${PAIMON_WAREHOUSE:?请设置 PAIMON_WAREHOUSE，如 hdfs:///user/<user>/paimon}"
DATABASE="paimon_database"
TABLE="wide_table"

"${FLINK_HOME}/bin/flink" run \
  -D pipeline.name=compaction_job \
  "${PAIMON_ACTION_JAR}" \
  compact \
  --warehouse "${WAREHOUSE}" \
  --database "${DATABASE}" \
  --table "${TABLE}" \
  --table_conf sink.parallelism=3 \
  --table_conf num-sorted-run.compaction-trigger=3 \
  --table_conf write.merge-max-file-num=6 \
  --table_conf write-buffer-spillable=true \
  --table_conf write-buffer-size=64m

# 等价的 Flink SQL 写法（在 sql-client / 平台 SQL 作业里执行，二选一）：
#   CALL sys.compact(
#     `table` => 'paimon_database.wide_table',
#     options => 'sink.parallelism=3,num-sorted-run.compaction-trigger=3,write.merge-max-file-num=6'
#   );

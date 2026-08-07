#!/usr/bin/env bash
# 06_compaction_job.sh —— 独立 Compaction 任务(crontab 周期批形态,对齐真实现場 2026-08-06)
#
# 真实形态:写入作业(DataStreamperf_paimon)write-only 只写不合;合并由 crontab 每 5 分钟
# 提交一次的 BATCH 任务完成(paimon-flink-action compact,作业名 paimon-compact),
# 单轮几十秒跑完即退出。旧描述"流式常驻 compaction_job"已从现场退役。
#
# 观测口径(重要):
#   * 批任务生命周期(几十秒)短于指标上报周期(3 分钟),且作业名不在分析视图白名单,
#     compactionThreadBusy 等作业侧指标在 RDW_ODS_FLINK_METRICS 中无数据——查不到 ≠ 没跑。
#   * 作业侧看本脚本输出的每轮日志(起止/退出码/耗时)与 YARN 应用历史(paimon-compact);
#     合并效果看表侧元数据,分析口径见 docs/写入与合并性能分析.md。
#
# crontab 示例(每 5 分钟):
#   */5 * * * * bash /path/to/06_compaction_job.sh >> /path/to/logs/paimon-compact.log 2>&1
# Kerberos 环境需保证 cron 用户持有效 ticket(HADOOP_CONF_DIR 已指向集群配置)。

set -euo pipefail

export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
export HADOOP_CLASSPATH="$(hadoop classpath)"

FLINK_HOME="${FLINK_HOME:-/opt/flink}"
PAIMON_ACTION_JAR="${PAIMON_ACTION_JAR:-${FLINK_HOME}/task/paimon-flink-action-1.1.1.jar}"
WAREHOUSE="${PAIMON_WAREHOUSE:?请设置 PAIMON_WAREHOUSE,如 hdfs:///user/<user>/paimon}"
DATABASE="paimon_database"
TABLE="wide_table"

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

START=$(date +%s)
log "提交本轮 compact(paimon-compact, BATCH)"

if "${FLINK_HOME}/bin/flink" run-application -t yarn-application \
  -Dexecution.runtime-mode=BATCH \
  -Dyarn.application.name=paimon-compact \
  -Djobmanager.memory.process.size=1536m \
  -Dtaskmanager.memory.process.size=10240m \
  -Dyarn.appmaster.vcores=1 \
  -Dyarn.containers.vcores=4 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  "${PAIMON_ACTION_JAR}" \
  compact \
  --warehouse "${WAREHOUSE}" \
  --database "${DATABASE}" \
  --table "${TABLE}" \
  --compact_strategy full \
  --table-conf changelog-producer=lookup \
  --table-conf changelog-producer.row-deduplicate=true \
  --table-conf scan.split-enumerator.batch-size=1 \
  --table-conf write-buffer-spillable=true \
  --table-conf write-buffer-size=64m \
  --table-conf num-sorted-run.compaction-trigger=3 \
  --table-conf sink.use-managed-memory-allocator=true \
  --table-conf write.merge-max-file-num=6 \
  --table-conf sink.parallelism=3 \
  --table-conf parquet.enable.dictionary=false \
  --table-conf read.batch-size=512; then
  log "本轮 compact 完成,耗时 $(( $(date +%s) - START ))s"
else
  rc=$?
  log "本轮 compact 失败,退出码 ${rc},耗时 $(( $(date +%s) - START ))s"
  exit "${rc}"
fi

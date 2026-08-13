#!/usr/bin/env bash
# 06_compaction_job.sh —— 独立 Compaction 任务(crontab 周期批形态,对齐真实现场;
# 资源参数 2026-08-11 随现场 crontab 复核更新:JM 2048m / TM 15360m / vcores 6 / slots 3)
# 参数勘误(2026-08-11):移除 write.merge-max-file-num——Paimon 1.1.1 CoreOptions
# 无此参数,配置了也被静默忽略;现场 crontab 与写入作业 hint 里的同名参数应一并删除。
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
APP_NAME="paimon-compact"

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

# 防重入两层:cron 到点必触发、自身不串行化,上一轮跑超 5 分钟时下一轮会重复提交,
# YARN 应用积压(2026-08-11 现场已发生)。上一轮未结束则本轮直接跳过——compact 是
# 幂等累积操作,跳过不丢工作,下一轮自然合掉累积的增量。
#   1. flock:防本机周期提交相互踩踏(写法与 collect_once.sh 一致);
#   2. YARN 应用名检查:兜底 flock 覆盖不到的同源路径——attached 客户端异常退出
#      (断连/进程被杀/RM 抖动)时 YARN 应用仍在跑,但脚本进程结束、flock 随之释放,
#      下一轮 cron 会再提交一个。已有未结束的同名应用(运行或排队)时本轮跳过。
# 注意:若旧应用卡死,此后每轮都会在第二层跳过并留日志,需人工 kill 该应用恢复。
LOCK_FILE="${COMPACT_LOCK_FILE:-/tmp/paimon-compact.lock}"
if command -v flock >/dev/null 2>&1; then
  exec 9>"${LOCK_FILE}"
  if ! flock -n 9; then
    log "上一轮 compact 仍在运行,本轮跳过"
    exit 0
  fi
else
  log "警告: 无 flock,本轮不防重入(上一轮超时将重复提交)"
fi

# 第二层防重:YARN 应用名检查(-list 默认只列 NEW/SUBMITTED/ACCEPTED/RUNNING 未结束应用)。
# 查询失败(空输出)放行并提示——RM 真故障时下方提交动作本身会失败暴露。
APP_LIST="$(yarn application -list 2>/dev/null || true)"
if printf '%s' "${APP_LIST}" | grep -q "${APP_NAME}"; then
  log "YARN 上已有未结束的 ${APP_NAME} 应用(运行或排队中),本轮跳过"
  exit 0
fi
if [ -z "${APP_LIST}" ]; then
  log "提示: 未能获取 YARN 应用列表,本轮跳过应用名检查直接提交"
fi

START=$(date +%s)
log "提交本轮 compact(${APP_NAME}, BATCH)"

# attached=true:flock 只在脚本进程存活期间有效,客户端必须等作业结束才返回;
# 顺带让下方的耗时/退出码日志反映作业真实结果,而非提交动作本身。
if "${FLINK_HOME}/bin/flink" run-application -t yarn-application \
  -Dexecution.runtime-mode=BATCH \
  -Dexecution.attached=true \
  -Dyarn.application.name="${APP_NAME}" \
  -Djobmanager.memory.process.size=2048m \
  -Dtaskmanager.memory.process.size=15360m \
  -Dyarn.appmaster.vcores=1 \
  -Dyarn.containers.vcores=6 \
  -Dtaskmanager.numberOfTaskSlots=3 \
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
  --table-conf sink.parallelism=3 \
  --table-conf parquet.enable.dictionary=false \
  --table-conf read.batch-size=512; then
  log "本轮 compact 完成,耗时 $(( $(date +%s) - START ))s"
else
  rc=$?
  log "本轮 compact 失败,退出码 ${rc},耗时 $(( $(date +%s) - START ))s"
  exit "${rc}"
fi

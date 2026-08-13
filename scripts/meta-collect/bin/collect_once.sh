#!/usr/bin/env bash
# collect_once.sh —— Paimon 元数据单轮采集(crontab 周期触发,默认每 3 分钟)
#
# 简化版(趋势观测优先,无本地状态):
#   * $snapshots/$statistics 每轮全量重采未过期部分;$files/$manifests/partitions/buckets
#     每轮采集当前最新 Snapshot,source_snapshot_id 由作业内 MAX(snapshot_id) 打标。
#   * SR 全部 PRIMARY KEY 表:重复采集 = 主键覆盖,天然幂等;极小概率的跨源误标
#     下一轮自愈。因此无需本地游标、无需 HDFS 回读、无需重放流程。
#   * 锁仅用于防止异常时 YARN 作业堆积,与正确性无关(重叠只产生幂等重复)。
#
# 单轮流程:
#   1. flock 防堆积(上一轮未结束则本轮跳过;无 flock 则告警后继续)
#   2. 渲染并提交 10_collect_main(快照/统计全量 + 文件/manifest/分区/bucket 当前态,单作业)
#   3. 渲染并提交 20_collect_sampling(consumers + options,失败仅告警)
#   4. 写 rdw_ods_paimon_meta_collect_runs(经 kafka-console-producer,失败仅告警)
#
# 用法: bash collect_once.sh /path/to/meta-collect.properties
# crontab 示例(每 3 分钟):
#   */3 * * * * bash /path/to/scripts/meta-collect/bin/collect_once.sh /path/to/meta-collect.properties >> /path/to/logs/meta-collect.log 2>&1
# 注意:Kerberos 环境请保证 cron 用户持有有效 ticket(或用 kinit -kt 包装本脚本)。
# 产物:渲染后的 SQL 落 state/rendered/(供复核),每轮开头自动清理超期文件,
#      默认保留 3 天(RENDERED_RETENTION_DAYS 可调);清理失败不影响采集。

set -euo pipefail

CONF="${1:?用法: bash collect_once.sh <meta-collect.properties 路径>}"
# shellcheck disable=SC1090
source "${CONF}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TPL_DIR="${SCRIPT_DIR}/../flink-sql"
WORK_DIR="${WORK_DIR:-${SCRIPT_DIR}/../state}"
mkdir -p "${WORK_DIR}/rendered"

# rendered SQL 超期清理(保留期供复核;失败放行,不因清理影响采集)
find "${WORK_DIR}/rendered" -name '*.sql' -mtime +"${RENDERED_RETENTION_DAYS:-3}" -delete 2>/dev/null || true

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

# ---------- 必填配置检查(fail-fast,与 Java 组件配置契约一致)----------
for v in PAIMON_WAREHOUSE PAIMON_CATALOG PAIMON_DATABASE PAIMON_TABLE \
         KAFKA_BOOTSTRAP_SERVERS FLINK_SQL_CLIENT \
         TOPIC_SNAPSHOTS TOPIC_STATISTICS TOPIC_FILES TOPIC_MANIFESTS \
         TOPIC_PARTITIONS TOPIC_BUCKETS TOPIC_CONSUMERS TOPIC_OPTIONS \
         TOPIC_RUNS; do
  if [ -z "${!v:-}" ]; then
    log "缺少必填配置: ${v}(见 conf/meta-collect.properties.template)"
    exit 1
  fi
done

# ---------- flock 防堆积(仅防 YARN 作业堆积,与正确性无关)----------
if command -v flock >/dev/null 2>&1; then
  exec 9>"${WORK_DIR}/.collect.lock"
  if ! flock -n 9; then
    log "上一轮仍在运行,本轮跳过"
    exit 0
  fi
else
  log "提示: 无 flock,本轮不防重入(重叠仅产生幂等重复,但异常时可能堆积 YARN 作业)"
fi

RUN_ID="$(date '+%Y%m%d%H%M%S')-$$"
START_TS="$(date '+%Y-%m-%d %H:%M:%S')"
SED_FILE="${WORK_DIR}/.render.${RUN_ID}.sed"
trap 'rm -f "${SED_FILE}"' EXIT

# ---------- 模板渲染(sed 占位符替换;转义 & | \ 防止值破坏 sed 表达式)----------
add_sed() { # $1=占位符名(不含 ${}) $2=值
  local esc
  esc="$(printf '%s' "$2" | sed -e 's/[\\&|]/\\&/g')"
  printf 's|${%s}|%s|g\n' "$1" "${esc}" >> "${SED_FILE}"
}

: > "${SED_FILE}"
add_sed PAIMON_WAREHOUSE       "${PAIMON_WAREHOUSE}"
add_sed PAIMON_CATALOG         "${PAIMON_CATALOG}"
add_sed PAIMON_DATABASE        "${PAIMON_DATABASE}"
add_sed PAIMON_TABLE           "${PAIMON_TABLE}"
add_sed KAFKA_BOOTSTRAP_SERVERS "${KAFKA_BOOTSTRAP_SERVERS}"
add_sed KAFKA_WITH_EXTRA       "${KAFKA_WITH_EXTRA:-}"
add_sed TOPIC_SNAPSHOTS        "${TOPIC_SNAPSHOTS}"
add_sed TOPIC_STATISTICS       "${TOPIC_STATISTICS}"
add_sed TOPIC_FILES            "${TOPIC_FILES}"
add_sed TOPIC_MANIFESTS        "${TOPIC_MANIFESTS}"
add_sed TOPIC_PARTITIONS       "${TOPIC_PARTITIONS}"
add_sed TOPIC_BUCKETS          "${TOPIC_BUCKETS}"
add_sed TOPIC_CONSUMERS        "${TOPIC_CONSUMERS}"
add_sed TOPIC_OPTIONS          "${TOPIC_OPTIONS}"
add_sed COLLECTOR_RUN_ID       "${RUN_ID}"

render_tpl() { # $1=模板文件名 $2=输出文件
  sed -f "${SED_FILE}" "${TPL_DIR}/$1" > "$2"
}

submit_sql() { # $1=渲染后 SQL 文件 $2=步骤名
  log "提交 Flink 批作业[$2]: $1"
  # FLINK_SUBMIT_OPTS 需要按空格分词,不加引号
  "${FLINK_SQL_CLIENT}" ${FLINK_SUBMIT_OPTS:-} -f "$1"
}

# ---------- collect_runs 上报(失败仅告警)----------
# 简化版不回填 previous/latest_snapshot_id(恒 NULL,可经 collector_run_id 关联 snapshots 表)。
send_runs() { # $1=status $2=error_message(固定英文 token,不含引号)
  local json end_ts
  end_ts="$(date '+%Y-%m-%d %H:%M:%S')"
  json="$(printf '{"collector_run_id":"%s","catalog_name":"%s","database_name":"%s","table_name":"%s","scheduled_time":"%s","start_time":"%s","end_time":"%s","status":"%s","error_message":"%s"}' \
    "${RUN_ID}" "${PAIMON_CATALOG}" "${PAIMON_DATABASE}" "${PAIMON_TABLE}" \
    "${START_TS}" "${START_TS}" "${end_ts}" "$1" "$2")"
  local producer="${KAFKA_CONSOLE_PRODUCER:-kafka-console-producer.sh}"
  if command -v "${producer}" >/dev/null 2>&1; then
    printf '%s\n' "${json}" | "${producer}" \
      --broker-list "${KAFKA_BOOTSTRAP_SERVERS}" --topic "${TOPIC_RUNS}" \
      ${KAFKA_CLIENT_PROPS:+--producer.config "${KAFKA_CLIENT_PROPS}"} >/dev/null 2>&1 \
      || log "警告: collect_runs 写入 Kafka 失败"
  else
    log "警告: 找不到 kafka-console-producer,runs 记录仅落日志: ${json}"
  fi
}

# ---------- 采集步骤 ----------
# consumers(消费进度)+ options(表显式配置)按时间采样,独立于主采集单独成败:
# 这两类是辅助观测事实,挂了只在 runs 记 token,不把整轮判 FAILED。
collect_sampling() {
  local sql="${WORK_DIR}/rendered/${RUN_ID}-20_collect_sampling.sql"
  render_tpl 20_collect_sampling.sql.tpl "${sql}"
  submit_sql "${sql}" sampling
}

# ---------- 主流程:主采集 + 时间采样 ----------
log "本轮 ${RUN_ID}: 开始采集 ${PAIMON_DATABASE}.${PAIMON_TABLE}"
SQL_MAIN="${WORK_DIR}/rendered/${RUN_ID}-10_collect_main.sql"
render_tpl 10_collect_main.sql.tpl "${SQL_MAIN}"

STATUS="OK"
ERR=""
if submit_sql "${SQL_MAIN}" main; then
  log "主采集完成"
else
  STATUS="FAILED_MAIN_JOB"
  ERR="main_job_failed"
fi

collect_sampling || ERR="${ERR:+$ERR,}sampling_failed"
send_runs "${STATUS}" "${ERR}"

[ "${STATUS}" = "OK" ] && exit 0 || exit 1

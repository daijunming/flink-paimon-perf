#!/usr/bin/env bash
# collect_physical.sh —— Paimon 表物理维度采集(crontab 周期触发,建议每 10 分钟)
#
# 定位:补主链路(Paimon 系统表)拿不到的物理事实——表目录真实占用、changelog 文件
# 数/体积、数据文件物理合计。$files 只含当前态数据文件(Paimon 1.1 无 file_source 列),
# changelog 文件(changelog-producer 产物)只能从 HDFS 文件名前缀(changelog-*)统计。
#
# 产出:每轮一行 JSON → Kafka topic rdw_ods_paimon_meta_physical → Routine Load →
# SR 表 rdw_ods_paimon_meta_physical(建表与装载见 sr/04_physical_ods.sql)。
# "总占用 vs 有效数据"的包袱拆解在分析层做(口径见 docs/写入与合并性能分析.md 3.2),
# 本脚本只采可复核事实。
#
# 用法: bash collect_physical.sh /path/to/meta-collect.properties
# crontab 示例(每 10 分钟;ls -R 开销随表文件数增长,勿套用主链路的 3 分钟):
#   */10 * * * * bash /path/to/scripts/meta-collect/bin/collect_physical.sh /path/to/meta-collect.properties >> /path/to/logs/meta-physical.log 2>&1
# 注意:Kerberos 环境请保证 cron 用户持有有效 ticket(hdfs CLI 与 kafka producer 都需要)。

set -euo pipefail

CONF="${1:?用法: bash collect_physical.sh <meta-collect.properties 路径>}"
# shellcheck disable=SC1090
source "${CONF}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${WORK_DIR:-${SCRIPT_DIR}/../state}"
mkdir -p "${WORK_DIR}"

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

# ---------- 必填配置检查(fail-fast,与 collect_once.sh 契约一致)----------
for v in PAIMON_WAREHOUSE PAIMON_CATALOG PAIMON_DATABASE PAIMON_TABLE \
         KAFKA_BOOTSTRAP_SERVERS TOPIC_PHYSICAL; do
  if [ -z "${!v:-}" ]; then
    log "缺少必填配置: ${v}(见 conf/meta-collect.properties.template)"
    exit 1
  fi
done
if ! command -v hdfs >/dev/null 2>&1; then
  log "缺少 hdfs 命令(需在有 HDFS 客户端的环境运行)"
  exit 1
fi

# ---------- flock 防堆积(只防 HDFS 响应慢时多轮 ls -R 叠加压 NN;重叠采集本身幂等)----------
if command -v flock >/dev/null 2>&1; then
  exec 9>"${WORK_DIR}/.collect-physical.lock"
  if ! flock -n 9; then
    log "上一轮仍在运行,本轮跳过"
    exit 0
  fi
else
  log "提示: 无 flock,本轮不防重入"
fi

RUN_ID="$(date '+%Y%m%d%H%M%S')-$$"
COLLECTED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
# Paimon warehouse 目录布局:<warehouse>/<database>.db/<table>
TABLE_PATH="${PAIMON_WAREHOUSE%/}/${PAIMON_DATABASE}.db/${PAIMON_TABLE}"

# ---------- du:目录真实占用(Hadoop3 输出"逻辑大小 含副本大小 路径";老版本两列则两值相同)----------
if ! DU_OUT="$(hdfs dfs -du -s "${TABLE_PATH}" 2>&1)"; then
  log "错误: hdfs du 失败(路径 ${TABLE_PATH}): ${DU_OUT}"
  exit 1
fi
read -r TOTAL_BYTES TOTAL_BYTES_REP <<<"$(printf '%s\n' "${DU_OUT}" | awk '{if (NF>=3) print $1, $2; else print $1, $1}')"

# ---------- ls -R:按 Paimon 文件名前缀分类聚合(data-* / changelog-* / 其余)----------
# 其余 = manifest/snapshot/schema/index 等元数据文件;data_bytes 含快照保留窗口内旧文件
# 与孤儿文件,恒不小于 $files 当前态合计(差值即历史包袱,分析层拆解)。
if ! LS_STATS="$(hdfs dfs -ls -R "${TABLE_PATH}" | awk '
  $1 ~ /^-/ {
    size=$5; name=$8; sub(/.*\//, "", name)
    files++; bytes+=size
    if (name ~ /^changelog-/)      { cf++; cb+=size }
    else if (name ~ /^data-/)      { df++; db+=size }
    else                           { of_++; ob+=size }
  }
  END { printf "%d %d %d %d %d %d %d %d", files+0, bytes+0, cf+0, cb+0, df+0, db+0, of_+0, ob+0 }')"; then
  log "错误: hdfs ls -R 失败(路径 ${TABLE_PATH})"
  exit 1
fi
read -r FILE_COUNT LS_BYTES CHANGELOG_COUNT CHANGELOG_BYTES \
      DATA_COUNT DATA_BYTES OTHER_COUNT OTHER_BYTES <<<"${LS_STATS}"

JSON="$(printf '{"collector_run_id":"%s","catalog_name":"%s","database_name":"%s","table_name":"%s","collected_at":"%s","total_bytes":%d,"total_bytes_replicated":%d,"file_count_total":%d,"data_file_count":%d,"data_bytes":%d,"changelog_file_count":%d,"changelog_bytes":%d,"other_file_count":%d,"other_bytes":%d}' \
  "${RUN_ID}" "${PAIMON_CATALOG}" "${PAIMON_DATABASE}" "${PAIMON_TABLE}" "${COLLECTED_AT}" \
  "${TOTAL_BYTES}" "${TOTAL_BYTES_REP}" "${FILE_COUNT}" \
  "${DATA_COUNT}" "${DATA_BYTES}" "${CHANGELOG_COUNT}" "${CHANGELOG_BYTES}" \
  "${OTHER_COUNT}" "${OTHER_BYTES}")"

# ---------- 上报 Kafka(失败仅告警,事实落日志不丢;与 collect_runs 同一容错约定)----------
PRODUCER="${KAFKA_CONSOLE_PRODUCER:-kafka-console-producer.sh}"
if command -v "${PRODUCER}" >/dev/null 2>&1; then
  printf '%s\n' "${JSON}" | "${PRODUCER}" \
    --broker-list "${KAFKA_BOOTSTRAP_SERVERS}" --topic "${TOPIC_PHYSICAL}" \
    ${KAFKA_CLIENT_PROPS:+--producer.config "${KAFKA_CLIENT_PROPS}"} >/dev/null 2>&1 \
    || log "警告: 写入 Kafka 失败,本行仅落日志: ${JSON}"
else
  log "警告: 找不到 kafka-console-producer,本行仅落日志: ${JSON}"
fi

log "本轮 ${RUN_ID}: total=${TOTAL_BYTES}B(含副本 ${TOTAL_BYTES_REP}B) data=${DATA_COUNT}个/${DATA_BYTES}B changelog=${CHANGELOG_COUNT}个/${CHANGELOG_BYTES}B other=${OTHER_COUNT}个/${OTHER_BYTES}B"

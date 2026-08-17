#!/usr/bin/env bash
# collect_kafka_offsets.sh —— Kafka topic 末端位移周期采集(crontab,建议 */1 分钟)
#
# 定位:采集"各分区 latest offset"原始事实 → Kafka → SR 表 rdw_ods_kafka_topic_offsets;
# 生产速率(条/秒)由分析层视图 v_kafka_topic_write_rate 对相邻采样做"位移差/实际间隔秒"
# 得到(与 analysis-sql 吞吐同一口径:周期抖动/漏轮不影响速率正确性)。
# 本脚本无跨轮状态,任意轮失败等下一轮即可;单 topic 失败不影响其他 topic(失败隔离)。
#
# 用法: bash collect_kafka_offsets.sh /path/to/meta-collect.properties
# crontab 示例(每 1 分钟,crontab 最小粒度;速率即 ≈60s 窗口均值):
#   */1 * * * * bash /path/to/scripts/meta-collect/bin/collect_kafka_offsets.sh /path/to/meta-collect.properties >> /path/to/logs/kafka-offsets.log 2>&1
# 注意:Kerberos 环境需 cron 用户持有效 ticket,且 KAFKA_CLIENT_PROPS 指向含 SASL 属性的
# 客户端配置;老版本 Kafka 的 GetOffsetShell 若无 --command-config 参数,
# 改用 KAFKA_OPTS 挂 JAAS 并去掉该参数。

set -euo pipefail

CONF="${1:?用法: bash collect_kafka_offsets.sh <meta-collect.properties 路径>}"
# shellcheck disable=SC1090
source "${CONF}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${WORK_DIR:-${SCRIPT_DIR}/../state}"
mkdir -p "${WORK_DIR}"

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

# ---------- 必填配置检查(fail-fast,与 collect_once.sh 契约一致)----------
for v in KAFKA_BOOTSTRAP_SERVERS KAFKA_OFFSET_TOPICS TOPIC_OFFSETS; do
  if [ -z "${!v:-}" ]; then
    log "缺少必填配置: ${v}(见 conf/meta-collect.properties.template)"
    exit 1
  fi
done
RUN_CLASS="${KAFKA_RUN_CLASS:-kafka-run-class.sh}"
if ! command -v "${RUN_CLASS}" >/dev/null 2>&1; then
  log "缺少 ${RUN_CLASS}(需在有 Kafka 客户端的环境运行)"
  exit 1
fi

# ---------- flock 防堆积(采样本身秒级;防的是 Kafka 无响应时多轮叠加)----------
if command -v flock >/dev/null 2>&1; then
  exec 9>"${WORK_DIR}/.collect-kafka-offsets.lock"
  if ! flock -n 9; then
    log "上一轮仍在运行,本轮跳过"
    exit 0
  fi
else
  log "提示: 无 flock,本轮不防重入"
fi

RUN_ID="$(date '+%Y%m%d%H%M%S')-$$"
COLLECTED_AT="$(date '+%Y-%m-%d %H:%M:%S')"

# ---------- 逐 topic 采样(GetOffsetShell 输出行形如 topic:分区:位移)----------
ROWS=""
FAILED=0
for TOPIC in $(printf '%s' "${KAFKA_OFFSET_TOPICS}" | tr ',' ' '); do
  if ! OUT="$("${RUN_CLASS}" kafka.tools.GetOffsetShell \
      --broker-list "${KAFKA_BOOTSTRAP_SERVERS}" --topic "${TOPIC}" --time -1 \
      ${KAFKA_CLIENT_PROPS:+--command-config "${KAFKA_CLIENT_PROPS}"})"; then
    log "错误: 采集 topic ${TOPIC} 位移失败(上方 stderr 有详情)"
    FAILED=1
    continue
  fi
  while IFS=: read -r T P O; do
    [ -n "${T}" ] || continue
    ROWS="${ROWS}$(printf '{"collector_run_id":"%s","topic":"%s","partition_id":%s,"collected_at":"%s","end_offset":%s}\n' \
      "${RUN_ID}" "${T}" "${P}" "${COLLECTED_AT}" "${O}")"
  done <<<"$(printf '%s\n' "${OUT}" | grep -E '^[^:]+:[0-9]+:[0-9]+$' || true)"
done

if [ -z "${ROWS}" ]; then
  log "错误: 本轮无任何分区位移采到"
  exit 1
fi

# ---------- 上报 Kafka(失败仅告警,事实落日志不丢)----------
PRODUCER="${KAFKA_CONSOLE_PRODUCER:-kafka-console-producer.sh}"
if command -v "${PRODUCER}" >/dev/null 2>&1; then
  printf '%s' "${ROWS}" | "${PRODUCER}" \
    --broker-list "${KAFKA_BOOTSTRAP_SERVERS}" --topic "${TOPIC_OFFSETS}" \
    ${KAFKA_CLIENT_PROPS:+--producer.config "${KAFKA_CLIENT_PROPS}"} >/dev/null 2>&1 \
    || log "警告: 写入 Kafka 失败,本轮仅落日志: ${ROWS}"
else
  log "警告: 找不到 kafka-console-producer,本轮仅落日志: ${ROWS}"
fi

log "本轮 ${RUN_ID}: $(printf '%s' "${ROWS}" | grep -c .) 个分区位移已采集"
[ "${FAILED}" = "0" ] && exit 0 || exit 1

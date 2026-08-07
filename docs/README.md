# 文档导读

**核心任务**：实时场景下，对 Paimon 表的**写入、读取、治理（compact）**三类任务的运行状况
进行分析，获取性能表现参数指标，支撑 SLA 判定（写入吞吐 ≥ 20000 条/秒、端到端延迟 ≤ 3 分钟）
与瓶颈定位。

现场推进按此顺序使用文档：

| 步骤 | 文档 | 干什么 |
|------|------|--------|
| 1 | `性能测试需求.md` | 对齐目标与判定标准：测什么、什么叫达标 |
| 2 | `开测前检查清单.md` | 环境/链路/组件逐项就绪检查，不过项不开测 |
| 3 | AGENTS.md 第七节 + 各目录 README | 部署运行，让数据流动起来 |
| 4 | `观测指标地图.md` | 五个观测场景各自看什么指标、取数路径、健康标准 |
| 5 | `写入与合并性能分析.md` | 写入与治理（compact）两条线的分析口径与判读 |
| 6 | `analysis-sql/09_streaming_read.sql` + `analysis-sql/README.md` | 读取线分析（流式读作业指标） |
| 7 | `测试报告模板.md` | 按场景记录结果、下结论 |

## 四线结构（文档与脚本各就各位）

- **写**：`data-generator`（数据生成）→ Kafka → `DataStreamperf_paimon` 写入作业
  （`scripts/sql/`，write-only 只写不合）
- **读**：`scripts/sql/07_streaming_read.sql` / `08_streaming_agg.sql`（`streaming_read_job`），
  分析见 `analysis-sql/09_streaming_read.sql`
- **治理**：crontab 批 `paimon-compact`（`scripts/sql/06_compaction_job.sh`，每 5 分钟一轮）；
  元数据历史采集 `scripts/meta-collect/`（crontab 每 3 分钟批采 Paimon 系统表 →
  StarRocks `rdw_ods_paimon_meta_*` 表）
- **观测底座**：`metadata-collector` / `resource-collector` → `RDW_ODS_FLINK_METRICS` →
  `analysis-sql/` 视图（01 底座、02 四类指标、05 健康标志、08 checkpoint 健康、09 流式读）

## 参考手册（备查，非推进路径）

- `指标与告警手册.md` — Paimon 1.1 原生指标全量清单与告警规则
- `Paimon表配置设计.md` — 读写合并分离架构下的表配置设计讨论

## archive/(已完成使命，仅历史参考，不作为现行工作依据)

- `OGG格式兼容与验证方法.md` — OGG-JSON 兼容性结论与流式验证方法（格式确认已完成）
- `流式分层计算讲义.md` — Flink+Paimon 流式分层通用机制讲义

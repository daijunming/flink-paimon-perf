# Paimon 流式分层计算讲义（Flink 1.19 + Paimon 1.1.1）

> 适用版本：Apache Flink 1.19、Apache Paimon 1.1.1（jar: `paimon-flink-1.19-1.1.1.jar`）。
> Paimon 1.1 官方支持 Flink 1.15 / 1.16 / 1.17 / 1.18 / 1.19 / 1.20。
> 主题：以 Paimon 主键表为中间存储，在**纯 Flink 流式计算**场景下做 ODS→DWD→DWS→ADS 的近实时分层计算。
>
> 版本适用性说明：文中所有 Paimon 选项语法（`changelog-producer`、`scan.mode`、`consumer-id`、`precommit-compact`、`scan.remove-normalize` 等）均已对照 **Paimon 1.1 官方文档**核对；HBase connector 选项对照 **Flink 1.19** 文档核对。其余内容（checkpoint 可见性、RowKind、lookup join、`upsert-kafka` 等）是 Flink/Paimon 的通用机制，跨小版本稳定。

## 讲义结构

本讲义分两部分：

- **第 0 节 · 关键认知**：为什么是 Flink + Paimon，它改变了什么。先建立整体认知，后面的机制才能衔接到主线上。
- **第一部分 · 核心知识点**（第 1–8 节）：机制、参数、三种消费范式、延迟模型、运维。先建立心智模型。
- **第二部分 · 完整业务场景案例**（第 9–15 节）：以「实时财务状态平台」为主线，把核心知识点串成一条端到端的流式分层链路。
- **专题与附录**（第 16–17 节、附录）：changelog 深水区、join 行为与优化全景、速记表、Checklist、**配置总表（附录 D）**、**特性组合矩阵与场景适配（附录 E）**。

> 阅读建议：第一次读按顺序，先读第 0 节建立框架；**做方案时先翻附录 E「场景→组合配方」选型、附录 D 查单配置影响面**，遇到概念再回查第一部分与专题。
>
> 关键提醒：用好 Paimon+Flink 的难点不在单个配置，而在**多特性组合的效应与场景适配**——这部分集中在**附录 E**，是本讲义的实战落点。

---
---

# 第 0 节 · 关键认知：为什么是 Flink + Paimon

> 这一节不讲语法，只立认知框架。后面所有章节都是这几条认知的具体展开。

## 0.0 方法论：范式驱动，一点带面

> 本讲义为什么按「三范式」组织、该怎么用——这套方法也适用于学习其它复杂技术组件。

**核心矛盾：需求范式有限，方案组合很多。** 配置有几十个、组合近乎无穷、还随版本漂移；但**真正的核心需求范式只有少数几个、且高度稳定**。

**从功能清单出发学，只会记住一堆孤立选项，容易停留在死记硬背**：知道有哪些功能，却不知道这些功能**为什么存在、在什么场景下有用、解决了链路中的哪个问题**。这种掌握经不起追问，也用不到点上。

**更有效的方式：从真实场景要达成的效果和能力出发。**
1. 先判断这个场景属于**哪类需求范式**（本讲义 = 第 5 节三范式）；
2. 再说明在这个范式下，**为什么选这条技术链路、放弃了什么、承担了什么风险**；
3. 最后落到**如何运行、观测、验证**。

这样每个技术选项都不再是孤立知识，而是因为**它解决了链路上的某个真实卡点**而被记住。技术理解就从「**记住功能**」转向「**理解问题、约束、取舍与后果**」——这才是更高效、更**抗追问**的掌握方式。

> 所以「一点带面」的**「点」是核心场景范式**，**「面」是被范式串起来的特性、原理、多组件交互、取舍后果**。一旦认定属于哪个范式，`changelog-producer`、`scan.mode`、`merge-engine`、出口选型就基本被约束住了（这正是附录 E「场景→配方」成立的原因）。

**学习/落地的五段链路**（从「效果」出发，而非从「功能清单」出发）：

| 阶段 | 提问 | 本质 | 本讲义对应 |
|------|------|------|-----------|
| ① 真实需求（效果） | 要达成什么能力 | 定义目标 | 范式业务诉求（6.1 等） |
| ② 真实问题（能不能/怎么能） | 可行性与路径 | 框定问题 | 核心认知（6.2） |
| ③ 针对性研究（如何用/为什么） | 原理、约束、取舍 | 选链路、明放弃、知风险 | 机制与深水区（2 / 16） |
| ④ 真实业务环境（关联/互动/运行态） | 多组件如何协同 | 落到真实链路 | 状态后端协同 0.3、链路 9、join 14/17 |
| ⑤ 结果交付（持续/可观测） | 怎么运行、观测、验证 | 持续可信 | 运维规约（15） |

> ⚠️ 这条链路**必须闭环回流**：第 ⑤ 步运行态暴露的问题（延迟超标、状态爆、快照过期），要反过来修正第 ① 步的「效果定义」（如把「秒级」退让成「分钟级」）或第 ③ 步的取舍。**好的工程认知是这条链路的迭代闭环，不是一次性贯通。**

## 0.1 一句话：Paimon 改变了什么

Flink 是一个「能持续计算、但不长期保存数据」的流计算引擎——它的计算能力很强，但自身不持有业务数据，状态仅存在于作业内部、随作业的启动和停止而创建与销毁。Paimon 给了 Flink 一个**可被流式读写的、持久的、带主键和变更语义的表**。

> 两者结合的本质：把 Flink「动态表 = changelog」这个**理论**，从作业内部的临时状态，**物化成数据湖之上一张可持久、可流读、可批读、可当维表的真实表**。
>
> 于是：状态能外置、中间层能复用、流批能统一、重启能恢复——代价是数据可见性从消息级降到 checkpoint 级，且变更语义的正确性被**前移到表设计阶段**。

## 0.2 六条关键认知（每条都指向后文）

1. **流批一体的存储，配流批一体的计算**：Flink 早宣称流批一体，但存储不统一（实时 Kafka、离线 Hive）时只是计算层口号。Paimon 让同一张表既能流读（changelog）又能批读（snapshot），流批一体才落到存储。
2. **Paimon 物化了 Flink 的「动态表」**：Flink SQL 的理论根基是「动态表 = changelog 流」，但纯 Flink 里这个动态表是虚拟的、随作业结束而消失。Paimon 在磁盘上同时存了「当前快照」（动态表的物化）和「changelog 文件」（那个流）。`changelog-producer` 本质就是在配置「这张动态表要不要、如何持久化它的 changelog」。→ 第 16 节的根。
3. **状态可外置**：能写入 Paimon 表的数据就不必都存储在 Flink 状态后端里——维表用 lookup join 不进状态（第 17 节）、全量基线持久化在 Paimon 可重启恢复（第 16.4 节）、中间层写入 Paimon 可被多个下游复用。详见 0.3。
4. **一张表多种身份**：同一张 Paimon 主键表可同时是 sink、流 source、lookup 维表、temporal join 版本表（第 17 节）。Kafka（不能点查）、Hive（不能流式更新）都做不到。
5. **数据可见性的时间粒度变了**：Flink+Kafka 是消息级（毫秒），Flink+Paimon 是 **checkpoint 级近实时（分钟级）**（第 2 节）。Paimon 不是来替代 Kafka 做秒级的，是来替代 Hive/数仓做「能流式更新的湖」的。
6. **正确性前移到表设计**：用 Kafka 时变更语义是 format 的事；用 Paimon 后它变成建表决策（`changelog-producer`、`merge-engine`、`bucket`、主键），写进持久化 schema、不好改（第 16.6 节）。这是与写 Kafka 作业最大的思维转变。

## 0.3 Paimon 与 Flink 状态后端：正交的两层，不是替代也不是补充

常见误解：「有了 Paimon 是不是不需要状态后端了」/「Paimon 是不是一种状态后端」。**都不是。** 它们是流处理里两个不同层次、各自不可替代的东西。

| | Flink 状态后端（RocksDB 等） | Paimon |
|---|---|---|
| 存什么 | 算子的**运行时中间状态**（聚合的当前 SUM、join 两边待匹配行、窗口未触发数据、normalize 记的旧值） | **业务数据的持久表**（账户余额、订单明细、聚合结果） |
| 性质 | 过程态、作业私有、易失、引擎内部格式 | 结果态、共享、持久、有 schema 可 SQL 查、可跨引擎 |
| 生命周期 | 绑定作业（作业删除即没） | 独立于作业（写它的作业停了表还在） |
| 给谁用 | 引擎自己用，外部读不了 | 给人 / 给其它作业读 |
| 目的 | 让增量计算能进行下去 | 长期存储 + 流读批读 + 当维表 |

一句话区分：

> **状态后端存的是「算到一半的过程」（算子的草稿纸），Paimon 存的是「算出来的结果表 / 业务事实表」（归档的成品）。** 一个过程态、私有、易失；一个结果态、共享、持久。处在数据流的不同位置。

**为什么不是替代**：只要做流式聚合/join/窗口，Flink 照样需要状态后端。范式一的 `SUM(balance) GROUP BY subject_code`，每个科目的当前合计仍存在 RocksDB 里靠它增量维护；Paimon 存的是这个聚合的**输入表**和**输出表**，但「正在累加的中间值」还在状态后端。反过来，状态后端里的数据停了作业就没、外部查不了、不能当维表点查、不能批读对账——也替代不了 Paimon。

**为什么也不只是补充**（不是同一层互相辅助，而是正交两层，通过 checkpoint 衔接）：

```
        数据来了
           │
           ▼
   ┌──────────────────┐
   │  Flink 算子计算    │  ← 运行时中间状态存在【状态后端/RocksDB】(过程态、私有、易失)
   └────────┬─────────┘
            │ 每个 checkpoint 提交（Paimon commit 与 Flink checkpoint 绑定）
            ▼
   ┌──────────────────┐
   │  写入 Paimon 表    │  ← 业务结果/事实数据存在【Paimon】(结果态、共享、持久)
   └──────────────────┘
```

> 关键：状态后端做 checkpoint 的那一刻，正好是 Paimon 把这批数据持久化对外可见的那一刻（第 2 节）。两者通过 checkpoint **协同**，证明它们是配合的两层，而非谁替代谁。

**它们真正的关系——可以互相分担负载，但替代不了对方：**
- 把「全量维表」从状态后端转移到 Paimon：lookup join 代替 regular join，维表不再占状态（第 17 节）。
- 把「重启基线」从状态后端转移到 Paimon：状态丢了也能靠重读 Paimon 全量快照重建（第 16.4 节）。
- 但「正在聚合的中间值」无法外置：天然属于状态后端，Paimon 无法承担。

## 0.4 「湖」是什么：指存储形态，不是数据类型

本讲义例子（账户、订单）都是结构化数据，容易让人以为「湖只存结构化 JSON」。澄清两点：

**① 「数据湖」指底层存储形态，不是数据类型。** 它的本意是：用廉价、可扩展的对象/分布式存储（S3、OSS、HDFS），以**开放文件格式**集中存放海量数据。与「数据仓库」的对立点在于——

| | 数据仓库 | 数据湖 |
|---|---|---|
| 入库 | 先建模清洗、按固定 schema 入库 | 先原样存进廉价存储，schema 可后置 |
| 存储 | 专有封闭系统，贵 | 开放格式、廉价、容量近乎无限、存算分离 |

所以「湖」强调**介质廉价、格式开放、存算分离**，不是「只能存某种数据」。湖广义上能存日志、图片、视频、Parquet、JSON 任何东西。

**② Paimon 是「表格式（table format）」，专攻湖上的结构化/半结构化表。** 它不存图片视频，职责是**在湖的文件之上架起一层「表的抽象」**：

> 打个比方：**湖是一块巨大的廉价空地（文件系统），什么都能堆；Paimon 是在这块地上盖的「带户籍管理的标准仓库」——把杂乱的数据文件，组织成一张有主键、有 schema、能增删改查、能流读批读的表。**

Paimon 在底层文件（数据用 Parquet/ORC/Avro 列式格式 + manifest/snapshot 元数据）之上提供了：表 schema 与主键（第 16.6 节）、快照与事务（第 2 节）、LSM 主键 upsert（第 2.2 节）、changelog 文件（第 16 节）、流读/批读接口。

**正是这层「表抽象 + 事务 + 主键 + changelog」，让湖上原本只能批量追加的文件，变成能被 Flink 流式更新和流式读取的「表」。** 这就是**湖仓一体（Lakehouse）**：在数据湖的廉价开放存储上，获得接近数据仓库的表能力与事务能力。

---
---

# 第一部分 · 核心知识点

---

## 1. 心智模型：Paimon 流式分层是什么

Paimon 流式分层是 **checkpoint 驱动的分钟级近实时**：数据可见性与 commit/checkpoint 绑定，延迟随层数线性累加。它用「可查询、可回溯、低成本的写入数据湖」换取延迟，**不是 Kafka 那种秒级/消息级实时**。需要秒级的环节，中间层用 Kafka，而不是 Paimon。

一个贯穿全篇的关键结论先放这里：

> **流式做「全表状态计算」≠ 每次扫全表。** 它是 Flink 在状态里持续维护聚合结果，靠 Paimon 产出的**完整 changelog（带旧值回撤）**，让增量维护在数学上等价于「基于全表当前状态重算」。这一点是第 6 节范式一的核心，也是整份讲义最容易被误解的地方。

---

## 2. 核心机制：必须先理解的三件事

### 2.1 数据可见性 = commit = checkpoint

- Paimon 写入的数据要等生成新 **snapshot（commit）** 才对下游可见。
- commit 在 **Flink checkpoint 完成时**触发。
- 因此：checkpoint 间隔配 1 分钟 → 这一层数据最快 ~1 分钟才能被下一层读到。
- 语义是「攒一个 checkpoint 周期、原子提交一批」的**微批（micro-batch）**，不是来一条传一条。

> 推论：checkpoint 间隔是流式分层延迟的**基准单位**。

### 2.2 主键表底层是 LSM

- 主键表（Primary Key Table）底层是 LSM-tree，同主键多次写入会按 `merge-engine` 合并。
- 后台 compaction 会带来写放大和周期性资源占用，影响延迟的**稳定性**（抖动），而非平均值。

### 2.3 流读能拿到什么变更，取决于写入侧的 `changelog-producer`

- 「能不能拿到正确的 `UPDATE_BEFORE`（旧值）/ `DELETE`」是流式分层正确性的关键。
- 这个能力在**建表时**由 `changelog-producer` 决定，**事后改了也不影响历史数据**。
- 官方原话级别的解释（务必理解，第 6 节范式一直接依赖它）：
  > 下游做 `SUM(balance) GROUP BY ...` 时，某账户余额从 4 改成 5，下游必须知道**旧值是 4** 才能把汇总 +1。只看到新值 5，它无法判断该加多少。所以**流式聚合正确性依赖能拿到旧值**。

---

## 3. 两个独立维度（最容易混淆，务必分清）

很多人把这两个混成「Paimon 的读模式」，其实是互相独立的旋钮：

| 维度 | 由谁决定 | 作用 | 取值 |
|------|---------|------|------|
| **能读到什么 changelog** | 写入侧 `changelog-producer` | 决定流读拿到的变更完整性 | `none` / `input` / `lookup` / `full-compaction` |
| **流读从哪里开始** | 读取侧 `scan.mode` 等 | 决定起始位置 | `default` / `latest` / `latest-full` / `from-snapshot` / `compacted-full` 等 |

记住：`compacted-full` 是 **`scan.mode` 的取值（读取起点）**，不是一种 changelog 生产策略。

### 3.1 `changelog-producer` 四个取值

| 取值 | 该层延迟 | 流读能拿到的变更 | 适用 |
|------|---------|-----------------|------|
| `none`（默认） | 最低 | 跨快照合并后的结果，**拿不到可靠的 UPDATE_BEFORE**；下游若做聚合需 Flink「normalize」算子在状态里记旧值自行补回撤 | 下游只读最新值、不做流式聚合（如仅被维表 lookup join、或镜像到只认最新值的库） |
| `input` | 最低（≈1 个 checkpoint） | 直接透传输入流的 changelog，**要求输入本身是完整 CDC** | 上游是 OGG/Debezium 等完整 CDC |
| `lookup` | 略高于 input | 写入提交前通过 lookup 生成完整 `-U/+U/-D`，不依赖输入 | 上游不完整（append）但要低延迟精确流读 |
| `full-compaction` | 最高（数分钟级） | full compaction 时对比产出完整 changelog | 对延迟不敏感（如 30min）、想省额外开销 |

> ⚠️ **`none` + 下游聚合的代价**：能算对，但靠 Flink normalize 算子在状态里「记住每个 key 旧值」，**状态/内存开销极大，大表务必避免**。要下游聚合，优先让上游表用 `input` 或 `lookup` 产出完整 changelog。
>
> ⚠️ **`full-compaction` 延迟** = `full-compaction.delta-commits` × checkpoint 间隔（`delta-commits` 默认 1，即每个 checkpoint 都 full compaction 并产 changelog；调大则延迟随之放大）。官方建议：一般优先 `lookup`，`full-compaction` 开销更高。
>
> ⚠️ `lookup` / `full-compaction` 都支持 `changelog-producer.row-deduplicate=true`：值没变的记录不产 `-U/+U`，降低下游负载。
>
> ⚠️ `changelog-producer` 会显著增加 compaction 开销，**非必要不开**（即下游不需要完整 changelog 时，保持 `none`）。

### 3.2 `merge-engine` 对 changelog 内容的影响

| merge-engine | 行为 | 对下游的影响 |
|--------------|------|-------------|
| `deduplicate`（默认） | 保留同主键最新整行 | changelog 为完整行级变更 |
| `partial-update` | 同主键多次部分更新，按列合并 | 整行由多源分批拼出，下游会先读到「部分列为 null」的中间态行（详见下方注解） |
| `aggregation` | 按列做预聚合 | changelog 是**聚合后**结果，不是原始行级变更 |

> ⚠️ **流读硬约束（官方）**：`partial-update` 和 `aggregation` 这两种 merge-engine 做**流式查询**时，**必须配合 `lookup` 或 `full-compaction` 的 `changelog-producer`**（它们无法用 `input`/`none` 产出正确的流读 changelog）。这是 `merge-engine` 与 `changelog-producer` 的耦合约束，建表时就要一起定。

#### 3.2.1 `partial-update` 的「列稀疏」与下游抖动（重点澄清）

`partial-update` 的整行不是一次写完，而是**由多个来源分多次、按列拼出来**的。这带来两个容易误解的点：

**① 输入端的 null 与 changelog 输出端的 null，含义完全不同**

| | 含义 |
|---|---|
| **输入端（写入侧）的 null** | 「这一列不要更新，保留已有值」——这是 `partial-update` 的输入约定 |
| **changelog 输出端的 null** | 「这一列至今没有被任何来源写过」——而**不是**「这次没更新的列」 |

关键：Flink changelog 流里**每条记录物理上都是合并后的完整行（所有列都在）**，不存在「只带变更字段、其余省略」的记录。一个列**只要之前被写过、有值，就会带着真实值出现在 changelog 里，不会因为「本次没更新它」而变成 null**。所谓「列稀疏」指的是**某些列的值是 null**，不是物理上缺字段。

举例（注意 t3，来源 A 只更新 amount，但 changelog 仍带着 name/level 的真实值）：

| 时刻 | 写入来源 | 输入（input 侧） | changelog 输出的 `+U`（合并后完整行） |
|------|---------|-----------------|-----------------------------------|
| t1 | A | `(1, 100, null, null)` | `+U (1, 100, null,    null)` |
| t2 | B | `(1, null, 'Alice', 5)` | `+U (1, 100, 'Alice', 5)` |
| t3 | A | `(1, 120, null, null)` | `+U (1, 120, 'Alice', 5)` |

> 一句话：**输入端用 null 表达「不更新」，输出端 changelog 是合并后的完整行，null 只代表「这列还没被填过」。**

**② 「中间态」与下游「抖动」**

- **中间态**：一个主键在被所有来源拼齐之前，以「部分列为 null」的形态存在。
- **抖动**：下游若在列没填齐时就参与计算，会**先输出一个临时/错误值，随后被回撤（`-U`）修正**，期间结果在跳变。

下游做 `SELECT customer_level, SUM(amount) GROUP BY customer_level`，当「amount 先到、维度 level 后到」时：

```
t1（A 到，level 还是 null）:  level=null 这组 +100  → 出现一行 (null, 100)   ← 临时的、错的
t2（B 到，level=5）:          -U 撤掉 (null,100)，+U 把 100 记到 level=5 组
                              level=null → 0
                              level=5    → 100                              ← 修正后的正确值
```

在 t1~t2 之间读到结果的下游，会看到一行**本不该存在的 `(level=null, sum=100)`**，稍后它又消失、值跳到 `level=5` 组——这就是抖动。结果**最终一致（正确），但中间短暂不准**。

> 因此 `partial-update` 的下游必须满足：① sink 能处理 `-U/+U` 回撤（`upsert-kafka` / Paimon 主键表），由它吸收中间修正；② 业务能容忍「最终一致、短时不准」。若下游是普通 append `kafka` 或直连实时告警，会把中间态当真值，引发误报。

---

## 4. 延迟模型：逐层累加

每经过一层 Paimon，要多等「一个 checkpoint 周期 + 该层 changelog 生成延迟 + compaction 抖动」。

```
端到端延迟 ≈ Σ ( 每层 checkpoint 间隔 + 该层 changelog 生成延迟 + compaction 抖动 )
```

直观例子（每层 checkpoint=1min，均用 `input` 透传）：

```
ODS  --~1min-->  DWD  --~1min-->  DWS  --~1min-->  ADS     端到端 ≈ 3min
```

层数越多、checkpoint 越大、有层用 `full-compaction`，累加越明显。

### 4.1 每层延迟的三个决定因素

1. **checkpoint 间隔（最主要）**：延迟基准。调小可降延迟，但小文件变多、compaction 压力增大、协调开销上升。常见取 30s ~ 2min。
2. **changelog-producer 选型**：见 3.1 表。`input` ≈ `lookup` < `full-compaction`。
3. **compaction**：影响延迟**稳定性**（抖动），触发时写入吞吐短时下降。

### 4.2 不叠加延迟的情况：维表 lookup join

- 维表 lookup join 读的是**最新快照**，不依赖被关联表的 changelog，因此**不叠加「等 changelog 产出」的延迟**。
- 真正叠加延迟的是**级联的 changelog 流式消费 / 流式聚合**。

---

## 5. 三种流式消费范式（全篇核心分类）

> 所有讨论都在 **Flink 流式（`execution.runtime-mode=streaming`）常驻作业**前提下。
> 下游怎么消费 Paimon，本质就这三种。先认清自己属于哪一种，再选 `changelog-producer` 和 `scan.mode`。

| | 范式一：全表状态流式聚合 | 范式二：维表 lookup join | 范式三：追加快照持续消费 |
|---|---|---|---|
| **关心什么** | 某一时刻**整张表的完整状态**导出的聚合 | 用另一张表的**当前最新值**补维度 | 持续到来的**新增记录**（增量明细） |
| **典型业务** | 科目余额、汇总指标、统计口径重算 | 订单补客户/账户属性 | 流水分发、明细落库、事件转发 |
| **上游表 changelog 要求** | **必须完整**（`input`/`lookup`），否则算错 | 无要求（读最新快照，不看 changelog） | 主键表 changelog / 或 Append 表纯新增 |
| **scan.mode** | 默认（**初始全量快照 + 增量**） | 维表侧读最新快照 | `latest`（只读新增） |
| **写出** | `INSERT INTO` 持续 upsert 结果表 | join 后写下游 | `INSERT INTO` 持续追加 |
| **是否叠加分层延迟** | 是（等 changelog 产出） | 否 | 是（等新 snapshot） |
| **详见** | 第 6 节 | 第 7 节 | 第 8 节 |

---

## 6. 范式一：全表状态流式聚合（重点）

### 6.1 业务诉求

> 上游 CDC 持续把客户、账户的增删改 upsert 进 Paimon 主键表，形成「当前状态表」。下游要**基于全量账户的当前状态**算科目分类余额、汇总指标，并持续输出最新结果。它不关心单条 CDC 事件，关心的是**整张表此刻表达的完整业务状态**。

### 6.2 核心认知：流式增量聚合 ≡ 全表重算（前提是 changelog 完整）

- 这**不需要**每次扫全表，也**不该**用批处理。
- 做法是：常驻流作业**先读一遍初始全量快照**建立聚合基线，再**持续消费后续每一批变更**，靠 changelog 的旧值回撤（`-U`）+ 新值（`+U`）增量维护聚合结果。
- 只要上游表能产出**完整 changelog**，这个增量结果在任意时刻都等于「基于全表当前状态重算」的结果。
- 回到第 2.3 / 3.1 节的关键点：上游状态表的 `changelog-producer` **不能是 `none`**（否则要么算错，要么依赖昂贵的 normalize 补偿）。

### 6.3 第一步：状态表建表时保证 changelog 完整

```sql
CREATE TABLE dwd_account (
    account_id   BIGINT,
    subject_code STRING,
    balance      DECIMAL(20,2),
    status       STRING,
    PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'lookup',                 -- 关键：产出完整 changelog，下游聚合才算对
    'changelog-producer.row-deduplicate' = 'true',   -- 值未变的记录不产 -U/+U，降低下游负载
    'merge-engine' = 'deduplicate'
);
```

> 选型：上游 CDC 本身完整 → 可用 `input`（开销最小，透传）；想不依赖上游完整性、又要低延迟 → 用 `lookup`。两者下游聚合都正确。

### 6.4 第二步：常驻流作业做全表聚合

```sql
SET 'execution.runtime-mode' = 'streaming';

INSERT INTO ads_subject_balance
SELECT
    subject_code,
    SUM(balance) AS total_balance,
    COUNT(*)     AS account_cnt
FROM dwd_account
/*+ OPTIONS('consumer-id' = 'subject_balance_agg') */
GROUP BY subject_code;
```

- 默认 scan 行为 = **初始全量快照（全表基线）+ 持续增量**，正好对应「全表状态」。
- **不要**加 `scan.mode=latest`：那会跳过初始全量，余额基线就不完整了。只有「结果可以从空开始累计」的场景才用 `latest`。

### 6.5 第三步：结果表是带更新的主键表

```sql
CREATE TABLE ads_subject_balance (
    subject_code  STRING,
    total_balance DECIMAL(20,2),
    account_cnt   BIGINT,
    PRIMARY KEY (subject_code) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'lookup'   -- 若结果还要被再下游流式消费，则同样需要完整 changelog
);
```

聚合结果随上游变更持续 upsert，下游（看板 / 再上层汇总 / `upsert-kafka`）读到的永远是反映全量当前状态的最新值。

### 6.6 范式一关键坑

1. **`none` 会算错或代价高昂** —— 见 6.2 / 3.1。这是范式一最主要的错误来源。
2. **不要 `scan.mode=latest`** —— 会丢初始全量基线。
3. **GROUP BY 的键不必等于主键** —— 上例按 `subject_code` 聚合，但回撤正确性依赖的是 `account_id`（主键）维度的完整 changelog。
4. **聚合状态会膨胀** —— 分组基数大时关注 Flink 状态 TTL 与状态后端。

---

## 7. 范式二：维表 lookup join（详见第 14 节专题）

用另一张表的**当前最新值**给主流补维度。读最新快照、不依赖 changelog、不叠加分层延迟。
- 维表是 Paimon 主键表或 HBase 表均可，写法一致（`FOR SYSTEM_TIME AS OF proctime`）。
- 关键前提：**主流（probe 侧）必须带 `proctime`**；Paimon 物理表需先用视图补 `PROCTIME()`（这是高频坑，第 14 节详述）。

---

## 8. 范式三：追加快照持续消费

### 8.1 两种子情况

**A. 主键表，只想要「新增」语义**：主键表流读输出的是 changelog（`+I/-U/+U/-D`）。若下游只要新进来的记录、不想处理回撤：

```sql
SET 'execution.runtime-mode' = 'streaming';
SELECT * FROM dwd_customer
/*+ OPTIONS('scan.mode' = 'latest', 'consumer-id' = 'append_reader') */;
```

**B. 本质 append-only（只增不改）→ 应建 Append Table（无主键），而非主键表**：

```sql
CREATE TABLE log_events (
    event_id BIGINT,
    user_id  BIGINT,
    ts       TIMESTAMP(3)
    -- 无 PRIMARY KEY → Append Table
) WITH (
    'bucket' = '4'
);

SET 'execution.runtime-mode' = 'streaming';
SELECT * FROM log_events /*+ OPTIONS('scan.mode' = 'latest') */;
```

### 8.2 判断标准

> 数据有没有「按主键更新/删除」的语义？
> **有**（客户、账户基础数据）→ 主键表。
> **没有、只追加**（日志、流水、事件）→ Append Table，流读就是纯粹的「持续读新增快照」，无 changelog 合并开销，吞吐更高。

---
---

# 第二部分 · 完整业务场景案例：实时财务状态平台

> 用一条贯穿的业务主线，把第一部分的知识点串成端到端的流式分层链路。
> 所有作业都是 `execution.runtime-mode = streaming` 的**常驻流作业**。

## 9. 业务背景与链路全景

### 9.1 背景

某实时数据平台需求：

- 上游业务库的**客户、账户**基础数据持续增删改，经 CDC → Kafka。
- 另有**订单**数据（有状态流转：下单→支付→发货，会按 `order_id` 更新），也进 Kafka。
- 部分 topic 是 **OGG-CDC 格式**（完整 before/after），部分是**普通单层 JSON**（每条都是 `+I`，不带 before/after）。
- 下游需求：
  1. 基于**全量账户当前状态**实时算「科目分类余额、汇总指标」（范式一）。
  2. 订单流水实时补客户/账户维度后分发（范式二）。
  3. 原始流水明细持续落库供再加工（范式三）。

### 9.2 链路全景图

```
                     ┌─────────────────────────── ODS (Kafka) ───────────────────────────┐
                     │  topic: ogg_account   (OGG-CDC，完整变更)                          │
                     │  topic: ogg_customer  (OGG-CDC，完整变更)                          │
                     │  topic: json_order    (普通 JSON，每条 +I，无 before/after)        │
                     └────────────────────────────────┬───────────────────────────────────┘
                                                       │  Flink 常驻流作业（入湖）
                                                       ▼
   ┌──────────────────────────────── DWD (Paimon) ─────────────────────────────────┐
   │  dwd_account   主键表  changelog-producer=input    （范式一/二的状态&维表源）   │
   │  dwd_customer  主键表  changelog-producer=input    （范式二维表源）             │
   │  dwd_order     主键表  changelog-producer=lookup    （JSON 无 before，自产回撤）│
   └───────────────┬───────────────────────┬───────────────────────┬───────────────┘
                   │范式一                  │范式二                  │范式三
                   ▼                        ▼                        ▼
   ┌──────────── DWS/ADS ────────┐  ┌─── 维度补全流作业 ───┐  ┌─── 明细分发 ───┐
   │ 流读 dwd_account（全量+增量）│  │ 流读 dwd_order        │  │ 流读 dwd_order  │
   │ SUM(balance) GROUP BY 科目   │  │ +lookup join         │  │ scan.mode=latest│
   │ → ads_subject_balance        │  │   dwd_customer/HBase │  │ → 普通 kafka    │
   └──────────────┬───────────────┘  └──────────┬───────────┘  └────────────────┘
                  ▼                              ▼
            upsert-kafka                    upsert-kafka / Paimon
          (带更新的汇总结果)              (带维度的订单宽表)
```

> 注意三条下游支线**共享 DWD 层的表**，但因消费范式不同，对上游 `changelog-producer` 的要求不同——这正是第 5 节三范式分类的实战意义。

---

## 10. ODS → DWD：多源异构入湖

### 10.1 OGG-CDC topic（账户/客户）→ 主键表 `input`

OGG 带 op_type(I/U/D) 与 before/after，用 `ogg-json` format 解析出带正确 `RowKind` 的 changelog。输入已是完整 CDC，目标表用 `changelog-producer=input` 透传，开销最小。

```sql
-- ODS source
CREATE TABLE src_account_ogg (
    account_id   BIGINT,
    subject_code STRING,
    balance      DECIMAL(20,2),
    status       STRING,
    PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic'     = 'ogg_account',
    'properties.bootstrap.servers' = '...',
    'properties.group.id'          = 'kb_ogg_account',
    'scan.startup.mode'            = 'group-offsets',
    'format'    = 'ogg-json'            -- 解析出完整 changelog
);

-- DWD 状态表（范式一的输入、范式二的维表源）
CREATE TABLE dwd_account (
    account_id   BIGINT,
    subject_code STRING,
    balance      DECIMAL(20,2),
    status       STRING,
    PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'input',                  -- 上游 CDC 完整，透传
    'changelog-producer.row-deduplicate' = 'true',
    'merge-engine' = 'deduplicate'
);

-- 常驻入湖作业
INSERT INTO dwd_account SELECT * FROM src_account_ogg;
```

`dwd_customer` 同理（OGG → `input`），略。

### 10.2 普通 JSON topic（订单）→ 主键表 `lookup`

订单有状态流转（下单→支付→发货，按 `order_id` 更新），但 JSON format 解析出来**每条都是 `+I`，不带 before/after**。这类「有更新语义、但输入流不携带回撤」的数据，若直接用 `input`，Paimon 会把每条当 `+I` 透传，下游聚合会**重复累加**（同一 `order_id` 的多次状态更新被算成多行）。因此必须让 Paimon **自产回撤** → `lookup`。

```sql
CREATE TABLE src_order_json (
    order_id   BIGINT,
    account_id BIGINT,
    amount     DECIMAL(20,2),
    status     STRING,                   -- 状态会更新：PENDING/PAID/SHIPPED
    order_time TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic'     = 'json_order',
    'properties.bootstrap.servers' = '...',
    'properties.group.id'          = 'kb_json_order',
    'format'    = 'json'                  -- 每条 +I，无 before/after
);

CREATE TABLE dwd_order (
    order_id   BIGINT,
    account_id BIGINT,
    amount     DECIMAL(20,2),
    status     STRING,
    order_time TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'lookup',      -- 输入无 before，靠 Paimon 自产完整 changelog
    'merge-engine' = 'deduplicate'
);

INSERT INTO dwd_order SELECT * FROM src_order_json;
```

> 对比：若你的数据**确属只增不改的纯流水**（日志、事件，无任何按主键更新），则不该建主键表，应建 **Append Table**（无主键）省去 changelog 开销——判断标准见第 8.2 节。本案例订单有状态更新，故用主键表。

---

## 11. DWD → ADS 支线一（范式一）：全表账户状态 → 科目余额

直接复用第 6 节方案。这条支线消费 `dwd_account` 的**初始全量 + 增量**，增量维护科目余额。

```sql
SET 'execution.runtime-mode' = 'streaming';

CREATE TABLE ads_subject_balance (
    subject_code  STRING,
    total_balance DECIMAL(20,2),
    account_cnt   BIGINT,
    PRIMARY KEY (subject_code) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',         -- 带更新的汇总结果出 Kafka
    'topic' = 'ads_subject_balance',
    'properties.bootstrap.servers' = '...',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO ads_subject_balance
SELECT subject_code, SUM(balance) AS total_balance, COUNT(*) AS account_cnt
FROM dwd_account
/*+ OPTIONS('consumer-id' = 'subject_balance_agg') */
GROUP BY subject_code;
```

要点回顾：`dwd_account` 是 `input`（完整 changelog）→ 聚合算得对；默认 scan（全量+增量）→ 余额基线完整；结果带更新 → `upsert-kafka`。

---

## 12. DWD → 支线二（范式二）：订单补客户维度

订单流补客户/账户维度。事实表 `dwd_order` 是 Paimon 物理表，**必须先用视图补 `proctime`**（第 14 节专题的高频坑）。

```sql
SET 'execution.runtime-mode' = 'streaming';

-- 0) 维度补全结果表（带维度的订单宽表）
CREATE TABLE dws_order_enriched (
    order_id       BIGINT,
    account_id     BIGINT,
    amount         DECIMAL(20,2),
    customer_name  STRING,
    customer_level INT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'lookup'
);

-- 1) 给事实表补 proctime
CREATE TEMPORARY VIEW dwd_order_v AS
SELECT *, PROCTIME() AS proctime
FROM dwd_order
/*+ OPTIONS('consumer-id' = 'order_enrich') */;

-- 2) lookup join Paimon 维表 dwd_customer（也可换 HBase，见第 14 节专题）
INSERT INTO dws_order_enriched
SELECT
    o.order_id, o.account_id, o.amount,
    c.customer_name, c.customer_level
FROM dwd_order_v AS o
LEFT JOIN dwd_customer FOR SYSTEM_TIME AS OF o.proctime AS c
    ON o.account_id = c.account_id;
```

要点：维表读最新快照、不叠加延迟；维度后续变更**不回灌**已输出订单（范式二语义，见 14.7）。

---

## 13. DWD → 支线三（范式三）：订单明细持续分发

只要新增明细，向下游 Kafka 持续追加。用 `scan.mode=latest` 跳过历史全量。

```sql
SET 'execution.runtime-mode' = 'streaming';

CREATE TABLE sink_order_detail (
    order_id   BIGINT,
    account_id BIGINT,
    amount     DECIMAL(20,2),
    order_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',                -- 纯 append，普通 kafka 即可
    'topic' = 'dwd_order_detail',
    'properties.bootstrap.servers' = '...',
    'format' = 'json'
);

INSERT INTO sink_order_detail
SELECT order_id, account_id, amount, order_time
FROM dwd_order
/*+ OPTIONS('scan.mode' = 'latest', 'consumer-id' = 'order_detail_dispatch') */;
```

要点：明细直通无回撤 → 普通 `kafka`；`scan.mode=latest` → 只读新增。

---

## 14. 专题：流式消费 Paimon 关联 HBase 维表（lookup join 详解）

> 第 12 节用的是 Paimon 维表，这里展开 HBase 维表的完整实现（超大维度、高 QPS 点查场景更适合）。
> 依赖：`flink-connector-hbase-2.2`，版本 `4.0.0-1.19`，connector 名 `hbase-2.2`。

### 14.1 两个硬性前提

Flink lookup join（= **处理时间 temporal join**）要求：

1. **事实表（probe 侧，流读的 Paimon 表）必须带处理时间属性 `proctime`**。
2. **维表必须是 lookup source connector**（HBase connector 即是）。

> 关联的是维表「**当前最新值**」，按事实行到达的处理时间点查。HBase connector 是**同步 lookup 源**（可选异步点查），**不支持 event-time 版本化 join**。

### 14.2 第一步：给 Paimon 事实表补 proctime

⚠️ 坑点：Paimon catalog 里是物理表，**不能直接在表上加 `PROCTIME()` 计算列**。用临时视图补：

```sql
SET 'execution.runtime-mode' = 'streaming';

CREATE TEMPORARY VIEW dwd_order_v AS
SELECT *, PROCTIME() AS proctime
FROM dwd_order
/*+ OPTIONS('consumer-id' = 'join_job') */;
```

> 对比：若事实表是 **Kafka 表**，可直接在建表 DDL 写 `proctime AS PROCTIME()`，无需视图。只有 **Paimon 物理表**才需要视图包一层。

### 14.3 第二步：HBase 维表 DDL（Flink 1.19）

⚠️ **每个列族必须声明为 ROW 类型**：字段名映射列族名，嵌套字段名映射 qualifier。rowkey 是那个原子类型字段。

```sql
CREATE TABLE dim_customer_hbase (
    rowkey STRING,                          -- HBase rowkey
    info   ROW<name STRING, level INT>,     -- 列族 info，下挂 qualifier name / level
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector'        = 'hbase-2.2',
    'table-name'       = 'dim:customer',    -- namespace:table
    'zookeeper.quorum' = 'zk1:2181,zk2:2181',
    'lookup.async'                             = 'true',     -- 异步点查（仅 hbase-2.2），默认 false
    'lookup.cache'                             = 'PARTIAL',  -- 部分缓存，默认 NONE
    'lookup.partial-cache.max-rows'            = '100000',
    'lookup.partial-cache.expire-after-write'  = '10 min',
    'lookup.partial-cache.caching-missing-key' = 'true',     -- 缓存未命中 key，防穿透，默认 true
    'lookup.max-retries'                       = '3'          -- 默认 3
);
```

### 14.4 第三步：lookup join 查询

```sql
SELECT
    o.order_id, o.amount,
    d.info.name  AS customer_name,
    d.info.level AS customer_level
FROM dwd_order_v AS o
LEFT JOIN dim_customer_hbase FOR SYSTEM_TIME AS OF o.proctime AS d
    ON CAST(o.account_id AS STRING) = d.rowkey;   -- rowkey 是 STRING，注意 CAST
```

- 维度缺失要保留事实行 → `LEFT JOIN`；要过滤掉 → `INNER JOIN`。
- 关联键类型必须与 rowkey 一致。

### 14.5 性能调优三件套

| 手段 | 选项 | 作用 | 代价 |
|------|------|------|------|
| 缓存 | `lookup.cache=PARTIAL` + `max-rows` + `expire-after-write` | 热点维度缓存本地，减少 HBase 点查 | 维度更新有最长 = TTL 的延迟 |
| 防穿透 | `lookup.partial-cache.caching-missing-key=true` | 缓存未命中的 key | 维度新增的可见性也受 TTL 影响 |
| 异步 | `lookup.async=true` | 提高吞吐（默认同步） | 同 key 处理顺序默认不保证 |

### 14.6 延迟重试：应对「事实流早于维度到达」

事实数据先到、维度还没写进 HBase 时会 join 不上。用 `LOOKUP` hint 做 miss 重试：

```sql
SELECT /*+ LOOKUP('table'='dim_customer_hbase',
                  'retry-predicate'='lookup_miss',
                  'retry-strategy'='fixed_delay',
                  'fixed-delay'='1s',
                  'max-attempts'='3') */
    o.order_id, d.info.name
FROM dwd_order_v AS o
LEFT JOIN dim_customer_hbase FOR SYSTEM_TIME AS OF o.proctime AS d
    ON CAST(o.account_id AS STRING) = d.rowkey;
```

> 重试会阻塞该条记录处理，`fixed-delay` 和 `max-attempts` 别配太大，否则拖慢整体吞吐。

### 14.7 必须知道的语义坑

1. **只取最新值，不回溯历史**：proctime lookup join 取查询那一刻 HBase 的当前值。维度后来变了，**已输出的事实行不会被回灌/更新**。需要「维度变更触发下游重算」要换 event-time 版本化 join + 带 changelog 的维表（HBase lookup 做不到）。
2. **异步 + 缓存牺牲一致性**：缓存 TTL 内读到旧维度值；异步模式同 key 顺序默认不保证。
3. **不叠加分层延迟**：lookup join 读维表当前值，不依赖维表 changelog，因此不增加第 4 节的「等 changelog 产出」延迟。

### 14.8 Paimon 维表 vs HBase 维表 选型

| 对比 | Paimon 主键表作维表 | HBase 维表 |
|------|---------------------|-----------|
| 数据来源 | 已在 Paimon 湖内 | 外部 HBase 集群 |
| 点查性能 | 读最新快照 + 本地缓存 | 毫秒级随机点查，更适合超大维度高频点查 |
| 运维 | 无需额外组件 | 需维护 HBase 集群 |
| 适用 | 维度也在湖内、规模中等 | 维度超大、点查 QPS 高、已有 HBase |

两者查询写法一致（都用 `FOR SYSTEM_TIME AS OF`），区别只在维表 DDL 的 connector。

---

## 15. 运维与出口规约（贯穿全链路）

### 15.1 流读一律配 `consumer-id` 防快照过期

下游消费慢或重启慢时，起始快照可能已被清理 → 报「快照不存在」。配 `consumer-id` 让 Paimon 按消费进度安全保留快照：

```sql
-- ① consumer.expiration-time 是【表属性】，用 ALTER TABLE 设置（防止误配的 consumer 永久占用快照）
ALTER TABLE dwd_account SET ('consumer.expiration-time' = '1 d');

-- ② 读侧 hint 只给 consumer-id（可选 consumer.mode / consumer.ignore-progress）
SELECT * FROM dwd_account
/*+ OPTIONS('consumer-id' = 'agg_job') */;
```

> ⚠️ 易错点：`consumer.expiration-time` **不是读侧 hint 选项**，而是表属性，需 `ALTER TABLE ... SET` 设置，且**过期由该表的写入作业触发**（设完要重启写作业才生效）。读侧 hint 里只放 `consumer-id` 这类读取选项。
>
> 补充：`consumer.mode` 默认 `exactly-once`（消费与 checkpoint 严格对齐，支持精确断点续读）；可选 `at-least-once`（允许多 reader 不同速率、记录最慢位点，性能更好、支持 watermark 对齐）。两种模式 Flink 状态不兼容，**切换模式不能从原状态恢复**。
>
> 影响**正确性/稳定性**，不影响延迟，但分层链路里很容易踩。本案例每条流读支线都配了 `consumer-id`。

### 15.2 流读起点控制（`scan.mode`）

```sql
SET 'execution.runtime-mode' = 'streaming';   -- 开启流读靠这个，不是 'streaming'='true'

SELECT * FROM dwd_account;                                              -- 默认：全量快照 + 增量（范式一用）
SELECT * FROM dwd_order /*+ OPTIONS('scan.mode' = 'latest') */;         -- 只读增量（范式三用）
SELECT * FROM dwd_account /*+ OPTIONS('scan.snapshot-id' = '3') */;     -- 从指定 snapshot
SELECT * FROM dwd_account /*+ OPTIONS('scan.timestamp-millis' = '1700000000000') */;  -- 从时间戳（ms）
```

> ⚠️ 常见误区：Paimon **没有** `'streaming' = 'true'` 这个表选项。流读/批读由 `execution.runtime-mode` 决定。
> ⚠️ 用 `scan.snapshot-id` / `scan.timestamp-millis` 时**单独设即可**，scan.mode 会自动推断，不必再配 `from-snapshot` / `from-timestamp`。

### 15.3 出口：写 Kafka 还是写 Paimon

| 下游结果 | 出口选型 |
|---------|---------|
| 带更新/回撤（聚合、维表关联变化） | **`upsert-kafka`**（普通 `kafka` 遇 `-U/-D` 会报错/丢语义） |
| 纯 append（明细直通） | 普通 `kafka` |
| 写回 Paimon 主键表 | 正常 `INSERT INTO`，无限制 |

### 15.4 流读并行度

- 流读并行度默认 = bucket 数（受 `scan.infer-parallelism.max` 限制，默认 1024）。
- 可用 `scan.parallelism` 手动指定，或关 `scan.infer-parallelism` 用全局并行度。
- 合理设置 `bucket` 影响吞吐与并行度。

### 15.5 小文件治理

- checkpoint 间隔短 + bucket 多 → 每个 snapshot 产生大量小 changelog 文件。
- 开 `precommit-compact = true` 在 writer 后加 compact 算子合并小 changelog 文件。
- `changelog-producer` 会显著增加 compaction 开销，非必要不开。

---

## 16. 深水区专题：`none`、normalize、容错与表属性变更

> 这是第 2.3 / 3.1 节 `changelog-producer` 的底层展开，也是范式一正确性的根。
> 理解这一节，才算真正搞懂「为什么 `none` 下游聚合代价高」「重启还算不算得对」「线上能不能直接改表属性」。

### 16.1 `none` 不是「不产生变更」，而是「有新值、没旧值」

`changelog-producer=none` 常被误解成「Paimon 什么变更都不记」。实际不是：

- `none` 表仍是 LSM 主键表，每个 checkpoint 提交一个 snapshot，snapshot 之间**有差异**。Paimon 能告诉你「相比上个 snapshot，这些 key 现在是新值、这些 key 被删了」——官方称之为 **merged changes（合并后的变更）**。
- 它**缺的是旧值**：LSM 合并时旧值被新值覆盖，snapshot 里只留最新值。比如 key=100 的 balance 现在是 5，Paimon 知道「现在是 5」，但**不知道之前是 4 还是 6**。

所以流读 `none` 表，source 能吐「key=100 新值 5」（类 upsert / `+U`），吐不出「旧值 4」（`-U`）。

### 16.2 normalize 是什么，旧值的依据从哪来

**normalize 是 Flink SQL planner 自动插入的算子**（不是 Paimon 的，也不是你写的）。当一个流作业以 streaming 方式读 `none` 表、且下游需要完整变更语义（回撤）时，planner 自动在 source 后插入它。

它的依据是**自己 Flink 状态里缓存的、上一次见过的该 key 的值**：

```
Paimon source 读到:  key=100 → 新值 5        (merged change，只有新值)
        ▼
normalize 算子（依据 = 自己状态里 key=100 上次的值 4）:
   向下游发  -U(key=100, 4)   ← 旧值来自 normalize 状态，不是 Paimon 文件
            +U(key=100, 5)
   更新状态  key=100 → 5
```

- 这份状态要存**全表每个主键的最新整行**，表越大状态越大 → 内存/磁盘/checkpoint 成本越高。这就是「大表务必避免」的原因。
- 对比 `input`/`lookup`/`full-compaction`：旧值在**写入时**就写进了专门的 changelog 文件，source 直接读出 `-U`，**不需要 normalize**。本质是「补旧值」这件事在写入端做还是读取端做的区别。
- `scan.remove-normalize` 可强制去掉 normalize，但那是「确定下游不需要回撤」时的优化；去掉后聚合照样会错，不能用它来「省状态又指望算对」。

### 16.3 RowKind 基础

Flink 每条记录带一个 `RowKind`：`+I`（新增）、`-U`（更新前旧值）、`+U`（更新后新值）、`-D`（删除）。
聚合（`SUM`/`COUNT`）、去重、部分 join 需要 `-U`/`-D` 来「抵消」之前的累加。`none` 表流读缺这些，才需要 normalize 补。

### 16.4 异常重启后还能正确基于全表状态计算吗

能，**前提是开 checkpoint 且从 checkpoint/savepoint 恢复**。分三种情形：

| 情形 | 结果 | 说明 |
|------|------|------|
| 开 checkpoint，从 checkpoint 恢复（标准做法） | **正确** | normalize 状态、聚合状态、Paimon source 消费位点都在 checkpoint 里，一起回滚到同一点继续；exactly-once 保证不重不丢 |
| 状态彻底丢失，但用**默认 scan（全量+增量）** | **最终正确，但慢** | 重读全量快照重建 normalize 旧值缓存和聚合 → 重扫全量代价高 |
| 状态丢失 + `scan.mode=latest` | **会算错** | 既无历史状态，又跳过全量基线，旧值无从谈起 |

> 结论：范式一这类作业**必须开 checkpoint**；范式一**别用 `scan.mode=latest`**（见 6.4）；并注意 `none` 表 snapshot 过期问题——重启重扫全量时若历史 snapshot 已清理会读不全 → 配 `consumer-id` 防过期（15.1）。

### 16.5 读侧选项的三种覆盖方式

不止 SQL hint 一种：

**① SQL Hints（单条查询临时覆盖）**
```sql
SELECT * FROM t /*+ OPTIONS('scan.mode' = 'latest') */;
SELECT * FROM t /*+ OPTIONS('scan.snapshot-id' = '1') */;
```
需 Flink `table.dynamic-table-options.enabled`（1.19 默认开）。

**② 动态配置选项（作业级，不改 SQL、不动元数据）**
通过 Flink 配置按「catalog.database.table」精确指定，格式：
```
table.dynamic-option.${catalog}.${database}.${tableName}.${key} = ${value}
```
例：
```
table.dynamic-option.my_catalog.my_db.my_table.scan.snapshot-id = 1
```
catalog/database/table 段支持 `*` 通配。**优先级：动态选项 > 表选项 > 全局选项**（动态选项覆盖原始表选项，表选项覆盖全局）。适合「不想改 SQL、又要按作业注入不同读取参数」的场景（如同一份 SQL 提交多个起点不同的作业）。

**③ 临时视图（`CREATE TEMPORARY VIEW`）**
```sql
CREATE TEMPORARY VIEW dwd_account_latest AS
SELECT * FROM dwd_account /*+ OPTIONS('scan.mode'='latest') */;
```
不污染持久化元数据。临时视图在 **Flink SQL 会话关闭时自动删除**，把「带特定读取选项的查询」封装成可重用、有业务语义的逻辑表（如 `dwd_account_latest`），多处引用时不必每次重写 hint。

> ⚠️ 注意：Paimon **不支持** `CREATE TEMPORARY TABLE ... WITH (...) LIKE ...` 这种组合。
> - `CREATE TABLE ... LIKE`（含 `EXCLUDING OPTIONS`）只用于复制结构、创建**持久化表**，不能用于临时表。
> - `CREATE TEMPORARY TABLE` 必须自带 `connector`（如 `filesystem`/`values`），无法用 `LIKE` 复制 Paimon 表结构。
> - 因此对 Paimon 表做读侧临时覆盖，正确做法是**用 `CREATE TEMPORARY VIEW` 包一条带 hint 的查询**，或直接用方案 ①/②。

**三种方式怎么选：**

| 方案 | 优点 | 缺点 | 适用 |
|------|------|------|------|
| ① SQL Hints | 简单直接，无需额外 DDL | 每次查询都要重写，不可重用 | 单次/临时查询 |
| ② 动态配置选项 | 不改 SQL，作业/会话级生效 | 影响范围大，可能波及该匹配下的其他查询 | 同一份 SQL 提交多个起点不同的作业；运维侧统一注入 |
| ③ 临时视图 | 可重用、语义清晰、能封装复杂逻辑 | 需额外 DDL | 多个查询复用同一套读取选项 |

> 这三种都**不改表的持久化 schema**。要持久化地改属性才用 `ALTER TABLE`（见下）。

### 16.6 `ALTER TABLE` 对运行中任务的影响

`ALTER TABLE ... SET (...)` 会生成新的 schema 版本并**持久化**。底层走 `SchemaChange.setOption`，提交前会经 `SchemaManager.checkAlterTableOption` 校验——**不可变选项的修改会被直接拒绝（抛异常）**。

**对正在运行的消费（读）任务：不自动生效。**
- 任务启动时通过 `buildPaimonTable` 一次性读取表属性 + 动态选项并固定下来，运行中**不会自动重新加载**。
- 有 `Catalog.invalidateTable` 可使缓存失效，但需**手动调用**，不是自动刷新。
- 要用上新属性，**必须重启任务**。

**对正在运行的写入任务：取决于属性类型。**

| 属性类型 | 影响 |
|---------|------|
| 不可变选项 | `ALTER` 时直接被 `checkAlterTableOption` 拒绝 |
| 可变选项（如 `snapshot.num-retained-max`） | 新写入用新值；进行中的写入不受影响；部分选项需重启才生效 |
| `bucket` 数 | **特殊流程**：需 savepoint 停作业 → `ALTER TABLE SET ('bucket'='N')` → 从 savepoint 恢复（rescale-bucket），不能简单热改 |
| `changelog-producer` / `merge-engine` 等行为属性 | 只对之后写入生效；新旧 snapshot 语义不一致，下游可能读到「前段缺回撤、后段有回撤」的混合数据 → 建议停作业、改、用新 schema 重启 |

**实践建议（再强调）：**
> 涉及 `changelog-producer`、`merge-engine`、`bucket` 这类影响**数据物理形态 / 变更语义**的属性，不要对生产中正在跑的任务热改。正确姿势：① savepoint 停作业 → ② `ALTER`（或重建表 + 回刷）→ ③ 新配置重启。
> 只有 `scan.*` 这类纯读时选项，才适合用 SQL hint / 动态配置选项临时覆盖、不动元数据。

---

## 17. 流式 Paimon 表 join 行为与优化全景

> 第 7 / 14 节只讲了 lookup join（范式二）。这一节把 Flink 流式下三种 join 放在一起，并对比「Paimon 表 join」与「Kafka 多流 join」的语义差异。

### 17.1 根本认知：join 语义由 Flink 决定，Paimon 只决定「喂数据的形态」

Paimon 表参与 join 时，**join 的算法语义完全由 Flink SQL 的 join 类型决定**（Regular / Lookup / Temporal），Paimon 和 Kafka 用的是同一套 join 算子。Paimon 只负责「以什么形态、什么生命周期把数据喂给 Flink」——是「初始全量快照 + changelog 流」，还是「被点查的最新快照」。

> 推论：同一张 Paimon 主键表，用不同 join 写法，行为天差地别。先认清用的是哪种 join。

### 17.2 三种 join 的行为特性

**① Regular Join（双流 join）——状态会膨胀**
```sql
SET 'execution.runtime-mode' = 'streaming';
SELECT o.order_id, o.amount, c.customer_name
FROM dwd_order AS o
JOIN dwd_customer AS c ON o.account_id = c.account_id;
```
- 两张 Paimon 表都作为**普通流 source**（各自吐「初始全量 + changelog」），Flink 做双流 join。
- **双边都进状态**，任一边来新记录都去对侧状态找匹配。
- **状态无限增长**：默认无 TTL，长跑流作业的头号杀手。需配 `table.exec.state.ttl`，但 TTL 过短会导致老数据被清后漏关联（影响正确性）。
- **维度变更会回灌历史结果**：两边的 `-U/+U/-D` 都触发对侧重算并向下游发回撤。
- 输出是 retract 流 → sink 要能处理（`upsert-kafka` / Paimon 主键表）。
- ⚠️ **Paimon 特有风险**：启动时先灌初始全量快照进状态，比 Kafka 双流 join **更容易状态瞬间爆**、启动更慢。

**② Lookup Join（维表 join）——状态小、不回灌**（详见第 14 节）
```sql
SELECT o.order_id, c.customer_name
FROM dwd_order_v AS o                  -- 带 proctime 的视图
JOIN dwd_customer FOR SYSTEM_TIME AS OF o.proctime AS c
ON o.account_id = c.account_id;
```
- 维表被**点查最新快照**，不进 Flink 状态；状态小、可控，适合长跑。
- **不回灌**：取查询那一刻的维度值，维度后续变更不影响已输出行。
- 主流需 `proctime`（Paimon 物理表先建视图补，高频坑）。
- Paimon 官方明确支持**主键表和 append 表**作 lookup 维表；可用 `lookup.cache`、分区表 `max_pt()`、`sys.query_service` 加速。

**③ Temporal Join（事件时间版本 join）——按业务时点取值**
```sql
SELECT o.order_id, o.amount, r.rate
FROM dwd_order AS o
JOIN currency_rates FOR SYSTEM_TIME AS OF o.order_time AS r  -- event-time 字段
ON o.currency = r.currency;
```
- 按**事件时间**关联维表「那个时点的版本」，不是当前最新值。
- 需要维表是 **versioned table**：有主键 + event-time 属性 + watermark（Flink 要求，缺一报 `requires both primary key and row time attribute`）。
- **Paimon 主键表可直接作版本表**：流读时定义 watermark 和事件时间字段即可——这是相对 HBase 维表（只能 proctime lookup）的优势。
- 靠 watermark 推进触发，watermark 不推进会延迟输出。

### 17.3 三种 join 对比表

| 维度 | Regular Join | Lookup Join | Temporal（event-time） |
|------|-------------|-------------|----------------------|
| Paimon 表角色 | 双边都是流 source | 维表被点查最新快照 | 维表是版本表（流 source + watermark） |
| 谁进 Flink 状态 | 双边全量 | 维表不进 | 维表按版本进 |
| 状态成本 | **高，易爆** | 低 | 中 |
| 维度变更回灌历史结果 | **会** | 不会 | 按事件时点取版本 |
| 取到的维度值 | 最新（带回撤） | 查询时刻最新 | 事件时间点的版本 |
| 需要 proctime/watermark | 否 | 主流需 proctime | 需 event-time + watermark |
| 典型场景 | 双流对账 | 补维度属性 | 汇率/价格按时点取值 |

### 17.4 Paimon 表 join vs Kafka 多流 join 的语义区别

join 算法两者共用 Flink 同一套算子，**区别在「数据以什么形态、什么生命周期喂进来」**——即 source 特性差异。四点：

**① 有界性 / 初始快照行为（最根本）**
- Kafka source：无界，从某 offset 起，**无「全量基线」概念**；流 join 启动时状态从空开始，只能 join 到启动后两边都出现过的数据。
- Paimon source：默认**先读完整初始快照（全表基线）再接增量**；用在 regular join 里，启动即把整张表灌进状态——有完整历史基线，但大表启动慢、状态瞬间大。
> 这就是为什么 SQL 一样，Paimon×Paimon 双流 join 比 Kafka×Kafka 更易状态爆。

**② 变更语义来源**
- Kafka：是否 changelog 取决于 **format**（`kafka`+json = append 全 `+I`；`upsert-kafka` / `debezium`/`ogg-json` = 带回撤）。
- Paimon：是否完整 changelog 取决于建表的 **`changelog-producer`**（第 16 节）。`none` 主键表作 join 输入时 Flink 要插 normalize 补回撤；**Kafka append 流不触发 normalize，Paimon `none` 主键表流读会**。
> Kafka 变更语义写在「数据格式」里，Paimon 写在「表属性」里、且可能要 normalize 补。

**③ 可重放 / 容错**
- Kafka：受 topic retention，offset 过期回不去。
- Paimon：基于 snapshot + `consumer-id` 断点续读（15.1），但快照同样会过期，靠 `consumer-id` + `consumer.expiration-time` 防护。机制不同（retention vs snapshot 过期），关注点一致（消费跟不上→数据被清）。

**④ 能否作 lookup 维表**
- Paimon 主键表 / append 表**能直接**作 lookup 维表（读最新快照、可缓存、可 QueryService 加速）。
- Kafka **不能直接**作 lookup 维表（无「按 key 点查最新值」能力），需先 `upsert-kafka` 物化成动态表参与 regular join，或落 Paimon/HBase 再 lookup。

| 维度 | Kafka 流 join | Paimon 表 join |
|------|--------------|----------------|
| 启动基线 | 无，从 offset 起，状态从空 | **先灌初始全量快照**，再接增量 |
| 变更语义来源 | format（append/upsert/debezium） | 表属性 `changelog-producer`（可能要 normalize） |
| 能否作 lookup 维表 | 不能直接 | **能**（主键表/append 表） |
| 重放/容错 | 受 topic retention | snapshot + `consumer-id` 断点续读 |
| regular join 状态风险 | 持续增长 | 增长，**且启动即载入全量更易爆** |

> 最易踩：把 Paimon 表当 Kafka 流那样写 regular join，会因「初始全量快照灌入状态」比 Kafka 更易状态膨胀。**维度补全优先 lookup join（范式二），不要用 regular join。**

### 17.5 Lookup join 进阶与 join 性能优化（Paimon 特有能力）

第 14 节讲了基础 lookup join，这里补 Paimon 在 join 上提供的进阶能力与优化手段。

**① 异步 + 缓存模式**

`lookup.cache` 三种模式（默认 `AUTO`）：

| 模式 | 行为 | 适用 |
|------|------|------|
| `FULL` | 整表/整分区加载进本地缓存 | 维表小、能整体放下 |
| `PARTIAL` | 只缓存最近访问的 key | 维表大、热点集中 |
| `AUTO` | 自动选择（默认） | 不确定时 |

配合 `lookup.async=true`（默认 false）+ `lookup.async-thread-number`（默认 16）开异步点查提升吞吐。

**② 动态分区刷新：维表只关联「最新分区」**

传统数仓常把每个分区维护成「该时点的全量」，只需 join 最新分区。Paimon 用 `scan.partitions` 支持（`lookup.dynamic-partition` 是其 fallback 名）：

```sql
SELECT o.order_id, o.total, c.country
FROM orders AS o
JOIN customers /*+ OPTIONS('scan.partitions'='max_pt()',
                           'lookup.dynamic-partition.refresh-interval'='1 h') */
FOR SYSTEM_TIME AS OF o.proc_time AS c
ON o.customer_id = c.id;
```
- `max_pt()`：自动取分区值最大的分区（另有 `max_two_pt()`）；也可写 `dt=20230101` 指定固定分区。
- 对 lookup source，最大分区会**按 `refresh-interval` 周期刷新**（默认 1h）；对普通 source 则在作业启动前确定、运行中不刷新。

**③ Query Service：为维表起独立查询服务加速 lookup**

```sql
CALL sys.query_service('database_name.table_name', parallelism);
```
- 启动一个**独立的 Flink 流作业**作为该表的远程查询服务；存在时 lookup join 优先从它取数，而非读本地文件，显著提升点查性能。
- ⚠️ **执行约束**（源码级）：① **仅支持 streaming 模式**；② 表必须是 **`HASH_FIXED`（固定 bucket）模式且有主键**，否则抛 `UnsupportedOperationException`。
- 代价：是常驻作业，需额外资源。

**④ 异步重试的顺序坑（CDC 流）**

`allow_unordered`（异步重试用，避免一条 miss 阻塞后续）在主表是 **CDC 流时会被 Flink SQL 忽略**（它只支持 append 流），作业可能仍被阻塞。绕法：用 Paimon 的 `audit_log` 系统表把 CDC 流转成 append 流。

**⑤ Bucketed Join（避免 shuffle）——主要用于批，流式有限**

两张表用**相同 bucketing 策略 + 相同 bucket 数 + 相同 `bucket-key`** 时，join 可避免 shuffle：

```sql
CREATE TABLE FACT (order_id INT, f1 STRING) WITH ('bucket'='10', 'bucket-key'='order_id');
CREATE TABLE DIM  (order_id INT, f2 STRING) WITH ('bucket'='10', 'primary-key'='order_id');
SELECT * FROM FACT JOIN DIM ON FACT.order_id = DIM.order_id;
```
> ⚠️ Bucketed join 主要在**批处理**下避免 shuffle 见效；**流式模式下效果有限**。

**⑥ 动态分区过滤（Dynamic Partition Pruning）**

Paimon `DataTableSource` 实现 `SupportsDynamicFiltering`：星型 schema join 中，维表的分区过滤条件可在**运行时**下推给事实表，减少事实表扫描量。无需手动配置，由优化器在合适场景启用。

### 17.6 多 source watermark 对齐

Paimon 支持 Flink 的 **watermark alignment**，防止某些 source/split/partition 的 watermark 超前太多（对 event-time temporal join、多流 join 的语义一致性有用）：

| 选项 | 默认 | 说明 |
|------|------|------|
| `scan.watermark.alignment.group` | (none) | 需对齐 watermark 的 source 组名 |
| `scan.watermark.alignment.max-drift` | (none) | 最大漂移量，超过则暂停消费该 source 直到其它追上 |

对 append 表流式 join 的顺序保证：默认不保证顺序（类似 Flink-Kafka），有顺序要求需定义 `bucket-key`；同分区同 bucket 内严格按写入顺序，不同 bucket 之间无序，跨分区顺序由 `scan.plan-sort-partition` 决定。

---
---

# 附录

## A. 三范式速查

| 你的诉求 | 范式 | 上游 changelog-producer | scan.mode | 出口 |
|---------|------|------------------------|-----------|------|
| 全表状态算聚合（余额/汇总） | 一 | `input` / `lookup`（**不可 none**） | 默认（全量+增量） | `upsert-kafka` / Paimon |
| 补维度（最新值） | 二 | 无要求 | 维表读最新快照 | 看结果是否带更新 |
| 持续读新增明细 | 三 | 主键表 changelog / Append 纯新增 | `latest` | 普通 `kafka` / Paimon |

## B. 全局速记表

| 问题 | 结论 |
|------|------|
| 实时性级别 | 分钟级近实时（checkpoint 驱动） |
| 延迟基准单位 | 每层 checkpoint 间隔 |
| 延迟如何累加 | 随层数线性累加（lookup join 不叠加） |
| 流式全表状态计算的本质 | 完整 changelog 驱动的增量聚合 ≡ 全表重算，**非扫全表** |
| 全表聚合为何不能用 `none` | 拿不到旧值，要么算错，要么靠昂贵 normalize |
| OGG topic 入湖 | `ogg-json` + `changelog-producer=input` |
| 普通 JSON 入湖（有更新语义） | `json` + `changelog-producer=lookup` |
| 纯新增流水 | 建 Append Table（无主键），`scan.mode=latest` 流读 |
| Paimon 物理表做 lookup join | 先建视图补 `PROCTIME()`，再 join 视图 |
| 维表 lookup join 语义 | 只取查询时刻最新值，**不回灌**已输出行 |
| 流式 join 选型 | 补维度→lookup join；按时点取值→temporal join；双流对账→regular join（注意状态） |
| Paimon vs Kafka 做 join | Paimon 启动先灌全量快照（regular join 更易状态爆）；Paimon 能作 lookup 维表，Kafka 不能直接 |
| 开启流读 | `SET 'execution.runtime-mode'='streaming'`（无 `'streaming'='true'`） |
| 防快照过期 | 读侧 hint 配 `consumer-id`；`consumer.expiration-time` 用 `ALTER TABLE` 设在表上 |
| 带回撤出 Kafka | `upsert-kafka` |

## C. 设计 Checklist

1. **先归类范式**：每条下游支线先判断属于范式一/二/三，再定上游表的 `changelog-producer` 和 `scan.mode`。
2. **按「表怎么被消费」定 changelog-producer**：
   - 只被维表 lookup join → 可 `none`（省资源）。
   - 被流式聚合 / 级联 changelog 消费 → 必须 `input` 或 `lookup`。
3. **先做延迟加法**：定端到端延迟预算（如 5min），反推每层 checkpoint 间隔与层数。
4. **能合并的层就合并**：减少落湖次数和 checkpoint 周期累加。
5. **中间层慎用 `full-compaction`**：低延迟链路优先 `input`/`lookup`。
6. **流读作业一律配 `consumer-id`**：防快照过期。
7. **范式一别加 `scan.mode=latest`**：会丢初始全量基线。
8. **Paimon 物理表做 lookup join 先补 proctime 视图**。
9. **带回撤结果出 Kafka 用 `upsert-kafka`**，纯 append 用普通 `kafka`。
10. **秒级需求用 Kafka 兜底**：Paimon 负责可查询、可回溯、低成本的分层存储。
11. **范式一作业必须开 checkpoint**：normalize/聚合状态靠 checkpoint 容错，恢复才正确（第 16.4 节）。
12. **临时改读取参数优先用 SQL hint 或动态配置选项**（`table.dynamic-option.*`），不要用 `ALTER TABLE`（第 16.5 节）。
13. **改 `changelog-producer`/`merge-engine`/`bucket` 走「停—改—重启」**：不要对运行中任务热改（第 16.6 节）。
14. **流式 join 先选对类型**：补维度用 lookup join（状态小、不回灌）；双流对账才用 regular join 并配 `table.exec.state.ttl`；按业务时点取值用 event-time temporal join（第 17 节）。
15. **别把 Paimon 表当 Kafka 流写 regular join**：Paimon 启动先灌全量快照，比 Kafka 更易状态爆（第 17.4 节）。
16. **lookup join 性能优化按需上**：缓存（`lookup.cache`）→ 异步（`lookup.async`）→ 最新分区（`max_pt()`）→ Query Service（`sys.query_service`，仅 streaming + 固定 bucket + 主键）。CDC 主流注意 `allow_unordered` 失效（第 17.5 节）。

---

> 版本备注：以上语法基于 Flink 1.19 + Paimon 1.1.1。升级 Paimon 版本时，请到官方 download 页核对对应的 `paimon-flink-<flink_version>-<paimon_version>.jar` 与选项默认值，部分默认值可能随版本调整。

---

## D. 配置总表（对象 / 作用面 / 影响面 一处对齐）

> 全篇配置散在各节，这里集中查阅。「对象」=在哪设、谁的属性；「作用面」=直接控制什么；「影响面」=连带影响什么。

### D.1 写入侧 / 建表属性（持久化进 schema，改动需「停—改—重启」）

| 配置 | 对象 | 作用面 | 影响面 | 默认 | 详见 |
|------|------|--------|--------|------|------|
| `changelog-producer` | 建表 WITH | 决定写入时产不产、如何产完整 changelog 文件 | 下游流读能否拿到回撤→聚合正确性；compaction 开销；延迟 | `none` | 3.1 / 16 |
| `changelog-producer.row-deduplicate` | 建表 WITH | 值未变的行不产 `-U/+U` | 减下游负载 | false | 3.1 |
| `merge-engine` | 建表 WITH | 同主键多行如何合并 | changelog 内容形态；与 `changelog-producer` 有耦合约束（见 E） | `deduplicate` | 3.2 |
| `bucket` | 建表 WITH | 分桶数 | 流读并行度上限；写吞吐；改动需 rescale | — | 15.4 / 16.6 |
| `full-compaction.delta-commits` | 建表 WITH | 多少次 commit 触发一次 full compaction | `full-compaction` 模式下的 changelog 延迟 | 1 | 3.1 |
| `precommit-compact` | 建表 WITH | writer 后加算子合并小 changelog 文件 | 小文件治理；额外算子开销 | false | 15.5 |

### D.2 消费侧 / 读取选项（可用 hint、动态选项、临时视图覆盖，不动 schema）

| 配置 | 对象 | 作用面 | 影响面 | 默认 | 详见 |
|------|------|--------|--------|------|------|
| `scan.mode` | 读侧 hint | 流读起点（全量+增量 / 仅增量 / compacted 等） | 范式一丢基线会算错；范式三用 `latest` | `default` | 15.2 |
| `scan.snapshot-id` / `scan.timestamp-millis` | 读侧 hint | 从指定快照/时间点起读 | 单独设即可，自动推断 scan.mode | — | 15.2 |
| `consumer-id` | 读侧 hint | 按消费进度保留快照 + 断点续读 | 防快照过期；正确性/稳定性 | — | 15.1 |
| `consumer.mode` | 读侧 hint | `exactly-once` / `at-least-once` | 续读精度；切换不能恢复状态 | `exactly-once` | 15.1 |
| `scan.remove-normalize` | 读侧 hint | 强制去掉 normalize 算子 | ⚠️ 下游需回撤时去掉会算错 | false | 16.2 |
| `lookup.cache` | 维表 hint/WITH | lookup 缓存模式 FULL/PARTIAL/AUTO | 点查性能 vs 维度时效（TTL 内读旧值） | `AUTO` | 17.5 |
| `lookup.async` | 维表 hint/WITH | 异步点查 | 吞吐↑；同 key 顺序不保证 | false | 17.5 |
| `scan.partitions` (`max_pt()`) | 维表 hint | 只关联最新/指定分区 | 维表扫描量；按 refresh-interval 刷新 | — | 17.5 |

### D.3 表属性中的「消费生命周期」（设在表上，写作业触发）

| 配置 | 对象 | 作用面 | 影响面 | 详见 |
|------|------|--------|--------|------|
| `consumer.expiration-time` | `ALTER TABLE SET` | consumer 的存活时长 | 防误配 consumer 永久占用快照；过期由写作业触发 | 15.1 |
| `snapshot.num-retained-max` / `snapshot.time-retained` | 建表/`ALTER` | 快照保留 | 消费跟不上会读不到老快照 | 15.1 / 16.4 |

### D.4 状态后端 / 引擎侧（Flink 配置，不是 Paimon 选项）

| 配置 | 对象 | 作用面 | 影响面 | 详见 |
|------|------|--------|--------|------|
| `execution.runtime-mode` | 会话 SET | 流读 or 批读 | 开启流读靠它，不是 `'streaming'='true'` | 15.2 |
| `table.exec.state.ttl` | 会话 SET | regular join / 聚合状态存活 | 防状态膨胀；过短会漏关联/算错 | 17.2 |
| `table.dynamic-option.*` | 作业配置 | 不改 SQL 注入读取选项 | 作业级覆盖；优先级 > 表选项 | 16.5 |
| checkpoint 间隔 | 会话/作业 | 数据可见性节奏 | 延迟基准；小文件压力 | 2.1 / 4.1 |

---

## E. 特性组合矩阵与场景适配（用好工具的关键）

> **用好 Paimon+Flink 的关键不在单点配置，而在「多特性组合的效应」与「场景需求」是否适配。** 这一节把散落全篇的组合判断集中起来。

### E.1 必须协同的耦合约束（配错直接错）

| 组合 | 约束 | 后果 |
|------|------|------|
| `merge-engine=partial-update` + 流读 | **必须配 `changelog-producer=lookup` 或 `full-compaction`** | 配 `none`/`input` 流读语义错/报错 |
| `merge-engine=aggregation` + 流读 | **必须配 `lookup` 或 `full-compaction`** | 同上 |
| `merge-engine=first-row` | 只支持 `none` / `lookup` changelog-producer | 配其它报错 |
| 下游流式聚合 | 上游 `changelog-producer` 必须 `input`/`lookup`（**不可 `none`**） | `none` 要么算错，要么 normalize 爆状态 |
| 范式一（全表状态） | 默认 scan（全量+增量），**不可 `scan.mode=latest`** | 丢初始全量基线，余额算错 |
| 带回撤结果出 Kafka | **必须 `upsert-kafka`** | 普通 `kafka` 遇 `-U/-D` 报错/丢语义 |
| Query Service | 仅 streaming + `HASH_FIXED` bucket + 有主键 | 否则抛 `UnsupportedOperationException` |
| Paimon 物理表做 lookup join | 主流先建视图补 `PROCTIME()` | 物理表不能直接加计算列 |

### E.2 「场景 → 组合配方」速查

| 场景 | changelog-producer | merge-engine | scan.mode | join 方式 | 出口 |
|------|--------------------|--------------|-----------|-----------|------|
| 完整 CDC 入湖（OGG/Debezium） | `input` | `deduplicate` | — | — | — |
| append JSON 入湖、下游要聚合 | `lookup` | `deduplicate` | — | — | — |
| 多源拼宽表（按列更新） | `lookup` | `partial-update` | — | — | Paimon |
| 实时指标预聚合落表 | `lookup` | `aggregation` | — | — | Paimon |
| 全表状态算余额/汇总（范式一） | 上游 `input`/`lookup` | `deduplicate` | 默认（全量+增量） | — | `upsert-kafka` |
| 订单补维度（范式二） | 维表无要求 | — | 维表读最新快照 | lookup join | 看是否带更新 |
| 明细持续分发（范式三） | 主键表 changelog / Append | — | `latest` | — | 普通 `kafka` |
| 双流对账 | 两边完整 changelog | `deduplicate` | 默认 | regular join + `state.ttl` | `upsert-kafka` |
| 按业务时点取汇率/价格 | 维表完整 changelog | `deduplicate` | — | event-time temporal join | — |

### E.3 高频「踩雷组合」（看到就警惕）

| 雷组合 | 为什么错 | 正解 |
|--------|---------|------|
| `none` + 下游 `SUM`/`COUNT` | 拿不到旧值，normalize 爆状态或算错 | 上游改 `input`/`lookup` |
| `input` + append JSON 源 + 下游聚合 | 全是 `+I`，更新被当新增，重复累加 | 改 `lookup` 让 Paimon 自产回撤 |
| `partial-update`/`aggregation` + `input` + 流读 | 违反耦合约束 | 改 `lookup`/`full-compaction` |
| 范式一 + `scan.mode=latest` | 丢全量基线 | 用默认 scan |
| Paimon 表 + regular join（当 Kafka 用） | 启动灌全量、状态爆 | 维度补全改 lookup join |
| 带回撤结果 + 普通 `kafka` sink | `-U/-D` 报错/丢语义 | 改 `upsert-kafka` |
| 中间层全用 `full-compaction` | 延迟逐层放大到分钟级以上 | 中间层用 `input`/`lookup` |
| 线上对运行作业 `ALTER` 改 `changelog-producer`/`bucket` | 新旧 snapshot 语义不一致/数据错乱 | 停—改—重启 |

### E.4 一句话方法论

> 先定**场景属于哪种范式**（E.2 选配方）→ 检查**组合是否触犯耦合约束**（E.1）→ 规避**踩雷组合**（E.3）→ 最后用 D 表逐项确认每个配置的影响面。**单点配置是字母，组合配方才是单词，场景适配才是句子。**

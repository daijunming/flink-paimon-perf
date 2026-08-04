# Steering 起始套件（通用 · 可移植）

本套件从一个生产级项目的 `.kiro/steering/` 提炼而来，**抹去了原项目的领域事实**（外部系统名、协议、模块名等），只保留可跨项目复用的「怎么协作、怎么思考、怎么审查、怎么组织、怎么防失控」的工作护栏。用于**新建工程**时快速建立 AI 协作底线。

它不是任何仓库的 active steering，也不会被自动加载——它是**模板**，供复制。

## 怎么用（为新项目实例化）

1. 把需要的护栏文件复制进新项目的 `.kiro/steering/`。
2. 填 `fact-base/` 里的骨架（product / structure / tech / AGENTS）——这是**每个项目自己的事实**，套件只给空模板，绝不含预设内容。
3. 按下表设置各文件 frontmatter 的 `inclusion`。
4. **按项目裁剪**：不需要的删掉（无 Web 界面删 web-visualization-design、非 Python 删 python-code-smells）。**别把整套原样塞进去**——那会把「规则压力集中在编码期」这个病一起搬过去。

## 文件与建议 inclusion

护栏（通用，可移植）：

| 文件 | 作用 | 建议 inclusion |
|---|---|---|
| working-agreement | 协作底线：环节、事实/推断、收边界 | always |
| work-deviation-triggers | 自检五机制（廉价代理替真实目标） | always |
| work-deviation-triggers-full | 上条的完整复盘参考 | manual |
| coding-principles | 改动行动准则 | always |
| ai-coding-decision-authority-model | AI 编程决策权泄漏与恢复模型 | manual（或只把 §0 设常驻） |
| software-design-principles | 设计/重构检查清单 | auto（设计/重构任务） |
| project-code-organization | 源码组织与放置 | auto（增删移文件时） |
| code-review-handbook | 代码审查手册（层级×维度×视角） | auto（审查任务） |
| observability-debuggability | 操作者可观测性 M1–M7 | auto（碰 I/O 边界/入口时） |
| question-clarification | 带预设前提的问题先澄清 | auto |
| correction-triggers | 纠正触发词（用户纠偏工具） | manual |
| git-commit-language | 提交信息语言口径 | manual（或生成 commit 时 auto） |

语言/领域可选（按项目取用，不适用就删）：

| 文件 | 何时用 |
|---|---|
| python-code-smells | Python 项目 |
| tob-product-principles(+full) | To B / 企业级产品判断 |
| web-visualization-design | 有 Web 可视化界面（界面设计判断透镜） |
| frontend-console | 有前端代码（React/Vue/Svelte 等）时的前端编码规则 |

事实底座骨架（`fact-base/`，新项目填自己的事实后放进 `.kiro/steering/`）：

| 骨架 | 填什么 | inclusion |
|---|---|---|
| product.template | 产品定位、服务谁、做/不做的硬边界、稳定输出契约 | always |
| structure.template | 模块职责、依赖方向、命名规则 | always |
| tech.template | 技术栈、运行入口、环境变量/配置合同、测试命令 | always |
| AGENTS.template | 项目级 agent 约定入口：指向本项目 steering、合同边界、验证 | always（仓库根 AGENTS.md） |

## 原则

- **套件是起点，不是清单**：按项目阶段和领域裁剪，只把当前真需要的设 always，其余 auto/manual。
- **事实与护栏分离**：护栏可移植；事实（product/structure/tech）每个项目自己写，套件只给骨架。
- **别搬来就重**：护栏是为了让人守住掌控，不是为了完整；每加一条 always 都在占每个任务的审查带宽。

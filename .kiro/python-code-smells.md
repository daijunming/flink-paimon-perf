---
inclusion: fileMatch
fileMatchPattern: ['**/*.py']
---

# Python 代码异味检查

用于编辑、评审或重构 Python 代码和 Python 工程配置。目标是可读、可测、类型足够、改动安全。

## 1. 结构和导入

避免：

- 为当前补丁临时创建无归属文件，或随手新增万能模块（清单与判定见 `project-code-organization.md` 第 2 节）；
- 生产代码导入测试 fixture 或 mock；
- 循环导入、`sys.path` 黑魔法、`from module import *`；
- import 阶段执行 I/O、网络调用、真实配置加载或任务启动。

出现循环导入时，先检查模块边界，不要用局部 import 掩盖问题，除非有明确理由。

## 2. 命名和类型

同一对象不要在不同地方反复换名字。`case`、`record`、`snapshot`、`result`、`item`、`payload`、`data` 只有在语义不同的时候才分别使用。

警惕：

- 跨模块 API 使用裸 `dict` 或无类型 `list`；
- 公共函数没有类型标注；
- 大量 `Any`；
- 可选和必填字段不清楚；
- 一个函数签名塞多个布尔开关；
- 用裸字符串表达状态，没有常量、枚举或 schema。

边界数据优先使用项目已有约定；没有约定时，考虑 `dataclass`、`TypedDict`、Pydantic 模型或枚举。

```python
# 反例：裸 dict + 多个布尔开关，调用方靠猜
def build(data: dict, retry: bool, async_: bool, dry: bool): ...

# 正例：结构化输入 + 枚举表达模式
@dataclass
class BuildRequest:
    payload: RequestPayload
    mode: RunMode

def build(req: BuildRequest) -> BuildResult: ...
```

## 3. Python 运行时特有问题

避免：

- 可变默认参数；
- 隐藏全局可变状态；
- 构造函数里做重 I/O 或远程调用；
- 看着像查询、实际改状态的函数；
- 测试以外的 monkey patch；
- 同步和异步边界不清；
- async 函数里做阻塞 I/O；
- 库代码里散落 `asyncio.run()`。

```python
# 反例：多次调用共享同一个 list
def collect(items=[]):
    items.append(1)
    return items

# 正例
def collect(items: list[int] | None = None) -> list[int]:
    items = [] if items is None else items
    items.append(1)
    return items
```

## 4. 函数、异常和日志

一个函数或类不要同时解析输入、读配置、校验数据、应用业务规则、调用外部服务、修改状态、格式化输出、记录错误。

避免：

- 裸 `except`；
- `except Exception: pass`；
- 吞掉错误且不留上下文；
- 用 `print()` 做项目日志；
- 把密钥、token、凭据或敏感记录写进日志；
- 错误日志漏掉对象、阶段、依赖或原因。

```python
try:
    fetch(resource_id)
except HTTPTimeout as e:
    logger.warning(
        "拉取超时 resource_id=%s stage=fetch dep=upstream_http "
        "reason=%s impact=result_missing retryable=true",
        resource_id,
        e,
    )
```

## 5. 配置、依赖、测试

避免硬编码端口、路径、URL、账号、队列名或环境名；测试配置混进生产配置；为几行小逻辑引入新依赖；核心逻辑直接绑定厂商 SDK；无固定版本或无理由的依赖变更。

测试避免只测 happy path、依赖真实网络/数据库/平台、fixture 过大、断言实现细节、改动后没有验证路径。

核心逻辑应能在不依赖真实外部系统的情况下被测试。

从验收标准/属性派生测试时，先判输入空间再定形态，不默认“每条 AC 一个属性测试”：

- 闭集 / 枚举 / 映射表 / 真值表 / 单条件 → 参数化或示例测试（枚举即全覆盖，比随机采样更完整、更快）。
- 大输入空间 / 对抗性（如防泄漏）/ 排序·去重·序列化 / 数值边界与非法值 → 属性测试（PBT）。
- PBT 是有成本的工具（自定义生成器 + 独立 oracle + 上百次迭代），只在“能证伪手写用例想不到的隐蔽 bug”时用；不要把闭集/映射类断言写成 PBT。
- 用 PBT 时不写死 `max_examples`，交由项目测试配置（如 CI / 本地不同档位）统一控制；确需覆盖再局部标注 + 说明理由。

## 6. 重构纪律

发现异味后不要自动大改：

- 与当前改动直接相关：小步修；
- 边界被破坏：先隔离再加新代码；
- 高风险重构：先补测试；
- 无关异味：记录为后续项，不扩大本次范围。

改完后报告：修了哪些异味，还剩哪些，为什么留下。

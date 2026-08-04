---
inclusion: manual
name: git-commit-language
description: 仅在生成 git 提交信息（commit message）时加载，约束提交信息语言与格式口径。其它任务无需加载本文件。
---

# Git 提交信息约定

本文件约束生成 git 提交信息时的格式与语言口径。**语言按团队约定**（下例用中文，团队用英文则正文改英文即可）；格式约定与仓库既有历史保持一致。

## 口径

- 首行格式 `type(scope): 描述`——`type` 与可选 `scope` 用英文，描述用团队语言；
- 常用 `type`：`feat`、`fix`、`docs`、`refactor`、`test`、`chore`、`build`；
- 首行只讲这次改动的目的或结论，不堆过程细节，控制在一行内；
- 需要展开时，空一行后用正文补充背景、改动点和影响，仍遵循先结论后细节。

## 示例

- `docs(readme): 补充离线部署步骤与前置依赖`
- `fix(client): 外部服务不可达时归一化为错误，不再抛裸异常`
- `refactor(core): 抽出终态转换判断，去掉重复分支`

不要把英文类型前缀翻译成团队语言；不要在首行堆过程细节。

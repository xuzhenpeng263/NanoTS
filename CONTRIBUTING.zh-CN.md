# 贡献指南

感谢你对 NanoTS（社区版）的关注。本指南说明如何提交改动、搭建开发环境，并提交高质量的 PR。

## 相关文档

- 项目简介与用法：`README.zh-CN.md`
- 文件格式规范：`SPEC.zh-CN.md`

## 许可证与 CLA

- 许可证：AGPL-3.0-or-later
- 你的贡献会在该许可证下发布。
- 代码贡献前需同意 CLA，详见 `CLA.md`。

## 贡献方式

- Bug 报告与可复现用例
- 文档改进（README、SPEC、API 文档）
- 性能优化（基准测试、性能分析说明）
- 与项目定位一致的新功能

如果不确定是否适合，建议先开 issue 说明方案和使用场景。

## 开发环境

### 前置要求

- Rust 工具链（stable）与 `cargo`
- 可选：Python 3.8+ 与 `maturin`（用于 Python 绑定）

### 编译与测试

```bash
cargo build
cargo test
```

### 运行基准测试

```bash
cargo run --release -- benchmark
```

### Python 绑定（可选）

```bash
cd python/nanots-py
maturin develop --release
```

## 项目约定

### 代码风格

- 优先使用清晰、明确的命名与小而组合性强的函数。
- 公共 API 需要文档注释；行为微妙的地方请补充使用说明。
- 避免不必要的分配，优先采用流式或零拷贝路径。

### 格式化与静态检查

- 格式化：`cargo fmt`
- 静态检查：`cargo clippy --all-targets --all-features`

### 测试

- 新特性与 Bug 修复尽量补充测试。
- 测试输入应尽量小、可读、可复现。
- 变更文件格式时请更新 `SPEC.md`，并补充回归测试。

## 性能相关变更

如果涉及存储布局、codec 或热点路径：

- 在 PR 描述中补充简单的 benchmark 对比。
- 说明数据集形状、行数，以及与基线的差异。

## PR 检查清单

- Issue/意图清晰（issue 或 PR 描述）
- 本地构建与测试通过
- 行为变更对应文档已更新
- 未引入非预期的格式或 API 破坏

## 安全问题报告

请不要在公开 issue 中报告安全问题。请直接联系维护者。

## 版本变更说明

- 版本记录于 `Cargo.toml` 与 `python/nanots-py/pyproject.toml`。
- 如需升级版本，请在 PR 中明确说明。

## 提交建议

- 提交要小而聚焦，逻辑分组清晰。
- 提交信息建议写清楚“做了什么”和“为什么”。

感谢你的贡献。

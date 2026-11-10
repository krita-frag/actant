# 测试

## 分层

```text
tests/
├── python/
│   ├── unit/          # 纯 Python 单元测试（task / flow / runtime / dispatch / cli）
│   ├── integration/   # capability / runtime fallback / worker lifecycle
│   └── e2e/           # 跨节点 P2P（真实 iroh）
└── rust/
    ├── unit/          # 镜像 src/ 结构
    └── property/      # DAG 不变量、payload 签名/篡改
```

## 命令

```bash
uv run pytest tests/python/ -v
uv run pytest tests/python/ -n 4 -v
uv run pytest tests/python/unit/test_flow.py -v

cargo test --workspace
cargo test -p actant-core -p actant-common --no-default-features
```

`--workspace` 不能省略：本仓库 `workspace_default_members` 只有根包，而根包仅 2 个
`#[test]`，裸 `cargo test` 不会执行 `crates/actant-core` 的单元测试。
`--no-default-features` 只作用于两个框架 crate（根包是 PyO3 绑定层）。

## 约定

- 每次改动后运行受影响测试，例如 `uv run pytest tests/python/<module>/`。
- 测试必须确定性；外部依赖需 mock。
- 端到端测试用 `threading.Event` 做就绪信号。
- 异步 Rust 测试用 `#[tokio::test]`；单元测试放在同文件 `#[cfg(test)] mod tests`。
- Rust 基准测试：`cargo bench --bench <name>`。

## 构建新鲜度

测试会在 session 启动时比较已加载原生扩展与 Rust 构建输入（`Cargo.toml`、
`src/**/*.rs`、`crates/*/src/**/*.rs`）的 mtime。任一输入比 `.so` 新时直接
失败并提示先运行 `uv run maturin develop`，避免测试跑在过期实现上。确需跳过
（仅排查场景）可设置 `ACTANT_SKIP_BUILD_CHECK=1`。

改动 Rust 后必须先重建扩展，再运行 Python 测试。

## 文档门禁

文档与 API 参考同样受门禁约束：

```bash
uv run python scripts/gen_api_reference.py --check           # 参考页是否最新
uv run python scripts/api_surface.py --check                 # 公共 API 表面快照
uv run pytest tests/python/unit/test_docs_contract.py -q     # 公共 API docstring 契约
uv run mkdocs build --strict                                 # 死链与构建错误
```

改动 Rust 热路径（路由、调度、任务分发、传输、事件日志、上下文）后跑
`tests/python/unit/test_hot_path_guard.py`：它静态扫描这些文件，禁止出现
capability 三动词调用与 GIL 附加。新增热路径文件要同步登记该测试的
`_HOT_PATH_FILES`。

改动 `actant/actant.pyi`、配置类字段或 CLI 后，先重新生成参考页：

```bash
uv run python scripts/gen_api_reference.py
```

CI 在每次 push/PR 执行以上三项；生成页与源码不一致会直接失败。

## 行为基线

运维承诺与测试的对应关系见 [SLA 基线](../operations/sla-baseline.md)。改动 SLA 相关行为时，必须同时更新该页与对应测试。

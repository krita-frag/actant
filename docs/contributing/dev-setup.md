# 开发环境

## 初始化

```bash
git clone https://github.com/actant/actant.git
cd actant
uv sync --extra dev
uv run maturin develop
```

## 常用命令

```bash
uv run maturin develop                    # 构建 Rust 核心 + 安装 Python 包

uv run pytest tests/python/ -v            # Python 测试
uv run pytest tests/python/ -n 4 -v       # 并行 Python 测试
cargo test --workspace                    # Rust 全量测试（默认含 PyO3 模块）
cargo test -p actant-core -p actant-common --no-default-features  # 框架构建

cargo clippy --all-targets -- -D warnings # Rust 静态检查（零告警）
cargo fmt --check                         # Rust 格式化检查
ruff check actant tests/python            # Python 静态检查
mypy actant                               # Python 类型检查

cargo bench --bench scheduler             # Rust 基准测试
cargo audit                               # 依赖漏洞扫描
```

## 文档

```bash
uv run mkdocs serve          # 本地预览
uv run mkdocs build --strict # 严格构建（CI 同款）

uv run python scripts/gen_api_reference.py         # 重新生成 API / 配置 / CLI 参考页
uv run python scripts/gen_api_reference.py --check # 校验生成页是否最新（CI 同款）
```

站点输出到 `site/`，不要提交。参考页由源码生成，不要手工编辑。

### 版本化发布（mike）

`mkdocs.yml` 已配置 `extra.version.provider: mike`，站点按版本归档、`latest` 指向最新版本：

```bash
uv run mike deploy --push --update-aliases 0.3 latest  # 构建并推送到 gh-pages
uv run mike set-default --push latest                  # 默认打开最新版本
uv run mike serve                                      # 本地预览全部版本
uv run mike list                                       # 查看已发布版本
```

`mike deploy` 需要仓库已配置推送权限（CI 中由 `GITHUB_TOKEN` 提供）。不执行 `mike deploy` 时，`mkdocs serve` / `mkdocs build` 仍按当前源码渲染，不显示版本选择器。

## 分层边界自检

改动跨层时先确认：

- `crates/actant-core` 不依赖 PyO3。
- `crates/actant-common` 不依赖 `actant-core`。
- `cargo test -p actant-core -p actant-common --no-default-features` 仍通过。

## 相关页面

- [测试](testing.md)
- [代码约定](code-style.md)
- [文档地图](doc-map.md)

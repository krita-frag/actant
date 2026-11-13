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
uv run maturin develop          # 构建 Rust 核心并安装 Python 包
uv run pytest tests/python/ -v  # Python 测试

cargo test --workspace          # Rust 全量测试（含 PyO3 模块）
cargo clippy --all-targets -- -D warnings
cargo fmt --check

ruff check actant tests/python benches/ extensions/act-ext-testkit extensions/act-service
mypy actant
```

并行测试、框架构建、基准与依赖扫描等完整命令见[测试](testing.md)。

## 文档

```bash
uv run mkdocs serve          # 本地预览
uv run mkdocs build --strict # 严格构建（CI 同款）

uv run python scripts/gen_api_reference.py         # 生成 API / 配置 / CLI 参考页
uv run python scripts/gen_api_reference.py --check # 校验生成页（CI 同款）
```

站点输出到 `site/`，不要提交；参考页由源码生成，不要手工编辑。

### 版本化发布（mike）

`mkdocs.yml` 已配置 `extra.version.provider: mike`：

```bash
uv run mike deploy --push --update-aliases 0.4 latest
uv run mike set-default --push latest
uv run mike serve
uv run mike list
```

未执行 `mike deploy` 时，`mkdocs serve` / `mkdocs build` 按当前源码渲染，不显示版本选择器。

### 发布与产物

`.github/workflows/release.yml` 只在推送 `v*` tag 时运行：校验版本 → 构建并上传 wheel /
sdist artifact（7 天）→ 合并 `dist` 执行 `twine check` → 通过 Trusted Publishing 发布
PyPI → 用 `gh release create` 附加到 GitHub Release。PyPI 侧需预先登记 Trusted
Publisher（workflow `release.yml`、environment `pypi`）。上传依赖 GitHub 服务端，本机
`act` / `maturin` 只验证构建。

## 相关页面

- [测试](testing.md)
- [代码约定](code-style.md)
- [文档地图](doc-map.md)

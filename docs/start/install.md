# 安装

## 前置条件

- 安装预编译 wheel：Python >= 3.10，不需要 Rust。
- 从源码构建：额外需要 Rust 工具链（通过 [rustup](https://rustup.rs/) 安装）与
  [uv](https://docs.astral.sh/uv/)。

## 从源码构建

```bash
git clone https://github.com/actant/actant.git
cd actant
uv sync --extra dev
uv run maturin develop
```

验证安装：

```bash
uv run python -c "import actant; print(actant.__version__)"
```

## 安装预编译 wheel

发布版本提供 abi3 wheel，一个平台包覆盖 CPython 3.10+：

| 平台 | wheel 标签 |
| --- | --- |
| Linux x86_64 / aarch64 | `manylinux_2_28_*` |
| macOS arm64 + x86_64 | `macosx_*_universal2` |
| Windows x64 | `win_amd64` |

```bash
uv pip install actant-engine
# 或指定镜像
uv pip install actant-engine --index-url https://pypi.org/simple
```

发行名是 `actant-engine`（PyPI 的 `actant` 已被同名项目占用），导入包与 CLI 命令仍是 `actant`。

musllinux 与 Windows arm64 暂未提供 wheel，可从 sdist 构建（需要 Rust 1.91）：

```bash
uv pip install --no-binary actant-engine actant-engine
```

## 构建产物说明

`maturin develop` 会编译 Rust workspace 并安装 Python 包。模块名为 `actant.actant`，由 PyO3 生成；纯 Python 层位于 `actant/`。

## 文档站点

```bash
uv sync --extra dev
uv run mkdocs serve
```

浏览器打开 `http://127.0.0.1:8000`。构建静态站点：

```bash
uv run mkdocs build --strict
```

输出在 `site/`（已加入 `.gitignore`）。

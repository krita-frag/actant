# 安装

## 前置条件

- Rust 工具链（通过 [rustup](https://rustup.rs/) 安装）
- [uv](https://docs.astral.sh/uv/)
- Python >= 3.10

## 从源码构建

```bash
git clone https://github.com/actant/actant.git
cd actant
uv sync
uv run maturin develop
```

验证安装：

```bash
uv run python -c "import actant; print(actant.__version__)"
```

## 构建产物说明

`maturin develop` 会编译 Rust workspace 并安装 Python 包。模块名为 `actant.actant`，由 PyO3 生成；纯 Python 层位于 `actant/`。

## 文档站点

```bash
uv sync
uv run mkdocs serve
```

浏览器打开 `http://127.0.0.1:8000`。构建静态站点：

```bash
uv run mkdocs build --strict
```

输出在 `site/`（已加入 `.gitignore`）。

"""扩展点统一契约门禁。

`actant/extension_points.json` 是扩展点的唯一事实源：文档、CLI 与门禁都读它。
本模块负责证明清单与源码没有脱节：

- 集合相等：rust 层的注入符号必须与 ``RuntimeBuilder`` 的 trait 缝一一对应，
  python 层必须与 ``BUILTIN_CAPABILITIES`` 一一对应；
- 每个 ``entrypoint`` / ``production_call_site`` / ``acceptance_test`` 都必须
  指向真实存在的符号，且生产调用点不得落在 tests / examples / docs；
- ``cross_node_safe`` 按层固定，把「进程内闭包在 failover 后失效」变成可查字段；
- ``resolution`` / ``hot_path`` 约束执行域：热路径点必须构建期定稿；
- 热路径白名单文件不得出现 capability 动态分派调用或 GIL 附加；
- 扫描器自检：正则写错导致恒返回空集合时必须失败，而不是假绿。

新增扩展点必须同时更新清单、生成页、CLI 可见的列表与验收测试，否则这里变红。
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from actant.capabilities import BUILTIN_CAPABILITIES

ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "actant" / "extension_points.json"
BUILDER = ROOT / "crates" / "actant-core" / "src" / "runtime" / "builder.rs"
PY_BRIDGE_DIR = ROOT / "src" / "py"
GENERATED_PAGE = ROOT / "docs" / "reference" / "extension-points.md"
CLI = ROOT / "actant" / "cli.py"
CI = ROOT / ".github" / "workflows" / "ci.yml"

_REQUIRED_FIELDS = frozenset(
    {
        "id",
        "layer",
        "entrypoint",
        "production_call_site",
        "cross_node_safe",
        "resolution",
        "hot_path",
        "acceptance_level",
        "acceptance_test",
        "summary",
    }
)
_LAYERS = {"rust": True, "bridge": False, "python": False}
_ACCEPTANCE_LEVELS = {"behavior", "wiring", "unit"}
_RESOLUTIONS = {"build_time", "runtime"}
_PRODUCTION_FORBIDDEN = {"tests", "examples", "docs"}

# Rust 引擎的每任务 / 每消息路径。这里是门禁自身的知识，不放进清单：
# 清单的 production_call_site 记的是装配位置，不能用来判断调用频率。
_HOT_PATH_FILES = (
    "crates/actant-core/src/runtime/workflow/runtime.rs",
    "crates/actant-core/src/runtime/workflow/orchestrator/persistence.rs",
    "crates/actant-core/src/runtime/workflow/gossip.rs",
    "crates/actant-core/src/runtime/workflow/failover.rs",
    "crates/actant-core/src/runtime/workflow/runtime/network_router.rs",
    "crates/actant-core/src/runtime/context.rs",
)

# 动态分派进入热路径的形态。只禁「调用」，不禁类型名与网络帧序列化：
# context.rs 合法持有 CapabilityRuntime，热路径文件本来就用 postcard 编码网络帧。
_FORBIDDEN_IN_HOT_PATH: tuple[tuple[str, str], ...] = (
    (r"CapabilityRuntime::(?:ask|perform|emit)", "CapabilityRuntime 三动词"),
    (r"\.(?:ask|perform|emit)::<", "capability 三动词 turbofish"),
    (r"\bcapability\.(?:ask|perform|emit)\(", "capability 三动词方法调用"),
    (r"Python::(?:attach|try_attach|with_gil)", "PyO3 GIL 附加"),
)


def _manifest() -> dict[str, Any]:
    data: dict[str, Any] = json.loads(MANIFEST.read_text(encoding="utf-8"))
    return data


def _points() -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = _manifest()["points"]
    return points


def _parse_ref(ref: str) -> tuple[Path, list[str]]:
    """``path::symbol::method`` -> (绝对路径, 符号列表)。"""
    parts = ref.split("::")
    assert len(parts) >= 2, f"引用缺少符号: {ref}"
    return ROOT / parts[0], parts[1:]


def _symbol_exists(path: Path, symbol: str) -> bool:
    if not path.is_file():
        return False
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".rs":
        pattern = rf"\b(?:fn|struct|enum|trait)\s+{re.escape(symbol)}\b"
    else:
        pattern = rf"^\s*(?:class|(?:async\s+)?def)\s+{re.escape(symbol)}\b"
    return re.search(pattern, text, re.MULTILINE) is not None


def _builder_trait_seams() -> set[str]:
    """builder.rs 中参数带 ``Arc<dyn ...>`` 的 ``pub fn with_*``。

    ``with_role`` / ``with_orchestrator_ingest`` / ``with_data_dir`` 是配置开关，
    不是 trait 缝，故按签名是否含 ``Arc<dyn`` 区分。
    """
    text = BUILDER.read_text(encoding="utf-8")
    seams: set[str] = set()
    for match in re.finditer(r"pub fn (with_\w+)\(", text):
        if "Arc<dyn" in text[match.start() : match.start() + 200]:
            seams.add(match.group(1))
    return seams


# Python callable 被 Rust 侧调用的形态：`Py<PyAny>` 上的动态调用。
# 只认 `try_attach` 会把反方向的实现（Rust 线程执行 Python 闭包，如
# `gil_thread.rs`）误判成扩展点——它不持有任何 `Py<PyAny>`，也不做 call 分派。
_PY_CALLBACK_CALL = re.compile(r"\.call(?:\d+|_method)\(")


def _try_attach_bridge_files() -> set[str]:
    """src/py/ 中真正把 Python callable 接到 Rust 热路径的桥文件。"""
    bridges = set()
    for path in PY_BRIDGE_DIR.glob("*.rs"):
        text = path.read_text(encoding="utf-8")
        if "try_attach" in text and _PY_CALLBACK_CALL.search(text):
            bridges.add(path.relative_to(ROOT).as_posix())
    return bridges


def _hot_path_violations(text: str) -> list[str]:
    """返回文本中出现的动态分派形态标签。"""
    return [
        label
        for pattern, label in _FORBIDDEN_IN_HOT_PATH
        if re.search(pattern, text) is not None
    ]


# ---------------------------------------------------------------------------
# 扫描器自检：以下断言保证上面的正则确实看得见已知源码，防止恒返回空造成假绿。
# ---------------------------------------------------------------------------


def test_scanners_see_known_sources() -> None:
    known_seams = {
        "with_scheduler",
        "with_route_policy",
        "with_task_dispatcher",
        "with_transport",
        "with_discovery",
        "with_event_log",
    }
    assert known_seams <= _builder_trait_seams()
    assert {"src/py/route_policy.rs"} <= _try_attach_bridge_files()
    assert "src/py/gil_thread.rs" not in _try_attach_bridge_files()
    assert _symbol_exists(BUILDER, "init_worker")
    assert _symbol_exists(ROOT / "actant/_internal/runtime.py", "layer")
    assert not _symbol_exists(BUILDER, "definitely_not_a_symbol")
    for rel in _HOT_PATH_FILES:
        assert (ROOT / rel).is_file(), f"热路径白名单文件不存在: {rel}"
    assert _hot_path_violations("self.capability.perform::<Store>(req).await")
    assert _hot_path_violations("Python::try_attach(|py| Ok(()))")
    assert not _hot_path_violations(
        "self.network.broadcast(TOPIC, bytes).await;\n"
        "let held: Arc<CapabilityRuntime> = runtime.clone();"
    )


# ---------------------------------------------------------------------------
# 清单自身的结构与分层约束
# ---------------------------------------------------------------------------


def test_manifest_schema_and_unique_ids() -> None:
    data = _manifest()
    assert data["schema"] == 1
    points = data["points"]
    assert points, "扩展点清单为空；扫描器是否失效？"
    ids = [point["id"] for point in points]
    assert len(ids) == len(set(ids)), f"重复 id: {ids}"
    for point in points:
        assert point.keys() >= _REQUIRED_FIELDS, f"{point['id']} 字段不全"
        assert point["layer"] in _LAYERS, f"{point['id']} 层未登记"
        assert isinstance(point["cross_node_safe"], bool)
        assert point["resolution"] in _RESOLUTIONS, (
            f"{point['id']} 解析时机未登记: {point['resolution']}"
        )
        assert isinstance(point["hot_path"], bool), f"{point['id']}.hot_path 必须是 bool"


def test_resolution_and_hot_path_are_consistent() -> None:
    points = _points()
    for point in points:
        pid = point["id"]
        if point["hot_path"]:
            assert point["resolution"] == "build_time", (
                f"{pid} 位于热路径却用运行期解析；热路径实现必须构造期冻结"
            )
        if point["layer"] == "rust":
            assert point["resolution"] == "build_time", (
                f"{pid} 是 Rust trait 缝，只能经 RuntimeBuilder 注入"
            )
        if point["layer"] == "python":
            assert point["resolution"] == "runtime" and not point["hot_path"], (
                f"{pid} 是 Python handler 链，不能登记为构建期热路径"
            )
    # 自检：三种组合都必须有真实条目，防止把整列标成同一个值造成假绿。
    assert any(point["hot_path"] for point in points)
    assert any(point["resolution"] == "runtime" for point in points)
    assert any(
        not point["hot_path"] and point["resolution"] == "build_time"
        for point in points
    )


def test_cross_node_safe_is_fixed_by_layer() -> None:
    for point in _points():
        expected = _LAYERS[point["layer"]]
        assert point["cross_node_safe"] is expected, (
            f"{point['id']} 位于 {point['layer']} 层，cross_node_safe 必须为 {expected}；"
            "进程内对象在 failover 后不随工作流迁移"
        )


def test_acceptance_levels_are_honest() -> None:
    levels = [point["acceptance_level"] for point in _points()]
    assert set(levels) <= _ACCEPTANCE_LEVELS
    # 自检：至少存在已升级到行为级的条目，防止所有人被随手标成 wiring / unit。
    assert levels.count("behavior") >= 4, (
        "行为级验收过少；清单是否在没有 e2e 的情况下被降级标注？"
    )


def test_rust_seams_match_builder() -> None:
    declared = {
        point["entrypoint"].split("::")[1]
        for point in _points()
        if point["layer"] == "rust"
    }
    assert declared == _builder_trait_seams(), (
        "清单与 RuntimeBuilder 的 trait 缝不一致；新增/删除 with_* 必须同步清单"
    )


def test_python_capabilities_match_builtin_registry() -> None:
    declared = {point["id"] for point in _points() if point["layer"] == "python"}
    assert declared == set(BUILTIN_CAPABILITIES)


def test_python_to_rust_bridges_are_registered() -> None:
    declared = {
        point["entrypoint"].split("::")[0]
        for point in _points()
        if point["layer"] == "bridge" and point["entrypoint"].startswith("src/py/")
    }
    missing = _try_attach_bridge_files() - declared
    assert not missing, f"存在未登记的 Python→Rust 热路径桥: {sorted(missing)}"


# ---------------------------------------------------------------------------
# 热路径静态护栏：结构性、确定性，不依赖性能阈值断言
# ---------------------------------------------------------------------------


def test_hot_path_files_do_not_dispatch_capabilities() -> None:
    for rel in _HOT_PATH_FILES:
        text = (ROOT / rel).read_text(encoding="utf-8")
        violations = _hot_path_violations(text)
        assert not violations, (
            f"{rel} 位于 Rust 引擎热路径，却出现 {violations}；"
            "该路径的实现必须构造期冻结为 trait 句柄"
        )


# ---------------------------------------------------------------------------
# 每个引用都指向真实符号，且生产/测试路径各守其位
# ---------------------------------------------------------------------------


def test_entrypoints_and_production_call_sites_are_real() -> None:
    for point in _points():
        for field in ("entrypoint", "production_call_site"):
            path, symbols = _parse_ref(point[field])
            assert path.is_file(), f"{point['id']}.{field} 文件不存在: {path}"
            for symbol in symbols:
                assert _symbol_exists(path, symbol), (
                    f"{point['id']}.{field} 符号不存在: {point[field]}::{symbol}"
                )
        production_path, _ = _parse_ref(point["production_call_site"])
        parts = production_path.relative_to(ROOT).parts
        assert not _PRODUCTION_FORBIDDEN.intersection(parts), (
            f"{point['id']} 的生产调用点落在测试/示例/文档里: {point['production_call_site']}"
        )


def test_acceptance_tests_are_real_and_ci_runs_examples() -> None:
    ci_text = CI.read_text(encoding="utf-8")
    for point in _points():
        path, symbols = _parse_ref(point["acceptance_test"])
        rel = path.relative_to(ROOT)
        assert path.is_file(), f"{point['id']} 的验收文件不存在: {rel}"
        assert rel.parts[0] in {"tests", "examples"}, (
            f"{point['id']} 的验收必须落在 tests/ 或 examples/: {rel}"
        )
        for symbol in symbols:
            assert _symbol_exists(path, symbol), (
                f"{point['id']} 的验收符号不存在: {point['acceptance_test']}"
            )
        if rel.parts[0] == "examples":
            assert f"--example {path.stem}" in ci_text, (
                f"{point['id']} 以 example 作为验收，但 CI 没有运行它: {path.stem}"
            )


def test_generated_page_and_cli_expose_the_same_manifest() -> None:
    page = GENERATED_PAGE.read_text(encoding="utf-8")
    for point in _points():
        assert f"`{point['id']}`" in page, f"生成页缺少 {point['id']}，请重跑生成器"
        assert f"`{point['acceptance_level']}`" in page, "生成页缺少验收等级，请重跑生成器"
        assert f"`{point['resolution']}`" in page, "生成页缺少解析时机，请重跑生成器"
    assert "## 执行域" in page, "生成页缺少执行域说明，请重跑生成器"
    cli_text = CLI.read_text(encoding="utf-8")
    assert '"extensions"' in cli_text
    assert "extension_points.json" in cli_text
    assert "resolution=" in cli_text
    assert "hot_path=" in cli_text


def test_uncovered_intents_do_not_claim_to_be_wired() -> None:
    intents = _manifest()["uncovered_intents"]
    wired = {point["id"] for point in _points()}
    assert intents, "未覆盖方向列表为空；完整性状态不可查"
    for intent in intents:
        assert {"id", "summary", "reason"} <= intent.keys()
        assert intent["id"] not in wired, (
            f"{intent['id']} 已接线，必须从 uncovered_intents 移到 points"
        )

"""热路径静态护栏：每任务 / 每消息路径不得出现动态分派与 GIL 附加。

性能边界靠结构守，不靠数值阈值：Rust 引擎的热路径文件只允许调用构造期冻结的
trait 句柄（一次 vtable 调用），不允许经 ``CapabilityRuntime`` 三动词查注册表、
编解码、走 Actor 邮箱往返，也不允许在 tokio worker 上附加 GIL。

只禁**调用形态**，不禁类型名与网络帧序列化：``context.rs`` 合法持有
``CapabilityRuntime``，热路径本来就用 postcard 编码帧。

新增热路径文件要同步登记 ``_HOT_PATH_FILES``；白名单本身由
``test_hot_path_whitelist_files_exist`` 保证不过期。
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]

_HOT_PATH_FILES = (
    "crates/actant-core/src/runtime/workflow/runtime.rs",
    "crates/actant-core/src/runtime/workflow/orchestrator/persistence.rs",
    "crates/actant-core/src/runtime/workflow/gossip.rs",
    "crates/actant-core/src/runtime/workflow/failover.rs",
    "crates/actant-core/src/runtime/workflow/runtime/network_router.rs",
    "crates/actant-core/src/runtime/context.rs",
)

_FORBIDDEN_IN_HOT_PATH: tuple[tuple[str, str], ...] = (
    (r"CapabilityRuntime::(?:ask|perform|emit)", "CapabilityRuntime 三动词"),
    (r"\.(?:ask|perform|emit)::<", "capability 三动词 turbofish"),
    (r"\bcapability\.(?:ask|perform|emit)\(", "capability 三动词方法调用"),
    (r"Python::(?:attach|try_attach|with_gil)", "PyO3 GIL 附加"),
)


def _hot_path_violations(text: str) -> list[str]:
    """返回文本中命中的动态分派形态标签。"""
    return [
        label for pattern, label in _FORBIDDEN_IN_HOT_PATH if re.search(pattern, text) is not None
    ]


def test_hot_path_whitelist_files_exist() -> None:
    for rel in _HOT_PATH_FILES:
        assert (ROOT / rel).is_file(), f"热路径白名单文件不存在: {rel}"


def test_hot_path_files_do_not_dispatch_capabilities() -> None:
    for rel in _HOT_PATH_FILES:
        text = (ROOT / rel).read_text(encoding="utf-8")
        violations = _hot_path_violations(text)
        assert not violations, (
            f"{rel} 位于 Rust 引擎热路径，却出现 {violations}；"
            "该路径的实现必须构造期冻结为 trait 句柄"
        )


def test_scanner_sees_known_violations() -> None:
    """扫描器自检：正则写错导致恒返回空集合时必须在这里变红，而不是假绿。"""
    assert _hot_path_violations("self.capability.perform::<Store>(req).await")
    assert _hot_path_violations("CapabilityRuntime::emit(&ctx, ev).await")
    assert _hot_path_violations("Python::try_attach(|py| Ok(()))")
    assert _hot_path_violations("self.capability.emit(ev).await")
    assert not _hot_path_violations(
        "self.network.broadcast(TOPIC, bytes).await;\n"
        "let held: Arc<CapabilityRuntime> = runtime.clone();"
    )

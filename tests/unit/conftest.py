"""unit/ 目录共享 fixtures。"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

from actant.flow import FlowContext
from actant.task import clear_global_tasks

if TYPE_CHECKING:
    from types import ModuleType


@pytest.fixture
def reset_flow_context_local() -> None:
    """每个测试后重置全局 flow context。"""
    flow_mod: ModuleType = sys.modules["actant.flow"]
    old = getattr(flow_mod._context_local, "flow_context", None)
    yield
    flow_mod._context_local.flow_context = old


@pytest.fixture
def reset_global_task_registry() -> None:
    """每个测试前清空全局任务注册表。"""
    clear_global_tasks()
    yield
    clear_global_tasks()


@pytest.fixture
def fresh_flow_context() -> FlowContext:
    """返回一个干净的 FlowContext 实例。"""
    return FlowContext()

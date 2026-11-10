"""从 @task 到 ERH：渐进式深入 Actant 扩展模型。

本教程按 4 个阶段从高层 API 逐步下探到底层 ERH 原语，帮助用户理解
何时需要从 ``@task`` 切换到 ``ask``/``perform``/``emit``，以及如何组合
自定义 capability handler 实现业务策略。

运行::

    uv run python examples/task_to_erh.py

## 阶段概览

1. **阶段 1：@task 高层 API**——零配置提交任务，适合 80% 的常见场景。
2. **阶段 2：自定义 ask capability**——用 ``ask`` 组合用户自定义路由决策。
3. **阶段 3：任务重试**——重试由 Rust Worker 执行，不是 Python capability。
4. **阶段 4：自定义 emit capability**——用 ``emit`` 多播审计事件，
   演示如何声明全新 capability 并注册 handler 链。
"""

from __future__ import annotations

from actant import Runtime, emit
from actant.capabilities import RouteCtx
from actant.effects import ask
from actant.tasks import task


def stage_1_task_api() -> None:
    """``@task`` 是最简 API：装饰一个函数即可提交到 Runtime 执行。

    ``Runtime()`` 不注册 Python 策略 handler；任务执行、
    超时与重试由 Rust Worker 负责。此阶段适合：
    - 单节点任务执行
    - 不需要自定义路由/重试/调度策略
    - 快速原型开发
    """
    print("=== 阶段 1：@task 高层 API ===")

    @task
    def fetch_url(url: str) -> int:
        """模拟 HTTP 请求，返回状态码。"""
        print(f"  [fetch_url] processing {url}")
        return 200

    with Runtime():
        # submit() 异步提交，返回 AsyncResult 句柄
        handle = fetch_url.submit("https://example.com")
        # result() 阻塞等待结果
        status = handle.result(timeout=5.0)
        print(f"  fetch_url -> {status}")

        # 也可以直接同步调用（不走 Runtime，立即执行）
        status_sync = fetch_url("https://sync.example.com")
        print(f"  sync call -> {status_sync}")

    print()


def stage_2_custom_ask() -> None:
    """用 ``ask`` 组合用户自定义决策。

    场景：按标签选择目标节点。``ask`` 型 capability 的 handler 返回
    ``Optional[str]``：
    - 返回非 None：此 handler 决定结果
    - 返回 None：弃权，交给链中下一个 handler

    注意：这是**用户自定义 capability**，引擎不会自动把它用于任务放置。
    真正的放置由 Rust ``RoutePolicy``、节点标签与配额完成。
    """
    print("=== 阶段 2：自定义 ask capability ===")

    @task
    def train_model(dataset: str) -> str:
        return f"trained on {dataset}"

    with Runtime() as rt:
        # 注册自定义路由决策：带 "gpu" tag 的任务选择 "gpu-node"
        def gpu_router(ctx: RouteCtx) -> str | None:
            if "gpu" in getattr(ctx, "tags", []):
                return "gpu-node"
            return None  # 弃权

        rt.layer("MyRouter", "ask").chain(gpu_router)

        # 查询自定义决策（不实际提交任务）
        route = ask(
            "MyRouter",
            RouteCtx(task_name="train", peers=["node-a", "node-b"], tags=["gpu"]),
        )
        print(f"  routing(gpu task) -> {route}")

        # 无 gpu tag 时 handler 弃权，无后续 handler 则返回 None
        route = ask(
            "MyRouter",
            RouteCtx(task_name="etl", peers=["node-a", "node-b"]),
        )
        print(f"  routing(normal)   -> {route}")

    print()



def stage_3_task_retries() -> None:
    """任务重试由 Rust Worker 执行，不是 Python ``ask`` capability。

    ``@task(retries=N)`` 的重试决策在 Rust 侧完成；Python 层的自定义
    ``ask`` capability 不会被引擎自动调用。需要自定义重试语义时，应在
    任务内部实现幂等与错误分类，或把重试纳入业务流程。
    """
    print("=== 阶段 3：任务重试由引擎执行 ===")

    @task(retries=3)
    def flaky_task() -> str:
        raise RuntimeError("transient failure")

    with Runtime():
        handle = flaky_task.submit()
        # flaky_task 总是抛 RuntimeError，Rust 侧重试 3 次后最终失败
        try:
            handle.result(timeout=10.0)
        except RuntimeError:
            print("  flaky_task 最终失败（Rust 侧重试 3 次后耗尽）")

    print()


def stage_4_custom_emit() -> None:
    """``emit`` 是反应型 effect：所有 handler 按顺序被调用，无返回值。

    适合审计日志、指标采集、事件通知等"副作用"场景。用户可声明全新的
    capability 名（无需 Rust 侧注册），直接在 Python 层使用。

    与 ``ask``（逆序、首个非 None 决定）和 ``perform``（最后注册的执行）
    不同，``emit`` 调用所有 handler，适合多播。
    """
    print("=== 阶段 4：自定义 emit capability ===")

    with Runtime() as rt:
        audit_log: list[str] = []
        metrics: dict[str, int] = {}

        # 第一个 handler：写审计日志
        rt.layer("UserEvent", "emit").chain(
            lambda e: audit_log.append(f"{e['action']} by {e['user']}")
        )
        # 第二个 handler：累计指标
        def record_metric(e: dict) -> None:
            action = e["action"]
            metrics[action] = metrics.get(action, 0) + 1

        rt.layer("UserEvent", "emit").chain(record_metric)

        # 触发事件——两个 handler 都会被调用
        emit("UserEvent", {"action": "login", "user": "alice"})
        emit("UserEvent", {"action": "login", "user": "bob"})
        emit("UserEvent", {"action": "logout", "user": "alice"})

        print("  审计日志:")
        for line in audit_log:
            print(f"    {line}")
        print(f"  指标: {metrics}")

    print()


def main() -> None:
    print("Actant 渐进式教程：从 @task 到 ERH\n")

    stage_1_task_api()
    stage_2_custom_ask()
    stage_3_task_retries()
    stage_4_custom_emit()

    print("=== 教程完成 ===")
    print()
    print("关键要点:")
    print("  1. @task 适合简单场景，零配置即用")
    print("  2. ask 型 capability（自定义名称）用逆序决策")
    print("  3. perform 型 capability 用最后注册的 handler 执行副作用")
    print("  4. emit 型 capability 按顺序调用所有 handler，适合多播")
    print("  5. 自定义 capability 无需 Rust 侧注册，Python 层直接可用")
    print()
    print("下一步:")
    print("  - 阅读 examples/quickstart.py 了解完整 ERH 原语")
    print("  - 阅读 examples/custom_capability.py 了解自定义 capability 进阶")
    print("  - 阅读 examples/github_analyzer/ 了解真实业务流水线")


if __name__ == "__main__":
    main()

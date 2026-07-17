"""从 @task 到 ERH：渐进式深入 Actant 扩展模型。

本教程按 4 个阶段从高层 API 逐步下探到底层 ERH 原语，帮助用户理解
何时需要从 ``@task`` 切换到 ``ask``/``perform``/``emit``，以及如何组合
自定义 capability handler 实现业务策略。

运行::

    uv run python examples/task_to_erh.py

## 阶段概览

1. **阶段 1：@task 高层 API**——零配置提交任务，适合 80% 的常见场景。
2. **阶段 2：自定义 Routing handler**——用 ``ask`` 覆盖路由策略，
   把特定任务路由到指定节点。
3. **阶段 3：自定义 RetryPolicy handler**——用 ``ask`` 实现指数退避重试。
4. **阶段 4：自定义 emit capability**——用 ``emit`` 多播审计事件，
   演示如何声明全新 capability 并注册 handler 链。
"""

from __future__ import annotations

import actant
from actant import Runtime, ask, emit
from actant.capabilities import RetryCtx, RouteCtx
from actant.task import task


# ---------------------------------------------------------------------------
# 阶段 1：@task 高层 API
# ---------------------------------------------------------------------------


def stage_1_task_api() -> None:
    """``@task`` 是最简 API：装饰一个函数即可提交到 Runtime 执行。

    ``Runtime.with_defaults()`` 预装了默认 handler（本地路由 / FIFO 调度 /
    无重试），用户无需理解 ERH 即可开始。此阶段适合：
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

    with Runtime.with_defaults() as rt:
        # submit() 异步提交，返回 AsyncResult 句柄
        handle = fetch_url.submit("https://example.com")
        # result() 阻塞等待结果
        status = handle.result(timeout=5.0)
        print(f"  fetch_url -> {status}")

        # 也可以直接同步调用（不走 Runtime，立即执行）
        status_sync = fetch_url("https://sync.example.com")
        print(f"  sync call -> {status_sync}")

    print()


# ---------------------------------------------------------------------------
# 阶段 2：自定义 Routing handler
# ---------------------------------------------------------------------------


def stage_2_custom_routing() -> None:
    """当默认本地路由不够用时，用 ``ask`` 覆盖路由策略。

    场景：GPU 任务必须路由到带 GPU 的节点。``Routing`` capability 是
    ``ask`` 型（决策型），handler 返回 ``Optional[str]``：
    - 返回非 None：此 handler 决定路由目标
    - 返回 None：弃权，交给链中下一个 handler 决策

    后注册的 handler 优先级更高（逆序调用），因此自定义 handler 在
    默认 LocalRouter 之前被询问。
    """
    print("=== 阶段 2：自定义 Routing handler ===")

    @task
    def train_model(dataset: str) -> str:
        return f"trained on {dataset}"

    with Runtime.with_defaults() as rt:
        # 注册自定义路由：带 "gpu" tag 的任务路由到 "gpu-node"
        def gpu_router(ctx: RouteCtx) -> str | None:
            # RouteCtx 有 tags 属性（通过 kwargs 传入）
            if "gpu" in getattr(ctx, "tags", []):
                return "gpu-node"
            return None  # 弃权，回退到默认路由

        rt.layer("Routing").chain(gpu_router)

        # 查询路由决策（不实际提交任务）
        route = ask(
            "Routing",
            RouteCtx(task_name="train", peers=["node-a", "node-b"], tags=["gpu"]),
        )
        print(f"  routing(gpu task) -> {route}")

        # 无 gpu tag 时回退到默认本地路由
        route = ask(
            "Routing",
            RouteCtx(task_name="etl", peers=["node-a", "node-b"]),
        )
        print(f"  routing(normal)   -> {route}")

    print()


# ---------------------------------------------------------------------------
# 阶段 3：自定义 RetryPolicy handler
# ---------------------------------------------------------------------------


def stage_3_custom_retry() -> None:
    """``RetryPolicy`` 是 ``ask`` 型 capability，决定任务失败后是否重试。

    默认 handler（DefaultRetryPolicy）不重试。自定义 handler 可实现指数退避、
    基于错误类型的差异化重试等策略。

    ``RetryCtx`` 字段：
    - ``task_id``：任务 ID
    - ``attempt``：当前尝试次数（从 0 开始）
    - ``error``：上次失败的错误信息
    - ``max_retries``：``@task(retries=N)`` 设置的最大重试次数

    注意：cloudpickle 序列化任务到 worker 线程后，闭包变量不跨线程共享，
    因此本示例不在 task 内读取外部计数器，仅通过日志观察重试行为。
    """
    print("=== 阶段 3：自定义 RetryPolicy handler ===")

    @task(retries=3)
    def flaky_task() -> str:
        raise RuntimeError("transient failure")

    with Runtime.with_defaults() as rt:
        # 自定义重试策略：网络错误指数退避，其他错误不重试
        def smart_retry(ctx: RetryCtx) -> bool | None:
            if "transient" in ctx.error:
                # 临时错误：重试，但不超过 max_retries
                if ctx.attempt < ctx.max_retries:
                    print(
                        f"  [retry] attempt={ctx.attempt}, "
                        f"will retry (transient error)"
                    )
                    return True
                print(f"  [retry] attempt={ctx.attempt}, max reached, give up")
                return None  # 弃权，让默认 handler 决定
            # 非临时错误：不重试
            print(f"  [retry] attempt={ctx.attempt}, non-transient, no retry")
            return False

        rt.layer("RetryPolicy").chain(smart_retry)

        handle = flaky_task.submit()
        # flaky_task 总是抛 RuntimeError，重试 3 次后最终失败
        try:
            handle.result(timeout=10.0)
        except RuntimeError:
            print("  flaky_task 最终失败（重试 3 次后耗尽）")

    print()


# ---------------------------------------------------------------------------
# 阶段 4：自定义 emit capability
# ---------------------------------------------------------------------------


def stage_4_custom_emit() -> None:
    """``emit`` 是反应型 effect：所有 handler 按顺序被调用，无返回值。

    适合审计日志、指标采集、事件通知等"副作用"场景。用户可声明全新的
    capability 名（无需 Rust 侧注册），直接在 Python 层使用。

    与 ``ask``（逆序、首个非 None 决定）和 ``perform``（最后注册的执行）
    不同，``emit`` 调用所有 handler，适合多播。
    """
    print("=== 阶段 4：自定义 emit capability ===")

    with Runtime.with_defaults() as rt:
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


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------


def main() -> None:
    print("Actant 渐进式教程：从 @task 到 ERH\n")

    stage_1_task_api()
    stage_2_custom_routing()
    stage_3_custom_retry()
    stage_4_custom_emit()

    print("=== 教程完成 ===")
    print()
    print("关键要点:")
    print("  1. @task 适合简单场景，零配置即用")
    print("  2. ask 型 capability（Routing/RetryPolicy/Scheduling）用逆序决策")
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

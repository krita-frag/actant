# 调度器

## 契约

`Scheduler` 定义 Worker 与调度策略之间的最小契约：入队、出队、批量出队、关闭。

| 方法 | 语义 |
| --- | --- |
| `enqueue` / `enqueue_batch` | 入队；调度器已关闭时返回 `Err` |
| `dequeue` | 队列为空时可阻塞等待 |
| `try_dequeue` | 非阻塞，立即返回任务或 `None` |
| `dequeue_batch(limit)` | 按优先级返回至多 `limit` 个任务，不足时返回实际数量 |
| `drain_unrouted` | 只取未指定 `target_node` 的任务，保留已路由任务，避免“先全取出再重入队”的竞态 |
| `close` / `is_closed` | 进入 drain 模式 |

## 阻塞语义

Worker 主循环不 sleep 轮询：`SchedulerActor` 在 `enqueue` 后触发 `notify_task_enqueued()`，Worker 通过专用 `Notify` 信号被唤醒并调用 `try_dequeue()`。

## 关闭语义

`close()` 后调度器进入 drain 模式：

1. **新入队被拒**：`enqueue` / `enqueue_batch` 返回 `Err(ActantError::Internal("scheduler is closed"))`。
2. **等待中的消费者被唤醒**：阻塞在 `dequeue()` 的调用方立即收到 `None`，退出消费循环。
3. **已排队任务保留**：`close` 不清空队列，已入队任务仍可通过 `try_dequeue` / `dequeue_batch` 取出。
4. **幂等**：重复调用安全无副作用。

## 实现

生产路径为 `SchedulerActor`（持有队列状态）+ `ActorScheduler`（客户端，通过 Actor 消息转发）。`ActorScheduler::close` 是 fire-and-forget：只标记本地 `AtomicBool` 并异步通知 Actor；需要确认 Actor 完全停止时继续调用 `ActorScheduler::join`。

### enqueue 快路径

通过 `ActorScheduler::with_fast_path` 构造时，`enqueue` / `enqueue_batch` 直接调用共享的 `InnerScheduler::enqueue`，绕过 Actor 消息往返（postcard 编解码 + 邮箱调度 + 响应通道），单任务入队延迟从 ~10µs 降至 ~100ns 量级。

快路径安全性：`InnerScheduler` 的队列由 `parking_lot::Mutex` 保护，`notify` 为线程安全的 `tokio::sync::Notify`，`closed` 为 `AtomicBool`；`enqueue` 的 `notify_one()` 与 Actor 路径的 `notify_task_enqueued()` 唤醒语义一致。

`dequeue` / `try_dequeue` / `close` 仍走 Actor 消息协议——它们涉及阻塞等待与 drain 模式等 Actor 侧协调，不适合直接共享状态。

## 扩展点

`Scheduler` 是 Rust 核心的公共扩展点：外部实现只需满足 `Send + Sync`，可替换为公平调度、加权轮询、截止时间优先等策略；实现须保证 `close()` 后拒绝新任务并唤醒等待中的消费者。该点在[扩展点清单](../reference/extension-points.md)中登记为 `hot_path=true` / `resolution=build_time`：只能在构造期经 `RuntimeBuilder` 注入，运行期没有 setter、不可替换。装配入口见 [框架契约](framework-contract.md)。

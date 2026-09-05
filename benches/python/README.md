# Python 层性能基准测试

Actant Python API 性能基准测试套件，覆盖任务调度、gather 并行等待、
flow 编译执行、事件批量化、payload 大小、并发扩展性等热点路径。

## 运行方式

### 1. 独立运行（推荐：输出 Markdown 表格，无需 pytest-benchmark）

```bash
# 完整运行（约 5-10 分钟）
.venv/bin/python benches/python/run_bench.py

# 快速模式（CI 烟雾测试，约 1-2 分钟）
.venv/bin/python benches/python/run_bench.py --quick

# 过滤特定分组
.venv/bin/python benches/python/run_bench.py --filter gather

# 输出 JSON 报告供后续对比
.venv/bin/python benches/python/run_bench.py --json /tmp/bench.json
```

### 2. 通过 pytest-benchmark 运行（CI 友好，可对比基线）

```bash
.venv/bin/python -m pytest benches/python/ --benchmark-only
.venv/bin/python -m pytest benches/python/ --benchmark-only -k gather
```

## 前置条件

1. **必须先编译 Rust 扩展**：

   ```bash
   .venv/bin/maturin develop --release
   ```

2. **环境变量自动设置**：`ACTANT_DISCOVERY=none` 避免 iroh DNS 阻塞。

### 3. 进程池专项基准（独立脚本）

```bash
.venv/bin/python benches/python/bench_pool.py                         # 冷启动 + 池化收益 + 超时回收
.venv/bin/python benches/python/bench_pool.py --json /tmp/pool.json   # 输出 JSON 供入库
.venv/bin/python benches/python/bench_pool.py --workers 1 4 8 --reclaim-runs 6
```

测量三组与进程级隔离直接相关的特性（结果快照见 `results/bench_pool_result.json`）：
- `cold_start` — Runtime 启动耗时（含 worker 子进程 spawn）+ 首次任务端到端延迟（冷启动惩罚）
- `steady_state` — 热池稳态分发延迟（worker 复用，无进程创建）
- `timeout_reclaim` — 硬超时恢复周期：提交→超时暴露（含强杀+替补）、超时暴露→下一任务完成、扣除基线的纯回收开销

## 测试覆盖

| 文件 | 分组 | 测量内容 |
|------|------|----------|
| `bench_task_dispatch.py` | task_dispatch | 单任务 submit→result 端到端延迟 |
| `bench_gather.py` | gather | `actant.gather` 并行等待吞吐 |
| `bench_flow.py` | flow | 命令式 vs DAG 编译式 flow 对比 |
| `bench_events.py` | events | 事件批量化 + silent 跳过开销 |
| `bench_payload.py` | payload | 输入/输出大小对延迟的影响 |
| `bench_concurrency.py` | concurrency | 10/100/1000 并发任务扩展性 |
| `bench_pool.py` | pool | 进程池冷启动、池化收益、超时强杀回收延迟（独立脚本） |

## 输出指标

每项基准输出：

- `median` — 多次采样的中位数（毫秒）
- `op_time` — 单次操作延迟（微秒）
- `min` — 最快采样（毫秒）
- `stdev` — 采样标准差（毫秒）
- `n*r` — 单次采样内重复次数 × 采样次数

## 与同类项目对比

参考 `bench_summary.md`（运行 Rust + Python 基准后生成）。

"""Python 层 Actant 性能基准测试套件。

覆盖关键热点路径：
- ``bench_task_dispatch`` — 单任务端到端调度延迟
- ``bench_gather`` — ``actant.gather`` 并行等待吞吐（intrusively-linked futures）
- ``bench_flow_imperative`` — 命令式 ``@flow`` 执行延迟
- ``bench_events`` — 事件批量化吞吐与 ``silent`` 跳过开销
- ``bench_payload`` — 不同 payload 大小对调度延迟的影响
- ``bench_concurrency`` — 并发任务数扩展性（10/100/1000）
- ``bench_pool`` — 进程池专项：冷启动、池化收益、超时强杀回收延迟（独立脚本，不经 pytest-benchmark）

运行方式::

    # 1. 通过 pytest-benchmark（CI 友好，可对比基线）
    pytest benches/python/ --benchmark-only

    # 2. 通过独立运行入口（输出 JSON 报告 + Markdown 表格）
    .venv/bin/python benches/python/run_bench.py
    .venv/bin/python benches/python/run_bench.py --quick   # 跳过慢测试
    .venv/bin/python benches/python/run_bench.py --json /tmp/bench.json

环境要求：
- 必须先 ``maturin develop --release`` 编译 Rust 扩展
- 自动设置 ``ACTANT_DISCOVERY=none`` 避免 iroh DNS 阻塞
"""

from __future__ import annotations

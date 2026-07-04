"""Quick Start: 最小可运行的 Actant 示例。

运行方式:
    python examples/quickstart.py
"""

import actant


# 1. 定义任务(自动注册到全局表)
@actant.task
def add(x, y):
    return x + y


# 2. 定义工作流
@actant.flow
def my_workflow():
    result = add(1, 2)
    return result


# 3. 提交并获取结果(actant.submit 自动管理瞬态节点)
#    signing_key 可显式传入，或通过 ACTANT_SIGNING_KEY 环境变量提供
result = actant.submit(my_workflow, signing_key="example-key").get_sync(timeout=10.0)
print(f"add(1, 2) = {result.value}")  # 3

# 4. 查询状态
print(f"workflow state: {result.state}")  # Completed

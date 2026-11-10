# 扩展点维护

Actant 不插件化：扩展是编译期实现 trait + builder 注入，或 Python 侧登记
handler。因此「扩展点清单」是构建期数据，不是运行时注册表。

唯一事实源是 `actant/extension_points.json`。生成页、CLI 与门禁都读它，
不要在别处再维护一份扩展点表格。

## 新增或修改扩展点的顺序

1. **改清单**：在 `actant/extension_points.json` 的 `points` 里增删条目。
   必填字段的含义见[扩展点清单](../reference/extension-points.md)；新增条目还要
   填 `resolution` 与 `hot_path`，热路径点只能是 `build_time`。
2. **改生成器**：若新增了 `layer` 或 `resolution` 取值，同步登记到
   `scripts/gen_api_reference.py` 的 `EXTENSION_LAYERS` / `RESOLUTIONS`，
   否则生成器会拒绝未知取值。运行 `uv run python scripts/gen_api_reference.py`
   重新生成 `docs/reference/extension-points.md` 与 `docs/reference/cli.md`。
3. **过门禁**：`uv run pytest tests/python/unit/test_extension_contract.py -q`。
   它会双向比对清单与 `crates/actant-core/src/runtime/builder.rs` 的 trait 缝、
   `actant/capabilities.py` 的 `BUILTIN_CAPABILITIES`，检查每个引用符号，
   校验执行域一致性，并静态扫描热路径白名单文件。新增热路径文件要同步登记
   门禁里的 `_HOT_PATH_FILES`。
4. **交验收**：每个条目都要有一个真实驱动生产路径的 e2e，且测试不得直接
   调用被接线的函数。以 example 作为验收时，必须在 CI 里真正运行它。

## 六条硬约束

本节约束的是**贡献流程**：改完要交什么。[扩展点清单](../reference/extension-points.md)的
「七项登记判据」约束的是**单个条目该填什么**。两者覆盖同一批事实，只是切入点不同。

- **入口与装配位置必须真实**：`entrypoint` / `production_call_site` 写成
  `路径::符号`，装配位置不得落在 `tests/`、`examples/`、`docs/` 里。
  `production_call_site` 记录绑定发生的函数，不表示运行期调用频率；调用频率
  由 `hot_path` 单独标注。只有登记了、却查不到引擎侧消费点的扩展点，正是
  历史上留下空壳 capability 的原因。
- **跨节点安全性按层固定**：`rust` 层恒为 `true`，`bridge` / `python` 层恒为
  `false`。进程内闭包在 workflow failover 后不随工作流迁移，所以任何把
  Python handler 标成跨节点安全的改动都会被门禁拦下。
- **验收必须能被 CI 执行**：`acceptance_test` 指向 `tests/` 或 `examples/`
  下的真实符号；指向 `examples/` 时，CI 必须包含对应的 `--example` 步骤。
- **验收强度必须诚实**：`acceptance_level` 取 `behavior` / `wiring` / `unit`。
  只有真实驱动生产路径、且断言注入实现影响了引擎行为才算 `behavior`；
  仅有装配期断言标 `wiring`。把弱验收标成 `behavior` 与留一个空壳等价。
- **未覆盖不等于可用**：尚缺外部契约的方向放进 `uncovered_intents`，
  它们不进 `actant extensions` 的可用列表，也不参与符号校验。
- **执行域一致**：`hot_path=true` 的点必须是 `build_time`，rust 层恒为
  `build_time`，python 层恒为 `runtime` 且非热路径。热路径白名单文件不得出现
  capability 三动词调用或 GIL 附加；只禁调用形态，不禁类型名与网络帧序列化
  （`context.rs` 合法持有 `CapabilityRuntime`，热路径本来就用 postcard 编码帧）。

## 反证门禁

改完清单后，用下面六种临时篡改确认门禁确实会变红（改完记得恢复）：

1. 把某个 `entrypoint` 的符号改成不存在的名字；
2. 删掉一条真缝的登记（例如 `transport`）；
3. 把 `acceptance_test` 指向不存在的文件或符号；
4. 把某个 `hot_path=true` 条目的 `resolution` 改成 `runtime`；
5. 在 `crates/actant-core/src/runtime/builder.rs` 临时加一个签名含
   `Arc<dyn ...>` 的 `pub fn with_*`，集合相等断言同样应该变红；
6. 反证热路径静态扫描时**不要改 Rust 生产文件**（原生扩展新鲜度检查会拦下）：
   用 `python -c` 导入 `test_extension_contract`，把 `_HOT_PATH_FILES` 临时
   指向一个含 `self.capability.perform::<Store>(req)` 的临时文件。

六种情况都变红，才能说明「完整」是 CI 可检查的状态，而不是文档里的形容词。

## 设计边界

- **统一的是契约，不是分发路径**：全部条目共用清单、capability / handler / effect
  词汇、生成页、CLI、门禁与验收分级；但 `build_time` 的 trait 句柄与 `runtime` 的
  handler 链是两条调用路径。给热路径缝套 capability 适配器，会把注册表查表、
  编解码与 Actor 邮箱往返带进每任务路径。
- **可见但不可注入**：`build_time` 点在清单、生成页与 `actant extensions` 里都查得到
  （「必须得有」），但没有 PyO3 setter，Python 无法在运行期替换（「不必暴露」）。
  查得到不等于装得上。
- **冻结后不可热更新**：构造期定稿后不再解析实现，换实现必须重建 Runtime。
- **性能边界用静态结构守，不用数值阈值**：热路径点必须 `build_time`，热路径
  白名单文件禁用动态分派调用与 GIL 附加。`.github/workflows/bench.yml` 只产出
  criterion artifact 供人工跨 run 对比，不做阈值断言；共享 runner 的噪声大于
  真实回归幅度，不能当门禁。
- **静态扫描只禁调用形态**：禁止 `CapabilityRuntime` 三动词调用与
  `Python::attach` / `with_gil`，不禁类型名与 postcard 序列化——`context.rs`
  合法持有 `CapabilityRuntime`，热路径本来就用 postcard 编码网络帧。
- **`production_call_site` 是装配位置**：多数条目填 `RuntimeBuilder::build` /
  `init_*`，不是运行期调用点；调用频率由 `hot_path` 标注，静态扫描目标由门禁里的
  `_HOT_PATH_FILES` 白名单给出。

## CLI 的边界

`actant extensions` 只列**可用扩展点**，并标出 `resolution` / `hot_path`：
rust 层条目能被查到，但没有 PyO3 setter，Python 无法在运行期替换它们。
Python 也无从得知 Rust 编译期注入了哪个 trait 实现，因此不要把它扩展成
「当前实例已注入实现」的查询接口——那会是又一个无法证伪的声明。

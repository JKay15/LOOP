# LOOP 系统第一版实现交接文档 v0.53

## 0. 目的

本文件不是再扩展最终效果，而是把 v0.52 已经沉淀出的制度骨架，翻译成**可直接交给 Codex 开始实现的第一版交接包**。

本文件回答三件事：
1. 第一版 repo 至少长什么样；
2. 哪些接口 contract 必须先立住；
3. root `AGENTS.md` 与 `.agents/skills/` 的最小骨架应当承担什么职责。

## SIMPLE-FIRST HARD REQUIREMENT

本文件后续所有功能、最终效果、接口面与实现要求都继承以下硬约束：
- Occam-style simplification is mandatory when a simpler design can run truthfully end-to-end.
- Regularization pressure must favor fewer entities, not more mirrored structure.
- Real execution beats placeholders, fake completeness, or theory-shaped scaffolding.
- Only correctness, real execution, durable audit, or a proven isolation boundary can justify a new entity.
- No new entity without necessity.

---

## 1. 现在是否可以交给 Codex 开始实现

可以，但前提不是“文档继续无限打磨”，而是先把下面三样东西落成仓库事实：

1. **repo skeleton**：顶层目录面与运行时状态面存在；
2. **最小接口 contract**：kernel / node / gateway / evaluator / runtime / topology / dispatch 的接口面存在；
3. **root AGENTS + skills 骨架**：每轮必加载的硬约束与按需工作流骨架存在。

只要这三样先有，Codex 就不再是在一片抽象制度里乱飞，而是在有边界、有锚点、有最小 contract 的仓库里开工。

---

## 2. 第一版 repo skeleton

## 2.1 顶层目录树

```text
repo-root/
├─ AGENTS.md
├─ README.md
├─ pyproject.toml                # 或等价项目入口；用于统一 Python 运行边界
├─ .agents/
│  └─ skills/
│     ├─ loop-runner/
│     │  └─ SKILL.md
│     ├─ child-dispatch/
│     │  └─ SKILL.md
│     ├─ split-review/
│     │  └─ SKILL.md
│     ├─ runtime-recovery/
│     │  └─ SKILL.md
│     ├─ evaluator-exec/
│     │  └─ SKILL.md
│     └─ repo-hygiene/
│        └─ SKILL.md
├─ docs/
│  ├─ design/
│  ├─ contracts/
│  └─ schemas/
├─ loop_product/
│  ├─ __init__.py
│  ├─ endpoint/
│  │  ├─ __init__.py
│  │  ├─ clarification.py
│  │  └─ policy.py
│  ├─ evaluator/
│  │  ├─ __init__.py
│  │  ├─ prototype.py
│  │  ├─ simple.py
│  │  └─ agent_execution_policy.py
│  ├─ kernel/
│  │  ├─ __init__.py
│  │  ├─ entry.py
│  │  ├─ state.py
│  │  ├─ submit.py
│  │  ├─ audit.py
│  │  └─ policy.py
│  ├─ gateway/
│  │  ├─ __init__.py
│  │  ├─ normalize.py
│  │  ├─ classify.py
│  │  ├─ accept.py
│  │  └─ safe_degrade.py
│  ├─ loop/
│  │  ├─ __init__.py
│  │  ├─ runner.py
│  │  ├─ roundbook.py
│  │  ├─ local_control.py
│  │  └─ evaluator_client.py
│  ├─ runtime/
│  │  ├─ __init__.py
│  │  ├─ liveness.py
│  │  ├─ recover.py
│  │  ├─ reconcile.py
│  │  └─ lifecycle.py
│  ├─ topology/
│  │  ├─ __init__.py
│  │  ├─ split_review.py
│  │  ├─ budget.py
│  │  ├─ merge.py
│  │  ├─ prune.py
│  │  └─ hoist.py
│  ├─ dispatch/
│  │  ├─ __init__.py
│  │  ├─ child_dispatch.py
│  │  ├─ heartbeat.py
│  │  └─ launch_policy.py
│  └─ protocols/
│     ├─ __init__.py
│     ├─ node.py
│     ├─ control_envelope.py
│     ├─ evaluator.py
│     └─ topology.py
├─ tests/
│  ├─ smoke/
│  ├─ kernel/
│  ├─ loop/
│  ├─ runtime/
│  ├─ topology/
│  └─ integration/
├─ scripts/
│  ├─ smoke.sh
│  ├─ check_state_tree.sh
│  └─ print_audit_tail.sh
└─ .loop/
   ├─ state/
   ├─ cache/
   ├─ audit/
   ├─ artifacts/
   └─ quarantine/
```

## 2.2 统一包根规则

第一版不要把系统实现拆成多个并列的顶层 Python 包。  
产品运行时源码统一收在一个包根下，这里用 `loop_product/` 作为统一包根：

- 前半部分放在 `loop_product.endpoint.*` 子包下；
- 后半部分放在 `loop_product.evaluator.*` 子包下；
- 新增的 `kernel / gateway / loop / runtime / topology / dispatch / protocols` 全部作为该包根下的子包存在。

这样做的目的不是“好看”，而是避免后续实现阶段重新退化成：
- import 边界四散；
- 包装入口分裂；
- child node / evaluator / host adapter 各自偷偷依赖 repo root 偶然布局。

## 2.3 系统单元必须先定义清楚

第一版 repo skeleton 里，至少要把下面四个系统单元视为一级概念，而不是实现细节：

- `kernel`：对话中的根监督者，负责权威图、结构提交、全局调度与审计；
- `node`：被 kernel 管理的一级执行单元，拥有自己的目标切片、代际、预算、局部控制与终态；
- `evaluator`：不是仓外神谕，而是节点图中的一个可追踪参与者，可以挂自己的 checker / ordinary-test / AI-as-User / reviewer lane；
- evaluator 里的这些 AI role 都应有固定的 runtime-owned role 目录：`run_root/.loop/<role>/`，并从各自专属工作目录启动；选择规则靠 `cwd` / `-C`，不是靠重写 `CODEX_HOME`；
- `host adapter`：宿主侧对接层，只负责把宿主环境接到该 repo 的产品面，不应反过来成为产品本体。

## 2.4 目录职责

### `docs/design/`
承载未冻结但可开工的设计交接、phase brief、迁移说明；它不是永久 contract 真源。

### `docs/contracts/`
承载冻结后的接口面 contract；当 handoff 文档中的设计收敛后，才应下沉到这里。

### `docs/schemas/`
承载结构化控制对象、节点状态、evaluator 返回等 schema。

### `loop_product/kernel/`
承载 kernel 的角色入口、权威状态、结构提交、全局策略与审计视图。

### `loop_product/gateway/`
承载所有 AI 生成结构化控制对象的接纳链：规范化、分类、接受/拒绝、安全退化。

### `loop_product/loop/`
承载单节点闭环：实现、送测、局部控制、轮次账本。

### `loop_product/runtime/`
承载存活性检测、resume/retry/relaunch、一致性与生命周期治理。

### `loop_product/topology/`
承载 split review、图复杂度预算、merge/prune/hoist 等拓扑治理。

### `loop_product/dispatch/`
承载 kernel 到 child node 的桥接：冻结 delegation、child-local 启动、heartbeat、回报。

### `loop_product/protocols/`
承载节点协议、控制 envelope、evaluator 协议、拓扑协议等最小结构真源。

### `.loop/`
只放运行时状态，不放源码；任何 durable state 都不得绕过这里偷偷落进源码树。

---

## 3. 第一版必须先立住的接口 contract

这里不是函数签名，而是**接口面 contract**。第一版只要把这些面立住，内部实现可以先粗。

## IF-1 kernel 交互接口面

至少要支持：
- 读取当前根任务与活跃节点图；
- 提交结构变化（split/merge/resume/reap）；
- 查询节点状态与预算；
- 读取最近关键审计事件；
- 接收新的用户要求并路由。

**禁止**：
- 让局部节点绕过该接口直接改图；
- 让 evaluator 或普通脚本直接写 `.loop/state/` 成为系统事实。

## IF-2 node 运行与 child-dispatch 接口面

至少要支持：
- 为每个节点定义统一的 node 协议，至少包括：
  - `node_id`
  - `parent_node_id`
  - `generation`
  - `round_id`
  - `node_kind`
  - `goal_slice`
  - `execution_policy`
  - `reasoning_profile`
  - `budget_profile`
  - `allowed_actions`
  - `delegation_ref`
  - `result_sink_ref`
  - `lineage_ref`
- kernel 先冻结 delegation / execution policy，再由 child-dispatch 桥接去 materialize child；
- child-local 运行时只消费冻结后的 delegation 与 node-local context，不直接消费原始父对话状态；
- child 节点可以回报 heartbeat、局部控制裁决、终态结果，但这些都只是上报，不是直接改图；
- root kernel 可以监督 child 节点，但不应在 child 已 materialize 后继续逐步代替 child 做实现/测试/review。

**禁止**：
- child 节点 materialize 之后，根对话 Codex 继续把 child 内部工作当成自己亲手顺做的普通步骤；
- 节点绕过 kernel / gateway 直接宣告 split、merge、resume 已生效；
- 把 `thinking budget`、provider 选择、sandbox 等执行策略只留在口头说明里，不进入 node protocol。

## IF-3 evaluator-as-node 交互接口面

至少要支持：
- LOOP 节点向 evaluator 节点提交：实现包 + 使用说明书 + 当前目标切片；
- evaluator 侧可以 materialize checker / ordinary-test / AI-as-User / reviewer 作为 node-local lane 或受控子节点；
- evaluator 返回：通过 / 未通过 / 结构化异常 / `BUG` / `STUCK` / 其他 fail-closed 终态；
- 返回必须带轮次、节点代际、来源 lane、证据链信息；
- 返回必须可进入 gateway 接纳链；
- AI-as-User 在 evaluator 内可以通过 `codex exec` 等执行位点真实使用产品，但其执行也必须挂在 evaluator node 的 lineage 下。

**禁止**：
- 把 evaluator 当成图外 oracle；
- 让 evaluator 结果直接跳过 gateway 成为控制依据；
- 让 evaluator 只负责“设计测试”，把高变执行面重新外包给脆弱固定脚本；
- evaluator / AI-as-User 在未先排查自因环境、路径、依赖、命令错误前，就把失败直接归因到实现侧。

## IF-4 结构化控制对象接口面

至少要有统一 envelope，覆盖：
- split request
- merge request
- resume / retry / relaunch request
- 局部控制裁决
- node terminal result
- child heartbeat / dispatch status
- evaluator 结构化返回

共同要求：
- 有来源；
- 有轮次/代际归属；
- 有载荷类型；
- 可以被 gateway 规范化；
- 可以被 kernel 接受/拒绝；
- 接受前不得直接生效；
- 至少能表达“这是提案 / 这是上报 / 这是已被 kernel 接受的事实”三种状态。

## IF-5 权威状态查询接口面

至少要能查询：
- 当前权威节点图；
- 每个节点的生命周期状态；
- 当前有效要求集；
- 当前预算/复杂度视图；
- 当前挂起/blocked 原因；
- 当前 delegation map；
- 当前有哪些 evaluator lane / child node 仍在活跃。

**要求**：只读查询必须简单清楚，不能逼 Codex 先读一堆内部实现细节。

## IF-6 审计与经验接口面

至少要支持：
- 回放关键结构性决策；
- 查询某节点最近关键事件；
- 查询当前生效文档/规则来源；
- 记录并读取被接受的经验资产；
- 区分经验资产、缓存、durable state、硬规则；
- 记录 evaluator / AI-as-User / child node 的自归因与自修复结论，避免已知错误再次被误判为实现问题。

---

## 4. root `AGENTS.md` 的最小骨架

root `AGENTS.md` 不该什么都写，但必须写**每轮都必须生效**的硬约束。

最小骨架应至少覆盖：

### A. 角色定位
- 与用户对话的 Codex agent 在完整系统里默认充当 **kernel**，不是普通 executor；
- 普通实现由 LOOP 节点承担；
- 一旦 child 节点已经 materialize，root kernel 默认负责监督、裁决、追踪与 debug 收敛，不再顺手接管 child 内部执行；
- 除非明确处于 prototype fallback 模式，不得偷偷退化成“我自己顺手把代码实现了”。

### B. 持久运行纪律
- 当前任务未完成时，不得中途退出；
- 必须等待必要 tool 返回；
- 用户中途 follow-up 默认视为当前任务的增量要求，而不是允许退出的信号；
- 若因 follow-up 打断，应优先继续同一线程/同一逻辑节点，而不是重开空线程。

### C. 结构事实纪律
- 任何 split / merge / resume / reap 等结构变化，在 kernel 正式提交前都只是提案；
- 不得私自改图；
- 不得绕过 gateway 直接采用 AI 结构化输出；
- child node、evaluator、AI-as-User 产出的结构化对象也不例外。

### D. 文档与路径纪律
- repo root 的硬约束优先存在于 root `AGENTS.md`；
- 角色进入更窄工作目录时，必须继续满足必要硬约束；
- 不得假定 skill 足以替代 root `AGENTS.md` 的每轮必加载纪律。

### E. 放权与归因纪律
- 能放权给 child node / evaluator AI 的地方，不要再用固定脚本替它做高变判断；
- evaluator 与 AI-as-User 必须先检查自因问题，再决定是否归因到实现；
- 全局 resume、全局调度、长期恢复属于 kernel，不属于普通节点或 repo 内局部 runtime；
- 后续高认知子节点可以使用更高 `thinking budget`，但该策略必须挂在 `execution_policy / reasoning_profile` 上，而不是散落在宿主提示词里。

### F. repo hygiene 底线
- 运行时状态写入 `.loop/`；
- 不得把临时运行状态混进源码目录；
- git 自动动作必须非破坏、可逆、可审计；
- 不得把 `.venv`、绝对路径、源 workspace 假设写进可复用 contract。

---

## 5. `.agents/skills/` 的第一版最小骨架

skills 不是宪法，它们是**按需工作流包**。第一版只需要 6 个最小 skill。

## 5.1 `loop-runner`
职责：指导单节点 LOOP 如何完成一轮：
- 接收目标切片；
- 产出实现；
- 调 evaluator；
- 处理返回；
- 形成局部控制裁决。

不负责：
- 改系统图；
- 直接决定 split 生效。

## 5.2 `child-dispatch`
职责：
- 把 kernel 冻结后的 delegation / execution policy materialize 为 child-local 执行上下文；
- 启动 implementer / reviewer / evaluator 等 child lane；
- 回收 heartbeat、局部结果、终态 envelope；
- 保证 root kernel 监督 child，而不是直接扮演 child。

不负责：
- 直接让 split 生效；
- 直接修改权威图。

## 5.3 `split-review`
职责：
- 识别是否进入 split review；
- 组织 split request；
- 检查 S1–S5 的 split 骨架；
- 不负责直接改图，只负责形成审查提案。

## 5.4 `runtime-recovery`
职责：
- 检测卡死/异常；
- 组织 resume / retry / relaunch 提案；
- 对恢复一致性做检查。

## 5.5 `evaluator-exec`
职责：
- 帮 evaluator 侧 AI 掌控高变执行面；
- 处理 uv / 路径 / 仓库结构 / 命令序；
- 区分测评侧自因与实现侧问题。

这条特别关键：
**不要再把“设计测试”和“实际高变执行”拆成 AI + 固定脚本这种脆弱组合。**
脚本只做低方差辅助动作，AI 才主导环境敏感、路径敏感、依赖敏感的执行位点。

## 5.6 `repo-hygiene`
职责：
- 产物放置与分层；
- quarantine/cleanup 的安全边界；
- 非破坏性 git 操作；
- 经验资产与 cache/durable state 的区分。

---

## 6. 第一版交给 Codex 的最小任务包

现在可以把下面这些直接交给 Codex：

1. **按第 2 节生成以 `loop_product/` 为统一包根的 repo skeleton**；
2. **按第 3 节生成 `IF-1` 到 `IF-6` 的占位 contract**；
3. **按第 4 节写 root `AGENTS.md` 初版**；
4. **按第 5 节创建六个最小 skills 的 `SKILL.md` 占位骨架**；
5. **提供一个 smoke path**：
   - 启动 kernel；
   - 创建一个根节点；
   - 通过 child-dispatch 启动一个 implementer 子节点；
   - evaluator 节点返回一次未通过；
   - 子节点继续一轮；
   - 最后完成或进入明确故障。

---

## 7. 现在还不该让 Codex 过早做的事

第一版不要一上来就让 Codex：

- 细化 15 个最小模块为很多包；
- 先做复杂 split 策略；
- 先做高阶经验抽象系统；
- 先把 runtime 设计成通用分布式平台；
- 先依赖宿主项目里的既有约束来跑通；
- 先把 child-dispatch 临时做成“root 对话 agent 继续亲手跑所有子任务”；
- 先把 evaluator 当成图外服务而不是图内节点；
- 先把旧 LeanAtlas LOOP 约束继续混进新 repo 才能勉强运行。

第一版的目标是：
**把制度骨架落成可运行仓库，而不是把所有聪明想法一次写满。**

---

## 8. 交接完成后的判断

当且仅当以下 5 条同时成立时，可以认为“已经可以正式交给 Codex 开工”：

1. root `AGENTS.md` 已存在，并承载每轮必加载的硬约束；
2. `.agents/skills/` 已存在，并有最小 workflow 骨架；
3. repo 顶层目录面与 `.loop/` 状态面已存在；
4. `IF-1` 到 `IF-6` 的接口 contract 至少已有占位定义；
5. smoke path 已经证明 `kernel -> child-dispatch -> evaluator -> 下一轮或终态` 这条主路径是连通的。

到这一步，后续就不是继续磨“系统原则”，而是开始进入：
- 让 Codex 先搭 repo；
- 再让 Codex 按 phase 逐步填实现。

---

## 9. 新 LOOP 与旧 LeanAtlas LOOP 的迁移策略

这件事不能靠“新旧并存很久”来完成。迁移必须有明确 cutover。

## 9.1 真源切换原则

- 新完整 LOOP 系统 repo 一旦建立，新的 kernel / node / dispatch / evaluator / topology contract 都应先在该 repo 内沉淀；
- 旧 LeanAtlas LOOP 文档、技能、运行时只保留为过渡参考，不再继续成为新系统的长期真源；
- 不允许长期存在“两套都算真的”状态。

## 9.2 LeanAtlas 的宿主侧 cutover

LeanAtlas 后续需要做的不是“继续承载产品本体”，而是：
- 在 root `AGENTS.md` 里明确对话 Codex 的角色是 kernel；
- 把宿主侧需要的 host adapter 规则接到新 LOOP 产品面；
- 移除旧 LOOP 系统中会误导新运行的路由、规则和宿主约束。

## 9.3 旧系统的清理条件

只有当下面三条成立时，旧 LOOP 系统内容才应被系统性删除或归档：

- 新 repo skeleton + `IF-1..IF-6` contract 已存在；
- 新系统 smoke path 已连通；
- LeanAtlas 已完成新的 kernel 角色接线，不再依赖旧 LOOP 默认路径。

在这之前，旧内容只能作为只读参考，不应继续增长。

## 9.4 迁移期间的禁止项

- 不要让旧 LOOP 和新 LOOP 同时接管同一条真实控制路径；
- 不要让旧 `AGENTS.md` 纪律与新 kernel 纪律互相打架；
- 不要把旧系统的 state tree 和新 `.loop/` state tree 混成一个事实面。

---

## 10. 前后半部分已经验证出的经验，必须进入后续所有实现

下面这些不是“建议”，而是后续实现必须继承的已知约束：

1. **能放权给 AI 的地方，不要让代码替 AI 做高变判断。**
   固定脚本只做低方差辅助动作，环境敏感、路径敏感、依赖敏感的执行位点必须由 AI 主导。

2. **所有实现必须面向通用产品面，而不是当前 repo 特供。**
   不能重新写回 LeanAtlas 假设、绝对路径、源 workspace `.venv` 假设、宿主特判。

3. **AI 必须具备自归因与自修复能力。**
   evaluator、AI-as-User、普通子节点都不应把自己的环境错误、命令错误、路径错误直接甩给实现。

4. **resume、全局调度、长期恢复属于 kernel。**
   子节点和局部 runtime 可以提案、上报、心跳，但不能私自接管全局恢复权。

5. **一旦 child implementer / evaluator node 已 materialize，就应真正放权。**
   root kernel 应监督和 debug 收敛，而不是重新塌回单 agent 亲手实现一切。

6. **高 thinking budget 是执行策略，不是架构例外。**
   后续如果 implementer 节点需要更高 thinking budget，应通过 `execution_policy / reasoning_profile` 落入节点协议，而不是靠宿主侧临时口头补丁。

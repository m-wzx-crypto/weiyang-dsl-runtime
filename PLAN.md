# Weiyang DSL Runtime — Weekly Implementation Plan

> [ROADMAP.md](ROADMAP.md) 的执行层拆解。ROADMAP 定义方向与门禁;本文件把它们
> 排进周历。两者冲突时,以 ROADMAP 为准;门禁与本文件冲突时,以测试套件为准。

- **周期**:2026-09-14 → 2026-12-20,共 14 周(≈ 一个季度)。
- **节奏假设**:每周 4–5 个专注日(1 名主力 + AI 结对)。若人力不同,按里程碑整体
  顺延,不砍每周边界。
- **铁律**(继承自 ROADMAP 原则 6 与 §6):
  1. **没有纯打地基的周**——每周五必须有一段可运行、可演示的值(哪怕很小)。
  2. 每个里程碑的状态只有两种:**open** 或 **gate 绿**。不存在"接近完成"。
  3. 每周以 `make ci` 全绿收尾;门禁测试先写(红),再实现(绿)。

---

## 1. 总览

| 周 | 日期 (2026) | 里程碑 | 本周切片 | 周五交付物 |
| --- | --- | --- | --- | --- |
| W1 | 09-14 ~ 09-20 | M1 Principals | 人类归属打通端到端 | ✅ 审批决策可归因(人) |
| W2 | 09-21 ~ 09-27 | M1 | 推理归属 + 强制 principal | ✅ **M1 gate 绿** |
| W3 | 09-28 ~ 10-04 | M1 收尾(缓冲) | 等待/超时/并行分支归属补全 | v2.1 发布 |
| W4 | 10-05 ~ 10-11 | M2 Attributed Rules | 版本注册表 + 结构化 diff | 可 diff 的不可变版本史 |
| W5 | 10-12 ~ 10-18 | M2 | 激活状态机(草稿→生效) | **M2 gate 绿** |
| W6 | 10-19 ~ 10-25 | M3 Evidence | 任意点重放 + 运行对比 v1 | compare API |
| W7 | 10-26 ~ 11-01 | M3 | 证据包(hash 链) | **M3 gate 绿** |
| W8 | 11-02 ~ 11-08 | 缓冲 / 复盘 | 见 §3 缓冲周策略 | 重新估 M4–M6 |
| W9 | 11-09 ~ 11-15 | M4 Bounded Capability | 工具白名单 + 参数 schema | **gate (a) 绿** |
| W10 | 11-16 ~ 11-22 | M4 | 效果预算 + 轨迹违规 | **M4 gate 绿** |
| W11 | 11-23 ~ 11-29 | M5 Provable Limits | 自治级别声明 + 执行期强制 | 级别变更入账 |
| W12 | 11-30 ~ 12-06 | M5 | 部署期静态限额证明 | **M5 gate 绿** |
| W13 | 12-07 ~ 12-13 | M6 Intent Units | 意图单元模型 + 子流程分解 | intent unit 可运行 |
| W14 | 12-14 ~ 12-20 | M6 | 验收标准可验证性检查 | **M6 gate 绿** + 季度收尾 |

节假日注:W2 周五(09-25 中秋)与 W3(含国庆)刻意排轻;具体调休以官方安排为准,
顺延工作量进入 W8 缓冲,不挤占后续周边界。

依赖主线:W1–W2 的 journal 形状变更是全季最大的破坏面,所以 M1 排最前且给足
缓冲;M3 的证据包依赖 M2 的版本史;M5 的静态证明依赖 M4 的预算声明。

---

## 2. 每周明细

### W1 (09-14) — M1a:人类归属端到端

**目标**:一次人类审批的"是谁批的"可从日志回答。

- `dsl`:定义 `Principal`(kind: human / agent / system / model + 稳定 ID + 显示名);
  `Event` 携带 principal;`OccEventConsumed` 记录归属(向后兼容:字段可空)。
- `dsl`:fold 路径恢复归属字段;`fold_property_test.go` 的 `assertResumeEq`
  自然覆盖(它逐字段比 fold 与活上下文,归属字段自动纳入)。
- 测试:人类 principal 随事件入账、穿越 fold 逐字段一致(性质测试跑 40 种子)。
- **周五交付**:审批类决策在日志与折叠状态中均带"批准人"。

> **完成记录(09-13,提前于计划)**:交付如上,全部门禁绿。实现落点:
> `principal.go`(新)、`Event.Principal`、`Occurrence.Actor`、`AcceptEvent` 副本
> 入账(调用方事后改对象不能改写已发生的账)、fold 经 Event 快照零改动复现;
> `principal_test.go` 覆盖入账/折叠/时间旅行/JSON 回传/防篡改/向后兼容;
> `compareCtx` 与 40 种子性质测试全程比较 principal。实现中发现并修正一处
> 测试基建缺陷:随机驱动从"均匀抽事件"改为**洗牌驱动**(每轮按随机顺序全量
> 投递事件池)——均匀抽法在并行段串联的拓扑下活性无界(实测 seed-30 停滞),
> 洗牌法保证每轮必推进、终止性有界。W2 接手时注意:`PrincipalKind=PrincipalModel`
> 已预留,模型/版本/prompt 元数据字段未加,归属强制点未做。

### W2 (09-21) — M1b:推理归属 + 强制 principal → **M1 gate 绿**

**目标**:每个 AI 决策可回答"哪个模型、哪个 prompt 版本";缺 principal 的决策
记录被结构性拒绝。

- `dsl`:ai 回调路径(`stepProcessAI`)记录 model + model version + prompt version
  (由宿主作为**数据**传入,内核不做任何模型调用——原则 3)。
- `dsl`:强制点——要求 principal 的 occurrence 在无 principal 时记录失败
  (WAL 语义:致命、可见,不静默)。
- `store-postgres`:归属字段随 occurrence JSON 全量持久化;补 round-trip 测试与
  按 principal 查询的索引。
- gate(ROADMAP M1):无 principal 的决策被拒绝 ✓;fold 后 principal 逐字段
  复现 ✓(新增专门 gate 测试 + 性质测试扩展)。
- **周五交付**:M1 gate 绿。

> **完成记录(09-19,提前于计划)**:交付如上,M1 gate 绿(`dsl/m1_gate_test.go`
> 六项门禁全过,`make ci` 全绿)。实现落点:`Principal` 增推理归属三元组
> (Model/ModelVersion/PromptVersion,仅 `PrincipalModel` 有意义);ai 回调
> 载荷契约扩展 `model/model_version/prompt_version`(`parseAIResult`),`Feed`
> 在消费前把三元组**提升**为事件上的模型 principal——复用 W1 的
> Actor 入账/折叠/复现机制,宿主在 `Event.Principal` 上显式给的归属优先;
> 强制点落在 `Feed`(线性等待路径):`requirePrincipal` 节点(approval/
> subprocess/ai)上缺 principal(或 ai 缺完整模型归属)的决策被结构性拒绝
> ——不消费、不入账、`ErrPrincipalRequired` 哨兵可判别、实例保持等待,补上
> 归属重投即接受;部署期校验拒绝非决策节点声明 `requirePrincipal`。
> 性质测试扩展:线性审批 3/4 概率声明强制,驱动器对"停靠强制节点 + 事件
> 将被接受"的投递先投无归属版(断言必拒、未入幂等表)再补归属重投,
> 40 种子实测 23 次拒-投循环全部保持折叠精确。store-postgres:归属随
> occurrence JSON 全量落库;新增 `idx_dsl_journal_actor` 部分表达式索引与
> `ListDecisionsByPrincipal`(EXPLAIN 实测走索引);round-trip 集成测试覆盖
> 归属逐字段还原、Fold/FoldTo 复现与按 principal 查询(本地 Postgres 16.6
> 实跑通过)。过程中发现并修复存量缺陷:`Append` 先序列化后由库分配 seq,
> 导致持久化载荷内 seq 恒为零——`FoldTo`/快照增量重放对持久化日志静默失效;
> 现由 `jsonb_set` 在同语句内把分配的 seq 写进载荷,读取侧行序号权威对齐;
> 顺手把 store 集成测试的固定实例 ID 改为运行 nonce(复用库上二次运行不再
> 误报 "instance already exists")。
> W3 接手时注意:并行分支上的决策归属强制、waiting 槽/deadline 升级
> (temporal.go)的归属尚未覆盖;`RequirePrincipal` 在并行分支审批上声明
> 合法但运行期不生效(强制点只在线性路径),W3 一并补全后发 v2.1。

### W3 (09-28,刻意排轻) — M1c 收尾 + v2.1

- 补全归属覆盖面:waiting 槽、deadline 升级(`temporal.go`)、并行分支上的决策。
- 既有 journal 的兼容性说明(字段纯增量,旧日志照常折叠)。
- 发布 v2.1(Principals)。假期如启动,工作量顺延 W8。

### W4 (10-05) — M2a:版本注册表 + 结构化 diff

**目标**:规则变更像代码变更一样可审。

- `dsl`:定义版本注册表——不可变、有序的版本史,每版带 proposer + accountable owner。
- 结构化 diff:对 `ProcessDef`(节点/迁移/类型/时间契约)做**稳定排序**的 diff;
  黄金文件测试锁定输出格式(同输入必产出同 diff)。
- **周五交付**:同一定义两个版本的 diff API + 示例。

### W5 (10-12) — M2b:激活即事实 → **M2 gate 绿**

- 激活状态机 `draft → active`;激活要求 proposer + owner 双字段齐备。
- 激活动作本身记为 journal 事实(定义级,与实例级日志分账)。
- gate(ROADMAP M2):无 proposer/owner 的激活失败 ✓;两版本产出稳定结构 diff ✓。
- **周五交付**:v2.2(Attributed Rules)。

### W6 (10-19) — M3a:重放与对比

- 重放 API 形式化:`FoldTo` 已存在,补"任意点重放 + 从该点继续"的完整路径与测试。
- 运行对比 v1:同一定义两次执行 —— 路由一致性、升级率、成本(成本先以归属
  中的模型调用计数为代理指标,M4 预算落地后接真实额度)。
- **周五交付**:compare API(输入:两个实例日志;输出:指标集)。

### W7 (10-26) — M3b:证据包 → **M3 gate 绿**

- 证据包导出:定义 hash + 版本史 + 执行日志 + principal 归属,**hash 链**串联
  (关键点:occurrence 的**规范化序列化**——Go map 遍历无序,必须做键排序的
  canonical JSON,并加规范形式稳定性性质测试)。
- 离线校验 API:重算 hash 链 + 定义 hash 即验证;不依赖引擎内存状态。
- gate(ROADMAP M3):导出包自校验通过 ✓;篡改任一条目被检出 ✓。
- **周五交付**:证据包 v1(绑定 M2 版本史)。

### W8 (11-02) — 缓冲 / 复盘 / 换挡

按优先级消耗,用不完则提前拉平行轨道:

1. 吸收 W1–W7 的顺延(首先检查 M1–M3 门禁仍绿)。
2. 性质测试扩容:把 timer / ai(宿主 mock)纳入随机生成器,种子数提到 100。
3. 重新估算 W9–W14(此时 M4–M6 的估计还停留在纸面,以此周校准)。
4. **再规划检查点**:若 M1–M3 门禁未全绿,砍掉/顺延后续里程碑,不带着红门禁往前走。

### W9 (11-09) — M4a:工具白名单 → gate (a)

- `AIConfig` 增加工具白名单:每个工具带参数 schema(类型系统 `types.go` 现成)。
- validator:未声明工具 = 结构上不可达(与 `Choose` 候选集同构的有界代理,
  校验期即拦截);运行期越界选择升级为拦截事实。
- gate (a):未声明工具不可达 ✓。**周五交付**:工具白名单可用。

### W10 (11-16) — M4b:效果预算 → **M4 gate 绿**

- 效果预算声明:金额、频次、波及面(blast radius)——与 token/成本预算并列的一等限制。
- 工具循环按 `max_iterations` + 效应预算双界;**轨迹级**判定:一串各自合法的
  调用累加超预算 = 违规(不是只看单次越界)。
- 每次拦截(未声明工具 / schema 违规 / 预算击穿)追加声明类型的 journal 事实
  (§6 第 3 条;新增 `OccKind` 常量,`String()` 同步)。
- gate (b) 预算击穿停止循环 ✓;gate (c) 合法调用轨迹超预算被捕获 ✓。
- **周五交付**:v2.3(Bounded Capability)。

### W11 (11-23) — M5a:自治级别

- 定义声明 autonomy level;执行期强制(级别封顶效果预算/要求人工确认阈值)。
- 级别变更是 journal 事实,且**从执行历史推导**而非手配(输入 = M3 的对比指标)。

### W12 (11-30) — M5b:部署期证明 → **M5 gate 绿**

- `analyzer.go` 扩展:静态数据流近似,回答"此流程是否可能超过 ¥X / N 次 / 触达 M"。
- 允许保守(宁可误拒);与激活流程对接:可超限的定义在激活前被拒。
- gate:声明限额可被超过的定义无法激活 ✓。**周五交付**:v2.4。

### W13 (12-07) — M6a:意图单元

- 节点可声明完整工作单元:目标、约束、验收标准、资源预算、责任 principal
  (复用 M1 类型)、期限(复用 `temporal.go`);单元分解为子流程
  (`subprocess` 节点类型已存在,补语义)。

### W14 (12-14) — M6b:可验证性 → **M6 gate 绿** + 季度收尾

- 验收标准必须映射到可执行检查(表达式 / schema / 子流程 gate),否则部署期校验失败。
- gate:声明了但不可验证的验收标准在校验期被拒 ✓。
- 季度收尾:ROADMAP 状态表更新(六个里程碑 status 列)、发布 v3.0、下季方向提案。

---

## 3. 平行轨道 — Service Form(服务器形态)

不占里程碑周,由需求触发,插入点优先 W8 之后(内核形状稳定后):

- 触发条件:**有真实宿主**需要跨语言使用(ROADMAP:"can start whenever there is
  a host that needs it")。没有宿主就不开工,不为发布而发布。
- 预估 3 周:W-s1 `main` 包 + HTTP gRPC 服务骨架(journal/store 复用);
  W-s2 鉴权 + 租户 + 流式事件;W-s3 薄客户端 SDK 一个先行,其余按宿主语言排。
- 红线不变:服务是宿主,不是内核;`dsl/` 依旧零 LLM 依赖、零服务化依赖。

---

## 4. 风险与对策

| 风险 | 影响 | 对策 |
| --- | --- | --- |
| M1 改 journal 存储形状 | 旧日志兼容、store 迁移 | 归属字段纯增量(旧日志照常折叠);store-postgres 用 JSON 载荷天然兼容;W2 即补 round-trip 测试 |
| hash 链依赖规范化序列化 | M3 证据包可被 map 序非确定性破坏 | 专属 canonical JSON 实现 + 规范形式稳定性性质测试(W7 第一天做) |
| M5 静态证明过保守 | 合法定义被误拒 | 语言上收窄限额声明(金额/频次/触达),证明器允许保守;误拒案例回灌测试 |
| 推力不足或人力波动 | 计划整体后移 | W8 是唯一换挡点;里程碑按依赖序顺延,周边界不压缩 |
| 节假日(W2 尾/W3) | 局部减员 | 两周刻意排轻,顺延入 W8 |
| AI 归属元数据依赖宿主提供 | W2 进度 | 原则 3 决定了内核只记录不获取;宿主接口 W1 就定,给宿主一周适配期 |

---

## 5. 每周仪式

- **周一**:从本文件取本周切片,门禁测试先写(红)。
- **周三**:中途检查——若切片明显装不下,当周只保交付物,其余移入 W8。
- **周五**:跑 `make ci` + 本周 gate;更新 ROADMAP 状态列与本文档实际完成标记;
  状态口径只有 `open` / `gate 绿`。

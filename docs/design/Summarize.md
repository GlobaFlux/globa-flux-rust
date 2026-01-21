# Self-Learning Youtuber Revenue Agent

> ⚠️ Legacy snapshot. Canonical docs live in:
> `../../../globa-flux-hydrogen/docs/design/youtube-revenue-agent/README.md`

## Overall System Design (AI-Implementable)

---

## 1. 目标（Single Objective）

> 最大化长期营收期望值，通过每天输出唯一方向：
> **EXPLOIT / EXPLORE / PROTECT**
> 并减少“灾难性营收错误”。

**成长定义（可验证）**

* 灾难性错误率下降
* confidence 校准更准
* 方向切换抖动减少
* Protect 过度率下降（但不以牺牲安全为代价）

---

## 2. 架构总览（Separation of Concerns）

### 四条线并行，但互相隔离

1. **Data Sync Line（事实获取）**
   YouTube → TiDB：只写事实，不做决策

2. **Decision Line（每日方向）**
   事实 → signals → decision_daily（唯一真相源）

3. **Chat Line（对话交互）**
   只读 decision_daily + 规则化风险评估（不改策略）

4. **Learning Line（周更学习）**
   用回放 replay 从 outcome 学参数（只改参数，不改结构）

---

## 3. 数据层设计（TiDB 表设计）

### 3.1 事实表（不直接给 Chat 推理）

**video_daily_metrics**（核心）

* `channel_id, dt, video_id`（PK）
* `estimated_revenue_usd`
* `impressions`
* `updated_at`

**sync_run_log**

* 每次同步的状态、错误码、耗时、范围

---

### 3.2 决策表（Chat 的唯一真相源）

**decision_daily**

* `channel_id, as_of_dt`（PK）
* `direction`（EXPLOIT/EXPLORE/PROTECT）
* `confidence`
* `evidence_json`
* `forbidden_json`
* `reevaluate_json`
* `created_at`

---

### 3.3 观测行为（不靠用户按钮，系统自动推断）

**observed_actions**

* `channel_id, dt`（PK的一部分）
* `action_type`（枚举）
* `action_meta_json`（可选）

> action_type 示例：
> `publish`, `change_metadata`, `explore_topic`, `no_action`
> （能自动推断多少算多少；推断不了就空着）

---

### 3.4 结果标签（用于学习）

**decision_outcome**

* `channel_id, decision_dt`（PK的一部分）
* `outcome_dt`（例如 decision_dt+7）
* `revenue_change_pct_7d`
* `catastrophic_flag`（是否灾难性）
* `new_top_asset_flag`（探索是否带来新头部）
* `notes`（可选）

---

### 3.5 策略参数与版本（可回滚）

**policy_params**

* `channel_id, version`（PK）
* `params_json`（阈值/权重/校准曲线）
* `created_at`
* `created_by`（system/manual）

**policy_eval_report**

* `channel_id, candidate_version`
* `replay_metrics_json`（灾难性错误率、抖动等）
* `approved`（bool）
* `created_at`

---

## 4. 决策引擎（Daily Decision Engine）

### 4.1 决策窗口

* 每天 d 输出决策
* 只用 `d-6..d` 的 7 天窗口
* endDate = today-1（避免当天未结算）

### 4.2 signals（固定结构，未来不轻易改）

* `revenue_concentration = top1_revenue / total_revenue`
* `dominant_asset_trend = slope(top1_revenue over 7d)`
* `system_stability = volatility(total_revenue over 7d)`
* `new_asset_emergence = whether a new video enters Top-N`

### 4.3 方向规则（结构冻结）

优先级写死：

1. 数据不足/冲突 → PROTECT
2. 高集中 + 头部上升 → EXPLOIT
3. 头部下滑 或 新资产出现 → EXPLORE
4. 默认 → PROTECT

### 4.4 confidence（v1 → v2）

* v1：规则一致性 + 安全默认值
* v2：用历史 replay 校准（见学习线）

---

## 5. Chat 交互（Agent “外壳”）

### 5.1 Chat 的职责

* 展示今日方向（从 decision_daily 读取）
* 回答 “我想做 X 可以吗？”（风险评估）
* 回答 “最近我处在什么阶段？”（历史总结）

### 5.2 Chat 的强约束

* Chat **不得**自己算方向
* Chat 只能用工具读 DB
* Chat 输出必须模板化（见下）

### 5.3 action_type 枚举（防胡说）

自由文本先映射到枚举：

* `change_thumbnail`
* `change_title`
* `new_topic`
* `new_format`
* `upload_more_of_same`
* `upload_shorts`
* `pause_upload`
* `no_action`

映射不了 → 必须问澄清。

### 5.4 Chat 输出模板（固定）

```
【今日方向】
EXPLOIT（confidence: 0.72）

【今天不要做】
- ...
- ...
- ...

【如果出现这些情况，再评估】
- ...
- ...

【一句原因】
（最多一句，不讲指标）
```

---

## 6. 学习线（Self-Learning / Self-Growth）

### 6.1 学习不是“模型在线学习”，而是“参数受控更新”

学习只允许改变：

* 阈值（threshold）
* confidence 校准曲线
* （可选）少量权重

**禁止自动改变：**

* signal 定义
* 规则优先级结构
* 输出空间（仍然只能三方向）

### 6.2 学习的监督信号（可靠来源）

学习不依赖用户“我照做了”的主观按钮，而依赖：

1. **观测行为**（observed_actions）
2. **结果标签**（decision_outcome）

用户按钮可有，但只进体验层，不进策略学习。

### 6.3 outcome 标签定义（示例）

* `catastrophic_flag = 1` 若：

  * T+7 总收入跌幅 > X%（可按频道规模动态阈值）
  * 或头部收入/展示断崖式下降（可选）

* `new_top_asset_flag = 1` 若：

  * T+7 出现新视频进入 Top-N 收入

### 6.4 学习任务（每周一次）

**Weekly Policy Update Job**
输入：

* 最近 8–12 周的 (signals, decision, observed_actions, outcomes)

输出：

* 候选参数 `policy_params` 新版本（candidate）

### 6.5 Replay Gate（上线闸门）

候选参数必须通过回放：

* 对历史每一天模拟输出方向
* 计算指标：

  * 灾难性错误率（必须不升）
  * Protect 过度率（可下降）
  * 方向抖动（不升）
  * confidence 校准误差（下降）

若任一指标失败：

* 不批准（approved=false）
* 不发布

### 6.6 发布与回滚

* policy_params 版本化
* 线上仅指向 `active_version`
* 监控异常 → 自动回滚到上一个版本

---

## 7. 多频道并行学习但不互相污染

### 7.1 默认隔离

* 每个 channel 单独 policy_params
* 学习任务按 channel 独立跑

### 7.2 共享只共享“先验摘要”

可选 cohort 分组（同类频道）：

* Shorts-heavy / Long-heavy
* Ads-heavy / Membership-heavy
* Scale bucket（小/中/大）

共享内容仅限：

* 分布摘要（分位数、成功率桶）
* 作为小样本频道的 prior

绝不共享：

* 具体样本数据
* 直接阈值替换

---

## 8. 运行任务（Jobs）

### Daily Jobs

1. Sync YouTube → video_daily_metrics
2. Compute signals → decision_daily
3. Compute observed_actions（可选）
4. Backfill outcome 标签（对 7 天前的 decision 打结果）

### Weekly Jobs

1. Train/calibrate params（candidate）
2. Replay gate evaluation
3. Approve & activate version（或拒绝）

---

## 9. 监控与安全（必须）

### 关键指标（每频道）

* catastrophic_error_rate（硬指标）
* protect_rate（过保守）
* direction_switch_rate（抖动）
* confidence_calibration_error（校准误差）
* sync_success_rate（数据链路健康）

### 自动降级

* catastrophic_error 上升 → 强制 PROTECT-only（L2）
* sync 失败 → PROTECT + 提示数据未更新

---

## 10. 实施路线图（最短可落地）

### Phase 1（先跑起来）

* Daily sync + decision_daily
* Chat 只读决策并模板化输出

### Phase 2（加可学习）

* decision_outcome（T+7 标签）
* weekly replay gate
* policy_params 版本化与回滚

### Phase 3（规模化）

* cohort prior（同类频道先验）
* 更稳定的 confidence 校准

---

## 11. 验收标准（Done Definition）

系统完成且可对外试点，当且仅当：

1. 每天每频道 1 条 decision_daily
2. Chat 永远不输出多方向
3. 学习更新必须通过 replay gate
4. 参数可回滚且有版本记录
5. 灾难性错误率可度量、可控、长期下降

---

## 12. 商业化与商用边界（Commercial Readiness）

### 12.1 ICP（付费方）与交付形态

优先 ICP（v1 先做可卖）：

* A 单创作者（单频道）：要“每天知道该稳还是该试”，不想看复杂报表
* B 团队/MCN（多频道）：要“批量运营 + 权限 + 复盘 + 稳定性”

后续可扩展 ICP（v2+）：

* 品牌/企业频道：要“合规/审计/SLA/可解释 + 可控风险”

交付形态（可并存）：

* SaaS 自助（默认）
* 人工+工具（高客单的运营服务）
* 私有化部署（Enterprise）

### 12.2 定价与单位经济（Unit Economics）

定价锚点（建议二选一为主）：

* 按 `channel/月`（简单、与价值对齐）
* 按 `seat/月` + `channel 限额`（团队更友好）

建议套餐（同时覆盖 A+B，示例）：

* Creator：1 channel / 1 seat（自助付费、轻支持；`$19/mo`，可选年付 `$190/yr`，默认展示月付）
* Studio/MCN：N channels / seats（含 Portfolio、RBAC、批量周报、审计）
* Add-ons：额外 channel、额外 seat、优先支持、数据导出/API

年付条款（v1）：

* 年付 `$190/yr`：不提供主动退款（除法律强制）；取消后到期不续费

单位经济必须先立“硬护栏”：

* YouTube API 配额预算（按租户/按日）
* LLM 调用预算（按租户/按日），超额自动降级为“只读 + PROTECT”
* 计算成本：signals/回放/周更学习尽量批处理、离线化

### 12.3 价值证明（ROI）与责任边界

商用系统需要可证明、可解释、可追溯：

* 结果归因：建议做 holdout（保留组）或交替策略窗，避免“自然波动”误判为收益
* 输出边界：明确“建议系统”而非保证收益；高风险动作需二次确认（或默认禁止）
* 审计：每次决策的 evidence/forbidden/reevaluate 必须可回看

### 12.4 Onboarding（降低试用摩擦）

A 单创作者：

* OAuth 连接 YouTube → 自动回填近 8–12 周历史
* 选择频道类型与目标（增长/稳健/变现）→ 载入默认 policy prior
* 首周只输出 PROTECT/低频建议（建立信任），第二周再逐步开放 EXPLORE

B 团队/MCN：

* 批量连接多个 channel + 导入成员（邀请/权限）
* 先产出 Portfolio 基线周报（风险/集中度/波动/建议遵循情况）
* 配额与护栏按 workspace 配置（预算、黑名单动作、审批）

### 12.5 功能对照（Creator vs Studio/MCN）

| 能力 | A Creator | B Studio/MCN |
|---|---|---|
| 频道规模 | 单频道 | 多频道（10–100+） |
| 视图 | 单频道今日方向 | Portfolio + 单频道下钻 |
| 协作 | 轻量（可选分享） | RBAC + 审计 + 批量操作 |
| 交付节奏 | 每日方向 + 周报 | 每日方向 + 周报/复盘 + 运营看板 |
| 成本护栏 | 默认预算/自动降级 | 按 workspace 配额/预算/审批 |

### 12.6 A Creator：自助付费转化（PLG）设计

核心目标：让用户在 **5 分钟内获得可执行价值**，并在试用期内形成“每日打开”习惯。

三种转化模型（建议先选一种主线做到底）：

1. **Free Trial（推荐）**：无信用卡试用 30 天（v1）→ 到期降级 → 付费解锁
2. Freemium：永久免费层（强限额）→ 付费解锁（更难控成本但利于扩散）
3. Hybrid：先给 1 次免费“今日方向”→ 再进入 Trial（兼顾低摩擦与控成本）

推荐（v1）：Hybrid → Free Trial

* 第 1 步（免费）：连接频道后立刻生成 1 次 `decision_daily` + 1 份“过去 7 天风险/集中度”小结
* 第 2 步（试用）：开启 30 天试用，解锁：每日方向、有限 Chat 风险评估、周报
* 第 3 步（付费）：解锁完整用量（仍受预算/配额护栏约束）

不绑卡试用（v1 选择）：

* 试用期开启不要求信用卡/支付方式；仅在升级付费时进入 checkout
* 试用严格限额（见第 14.5/15.4）：1 channel、有限 Chat 风险评估次数、日预算上限
* 试用资格：必须完成 OAuth；同一 OAuth 身份在一定窗口内仅允许 1 次试用（防薅）
* 30 天试用采用“阶梯权益”（v1）：前 7 天高权益、后 23 天更严限额（控成本并促进升级）

阶梯权益默认值（v1，可调；按试用成本上限 $5）：

* 目标：30 天 no-CC 试用的**可变成本上限** ≤ `$5 / trial`
* 预算约束：`7 * boost_budget_usd_per_day + 23 * sustain_budget_usd_per_day <= 5`
* 取默认值：`7*0.45 + 23*0.08 = 4.99`（满足上限）

| 阶段 | 天数 | `chat_risk_checks_per_day` | `budget_usd_per_day` | 备注 |
|---|---:|---:|---:|---|
| Boost | 1–7 | 10 | 0.45 | 促激活：允许更密集的“我想做 X 可以吗？” |
| Sustain | 8–30 | 3 | 0.08 | 控成本：保留每日方向+周报，限制高频问答 |

体验与转化提示：

* Day 6/7 提前告知：明天起试用进入 Sustain（额度下调），可升级保持更高额度
* Day 8 自动下调额度：不报错，给出“剩余额度/下次重置/升级入口”

激活路径（Activation）与指标：

* TTV：从注册到看到“今日方向” < 5 分钟
* A1：完成 OAuth 连接（connect_rate）
* A2：查看今日方向 + 至少 1 次“我想做 X 可以吗？”（activation_rate）
* R7：7 天内至少 4 天打开（habit_rate）
* R30：30 天内至少 16 天打开（≈ 每周 4 天；可调）
* P：试用→付费转化（trial_to_paid）

---

## 13. 市场与增长（Go-To-Market）

### 13.1 差异化（为什么是你）

* 受控学习（controlled learning）：可验证、可回滚、不会被一句话污染
* “每日唯一方向”降低认知负担（比“十条建议”更易执行与复盘）
* 可审计与可度量：replay gate + 监控指标让系统可信

### 13.2 获客与留存机制（可运营）

* 获客：创作者社区/课程合作、MCN 渠道分销、模板化“增长复盘报告”
* 留存：每日推送 + 每周复盘（策略版本变化、收益/风险指标、建议执行情况）
* 病毒传播：自动生成可分享的“频道周报”（脱敏/可选）

### 13.3 销售路径（Motion）

* A 单创作者：PLG（免费试用/低价入门）→ 自助升级 → 订阅续费
* B 团队/MCN：渠道合作（MCN/经纪/培训）+ 销售辅助（POC/试点）→ 年付/量大折扣

### 13.4 A Creator：PLG 指标与实验（Growth Ops）

North Star（建议二选一）：

* `WAC`（Weekly Active Channels）：每周有消费决策/周报的频道数
* `Habit`：每周 ≥4 天查看“今日方向”的频道数

必须埋点的转化漏斗：

* visit → signup → oauth_connected → first_decision_viewed → first_risk_check → trial_started → paid

优先实验（按 ROI 排序）：

* 付费墙时机：连接前 / 连接后看到 1 次结果后 / 试用结束前
* 试用策略：14 vs 30 天（默认 30）；不绑卡试用（默认） vs “延长/解锁高级权益需绑定支付方式”（后续实验）
* 阶梯权益：前 7 天 Boost（默认） vs 全程同等限额（后续实验）
* 定价呈现：默认月付（`$19/mo`）+ 年付切换（`$190/yr`，省约 17%）；年付不退款（v1）；后续实验“年付默认 vs 月付默认”
* 通知触达：email/推送频率与文案（降低流失，提高习惯）

---

## 14. 商用工程化（SaaS / Enterprise Readiness）

### 14.1 多租户、权限与审计

最小可用能力：

* tenant/workspace 概念 + RBAC（owner/admin/member/viewer）
* 频道授权与成员可见性隔离（谁能看哪个 channel）
* 审计日志（谁在何时做了什么：连号、带请求上下文）

建议新增系统表（示例）：

* `tenants`, `users`, `memberships`
* `channel_connections`（OAuth token 加密存储 + scope + 失效时间）
* `audit_events`
* `usage_events`（计费/限额依据）

### 14.2 安全与合规（必须提前设计）

* OAuth token：加密存储、最小 scope、轮换与吊销、按租户隔离
* 数据生命周期：保留策略、导出、删除（含聊天记录/决策证据）
* Prompt/Tool 安全：Chat 工具调用白名单；防提示注入（把 DB/工具输出当不可信输入处理）
* ToS 风险：对 YouTube API 的存储与使用范围做明确约束（避免“违规留存/再分发”）

### 14.3 可靠性（SLO）与降级

建议先定义 SLO（否则无法 scale）：

* 每日 `decision_daily` 生成时间：T+X（例如本地时区 08:00 前）
* 数据新鲜度：`video_daily_metrics` 的 `dt` 至少到 D-1

降级策略（与第 9 节一致，但更细）：

* 同步失败/数据缺失 → PROTECT + “数据未更新”提示 + 重试计划
* 预算耗尽/系统异常 → 禁止高风险动作，仅保留解释与复盘

### 14.4 可观测性与运营

除第 9 节指标外，还需商用指标：

* 活跃租户/频道、连接成功率、数据新鲜度分布
* 预算消耗（API/LLM/计算）、降级触发率
* 建议采纳率（通过 observed_actions 推断）、周报打开率

### 14.5 计费与权益（Billing & Entitlements）

商用 PLG 需要“可执行的权益层”，否则无法控成本与转化：

* 订阅状态：`trialing`, `active`, `past_due`, `canceled`, `downgraded`
* 权益（示例）：`max_channels`, `chat_risk_checks_per_day`, `weekly_report`, `export`, `budget_usd_per_day`
* 强制在所有对外 API/Job 上做 entitlement check（超额→降级，不是报错）
* 不绑卡试用：`trialing` 不要求 payment method；到期自动进入 `downgraded`（保留只读/历史/升级入口）
* 试用时长：`trial_length_days=30`（v1）；`trial_ends_at = trial_started_at + 30d`
* 阶梯权益：`trial_boost_ends_at = trial_started_at + 7d`；`trial_day`/时间窗驱动 entitlement resolver 计算“有效权益”（避免人工改表）
* 试用成本上限：`trial_cost_cap_usd=5`（v1，按租户/频道）；用于指导 `budget_usd_per_day` 的默认值与风控阈值

建议新增/落地的数据对象（与第 14.1/usage_events 配套）：

* `plans`：套餐定义（含默认权益）
* `subscriptions`：租户订阅与状态机
* `entitlements`：当前生效权益（可覆盖 plan 默认值）
* `billing_events`：外部支付事件落库（幂等键，便于审计/对账/重放）

---

## 15. Scale 能力（平台化与规模化运营）

### 15.1 技术扩展（计算/存储/队列）

* 按 `channel_id + dt` 分区/索引（避免全表扫描）
* Jobs 任务队列化（sync / decision / backfill / weekly replay 分离 worker pool）
* 缓存与批处理：同一租户多次查询复用 signals/证据

### 15.2 多频道运营能力（产品层）

* Portfolio 视图：把多频道的 direction/风险/收益集中看
* 批量操作：一键生成“本周探索池”、一键暂停高风险动作（PROTECT 执行）

### 15.3 运营与交付规模化

* 分层支持：自助文档 → 工单 → 专属 CS
* 标准化复盘：每周固定输出（指标 + 结论 + 下周策略）
* 企业特性：SSO、私有化、专属模型/策略版本隔离

### 15.4 PLG 成本与滥用控制（A Creator 必备）

* 默认限额：对 free/trial/paid 分层限额（LLM 调用、报表生成、并发查询）
* 成本剎车：到达租户预算/配额 → 自动降级为“只读 + PROTECT”，并给出恢复路径（升级/次日重置）
* Free 层不跑重计算：weekly replay/training 仅对付费或按需触发（避免免费用户拖垮批处理）
* 阶梯权益控成本：Trial Day 1–7（Boost）高额度、Day 8–30（Sustain）低额度；Day 6/7 预告、Day 8 自动下调并给出升级路径
* 试用滥用防护（不绑卡更关键）：
  * 必须 OAuth 后才发放试用（降低机器注册价值）
  * 试用发放约束：同一 OAuth 身份/频道在窗口期内仅 1 次；检测重复试用（OAuth/设备/网络信号）
  * 关键入口加速率限制与人机校验（注册/连接/试用开启）
  * 异常用量告警：突然的高频风险评估/报表请求/并发查询 → 自动降级并拉黑可疑租户

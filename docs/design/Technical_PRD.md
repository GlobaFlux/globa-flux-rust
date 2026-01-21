# 技术 PRD：Self-Learning Youtuber Revenue Agent（Shopify Hydrogen + TiDB Serverless + Vercel）

> ⚠️ Legacy snapshot. Canonical docs live in:
> `../../../globa-flux-hydrogen/docs/design/youtube-revenue-agent/README.md`

> Status: Draft v0.6  
> Updated: 2026-01-20  
> 本文目标：把 `docs/design/Summarize.md` 的系统设计，整理成“可直接开工”的技术 PRD，并明确对客技术栈：Hydrogen + TiDB Serverless（必要时用 Vercel Rust Functions）。

---

## 0. 一句话定义

面向创作者/团队的“每日唯一方向”营收策略 Agent：系统每天只输出 **EXPLOIT / EXPLORE / PROTECT** 之一，并通过受控学习（可回放、可闸门、可回滚）持续降低灾难性错误率。

---

## 1. 目标、非目标、成功指标

### 1.1 目标（MVP 必须满足）

- **每天每频道 1 条 `decision_daily`**（唯一真相源），可追溯证据与禁区
- **对话层（Chat）永远不“自算方向”**：只读决策与规则化风险评估（防胡说）
- **学习更新必须可控**：只允许改参数/校准曲线；发布前必须通过 replay gate；线上可回滚
- **商用可跑**：多租户隔离、审计、可观测、成本护栏（YouTube/LLM/计算预算）；RBAC 结构预留（v1=A Creator 可先只做 Owner）

### 1.2 非目标（v1 明确不做/不承诺）

- 不做模型在线学习（Online Learning），不让用户一句话污染策略
- 不做“十条建议的内容工厂”；v1 坚持每日唯一方向（降低执行成本）
- 不承诺“保证收益”，仅提供可审计的建议系统与风险边界

### 1.3 成功指标（首批付费前必须可度量）

- `decision_daily` 生成准时率（按租户时区，T+X 前生成）
- 数据新鲜度（至少到 D-1）与 `sync_success_rate`
- `catastrophic_error_rate`（硬指标）趋势下降
- `direction_switch_rate`（抖动）不升/下降
- `confidence_calibration_error` 下降
- PLG（A Creator）：TTV < 5min、R7/R30 习惯指标、Trial→Paid 转化

---

## 2. 用户与权限模型（SaaS 交付）

### 2.1 角色（最小集合）

- **Tenant/Workspace**：计费与权限边界（v1 已确认优先 A Creator：默认 1 人工作区）
- **Owner/Admin/Member/Viewer**（RBAC，v1 可只落地 Owner；其余为 vNext 预留）
- **Channel Connection**：YouTube OAuth 授权对象，归属某 tenant
- **Billing Identity（Shopify Customer）**：每个 tenant 绑定一个 Shopify Customer（`shopify_customer_id`），用于订阅/结算与对账

### 2.2 v1 用户路径（A Creator / PLG）

1. 访问对客站点（Hydrogen）→ 注册/登录
2. 连接 YouTube OAuth → 回填历史数据（8–12 周）
3. 生成首条 `decision_daily`（可在 5 分钟内看到）
4. 查看“今日方向”→ 做 1 次风险评估（“我想做 X 可以吗？”）
5. 每日定时生成决策 + 每周复盘/周报（自助）

### 2.3 vNext（B Studio/MCN，后续再做）

- 多频道 Portfolio 视图（按风险/方向/波动聚合）
- 成员邀请与更细 RBAC
- 批量操作（批量周报、批量保护策略/审批）

---

## 3. 技术栈与部署（已确认 + 可选项）

### 3.1 对客 Web：Shopify Hydrogen（Remix）

定位：Landing / Pricing / Auth / Onboarding / Dashboard / Chat UI（对客统一入口）。

关键约束：
- Hydrogen 更偏向 SSR/同构数据加载；建议把“敏感逻辑/密钥/写操作”全部放到后端 API（Functions/Service）
- 对外 API 统一做：鉴权、租户隔离、权益校验、速率限制、审计

### 3.2 Database：TiDB Serverless（MySQL 协议）

定位：唯一持久化真相源（事实表/决策表/学习表/商用表）。

关键约束（对 Serverless/高并发要提前设计）：
- 连接数与短连接成本：后端需控制连接池上限、尽量复用连接、避免把 DB 放到 Edge Runtime
- 所有写入都要幂等（job/webhook 重放是常态）

### 3.3 Backend：Vercel Functions（已确认：Job fan-out；Rust 为兜底）

结论（已确认）：**尽量全部跑在 Vercel Functions**，通过“调度器 + 单频道 worker”实现 fan-out，避免单次函数超时。

组成（MVP 最小闭环）：
- API：认证/租户/频道/决策/Chat/Shopify webhook
- Jobs：
  - Vercel Cron → `/api/jobs/*/dispatch`（只做枚举与 enqueue，不做重计算）
  - Vercel Cron → `/api/jobs/worker/tick`（每分钟触发：claim N 条任务并顺序处理，直到接近超时预算）

fan-out 的派发方式（已确认）：**TiDB `job_tasks` 任务表（依赖最少）**
- `dispatch` 只负责把任务写入 `job_tasks`（幂等 enqueue）
- `worker` 由 Vercel Cron 定时触发：每次 claim N 条 pending 并处理（单 task=单 channel），直到接近超时预算
- 重试/退避/死信通过 `job_tasks` 字段实现（无需外部队列）

> Rust：仅在 weekly replay/批处理即使 fan-out 仍无法满足超时/性能时，再把 heavy worker 迁出为长驻服务；v1 先不引入。

### 3.4 Billing：Shopify Subscriptions（已确认）

结论（已确认）：**同时使用 Shopify 做订阅/结算，作为付费状态的事实来源（source of truth）**。

推荐落地方式（SaaS to Creator 的最小闭环）：
- 套餐在 Shopify 中建为订阅商品（product/variant + selling plan）
- Hydrogen 发起 checkout（创建购物车/跳转 checkout）
- 后端接收 Shopify Webhook（验签 + 幂等）→ 落库 `billing_events` → 更新 `subscriptions`/`entitlements`

与 no-CC trial 的关系（兼容既有 PLG 决策）：
- `trialing` 可由我们内部发放（不走 Shopify 付款），到期降级 `downgraded`
- 用户升级为付费时，才进入 Shopify checkout；Shopify 成功后切换为 `active`

---

## 4. 系统架构（Separation of Concerns）

沿用 `docs/design/Summarize.md` 的“四条线隔离”，并补上对客与商用层：

1. **Data Sync Line（事实获取）**  
   YouTube → TiDB：只写事实，不做决策
2. **Decision Line（每日方向）**  
   事实 → signals → `decision_daily`（唯一真相源）
3. **Chat Line（对话交互）**  
   只读 `decision_daily` + 规则化风险评估（不改策略）
4. **Learning Line（周更学习）**  
   replay → outcome → 参数版本（只改参数，不改结构）
5. **Commercial Layer（多租户/计费/权益/审计）**  
   tenant、RBAC、订阅状态机、预算护栏、审计与用量
6. **Customer Web（Hydrogen）**  
   负责体验与呈现：连接、仪表盘、周报、Chat UI（不持有敏感密钥）

---

## 5. 数据模型（TiDB，MVP 最小可落地）

> 核心表详见 `docs/design/Summarize.md` 第 3 节；此处补充“多租户字段 + 商用表”以便直接建库。

### 5.1 核心事实/决策/学习表（需加租户字段）

对以下表，建议统一加字段：`tenant_id`（或 `workspace_id`）、`created_at/updated_at`、必要的幂等键：

- `video_daily_metrics`（事实）
- `sync_run_log`
- `decision_daily`（唯一真相源）
- `observed_actions`
- `decision_outcome`
- `policy_params`（versioned）
- `policy_eval_report`（replay gate）

索引建议（最小集合）：
- 所有核心表：`(tenant_id, channel_id, dt)` 或 `(tenant_id, channel_id, as_of_dt)` 复合索引
- `decision_daily`：额外索引 `(tenant_id, channel_id, created_at)`

### 5.2 多租户/权限/审计（商用必备）

最小表集合（建议）：

- `tenants`：`id`, `name`, `plan_id`, `created_at`
- `users`：`id`, `email`, `created_at`
- `memberships`：`tenant_id`, `user_id`, `role`, `created_at`
- `channel_connections`：`tenant_id`, `channel_id`, `oauth_provider`, `token_ciphertext`, `scopes`, `expires_at`, `revoked_at`
- `audit_events`：`tenant_id`, `actor_user_id`, `action`, `target_type`, `target_id`, `request_id`, `ip`, `ua`, `created_at`
- `usage_events`：`tenant_id`, `event_type`, `quantity`, `cost_usd`, `occurred_at`

### 5.3 Job 队列（TiDB 任务表，已确认）

用于替代外部队列，实现 job fan-out（单 task=单 channel）：

- `job_tasks`
  - `id`（PK，UUID/雪花）
  - `tenant_id`
  - `job_type`（`daily_channel` / `weekly_channel` / `backfill_outcome` 等）
  - `channel_id`
  - `run_for_dt`（可选：daily 任务对应的日期）
  - `status`（`pending`/`running`/`succeeded`/`retrying`/`failed`/`dead`）
  - `attempt` / `max_attempt`
  - `run_after`（下次允许执行时间；用于 backoff）
  - `locked_by` / `locked_at`（防并发重复处理；可配 lock TTL）
  - `last_error`（截断存储）
  - `created_at` / `updated_at`

幂等/去重（必须）：
- 增加唯一键 `dedupe_key`（例如：`tenant_id:job_type:channel_id:run_for_dt`）
- `dispatch` 使用 `INSERT ... ON DUPLICATE KEY UPDATE`（或 `INSERT IGNORE`）确保可重放

claim 方式（推荐事务）：
- 在事务内 `SELECT ... FOR UPDATE` 取 N 条 `status='pending' AND run_after<=now()` 的任务 → 标记为 `running` 并写入 `locked_by/locked_at`

worker 模式（已确认：简单版）：
- 每分钟跑一次 `worker/tick`：单次 claim `N`（建议起步 `N=10`，后续可配），顺序处理
- 单次 tick 做“尽量多但不超时”：接近函数超时预算就提前退出，下一分钟继续
- 锁回收：若 `status='running' AND locked_at < now()-lock_ttl`，视为 worker 崩溃/超时，重置为 `retrying` 并设置 `run_after=now()`

---

### 5.4 计费与权益（若走 Shopify/订阅）

- `plans`：套餐定义（包含默认权益 JSON）
- `plans` 需要包含 Shopify 映射字段（示例）：`shopify_product_id`, `shopify_variant_id`, `shopify_selling_plan_id`
- `subscriptions`：`tenant_id`, `status`（trialing/active/past_due/canceled/downgraded）, `provider`(shopify), `provider_customer_id`(shopify_customer_id), `provider_subscription_id`(shopify), `trial_ends_at`, `current_period_end`
- `entitlements`：`tenant_id`, `max_channels`, `chat_risk_checks_per_day`, `weekly_report`, `budget_usd_per_day`, ...
- `billing_events`：Shopify 事件落库（必须含 `provider_event_id` 幂等键；记录 topic/occurred_at/raw_payload）

v1 默认（A Creator，30 天 no-CC trial）：

- `trial_length_days=30`，`trial_boost_ends_at=trial_started_at+7d`
- `max_channels=1`
- `chat_risk_checks_per_day`：Boost（Day1–7）=10，Sustain（Day8–30）=3
- `budget_usd_per_day`：Boost=0.45，Sustain=0.08（对应 `$5 / trial` 成本上限）
- entitlement 由 resolver 按当前时间窗计算（避免人工改表）

Creator 定价（v1）：

- `price_usd_per_month=19`（默认展示/默认选中）
- `price_usd_per_year=190`（可选切换，约省 17%）
- 年付退款政策（v1）：不提供主动退款（除法律强制）；取消后到期不续费
- 实现建议：Shopify 用单独 selling plan/variant 承载年付；Hydrogen UI 提供月/年切换但不默认年付

---

## 6. API 设计（面向 Hydrogen 的最小接口）

### 6.1 认证与会话（建议）

- Hydrogen 端使用 Cookie Session（服务端渲染友好）
- 所有 API 请求必须带：`tenant_id` 上下文（从 session 解出），后端禁止从前端信任 tenant_id

### 6.2 Endpoint 列表（MVP）

| Method | Path | Auth | 作用 |
|---|---|---|---|
| GET | `/app` | ✅ | 仪表盘入口（当前 tenant 概览） |
| POST | `/api/oauth/youtube/start` | ✅ | 发起 OAuth（生成 state，写入短期存储） |
| GET | `/api/oauth/youtube/callback` | ✅ | 回调落库（token 加密存储），绑定 channel_connection |
| POST | `/api/jobs/daily/dispatch` | 🔒（cron） | daily 调度器：enqueue `job_tasks`（每 channel 1 task） |
| POST | `/api/jobs/weekly/dispatch` | 🔒（cron） | weekly 调度器：enqueue `job_tasks`（每 channel 1 task） |
| POST | `/api/jobs/worker/tick` | 🔒（cron） | claim N 条 `job_tasks` 并顺序执行（简单版 fan-out） |
| POST | `/api/chat/risk_check` | ✅ | 风险评估（必须 action_type 枚举化） |
| GET | `/api/decision/today` | ✅ | 读取今日方向（只读） |
| POST | `/api/webhooks/billing` | 🔒（shopify） | Shopify webhook（订单/订阅状态变化）→ 状态机落库 |

🔒 说明：
- cron/internal 接口必须：签名鉴权 + 速率限制 + 幂等键
- Shopify webhook 必须：验签（HMAC）+ 幂等处理 + 可重放（按 provider_event_id）

---

## 7. Jobs（Daily/Weekly）与可扩展性

### 7.1 Daily pipeline（每频道）

1. Sync：YouTube → `video_daily_metrics`（只写事实）
2. Compute signals → 写 `decision_daily`（唯一真相源）
3. observed_actions（能自动推断多少算多少）
4. outcome backfill：对 `decision_dt = today-7` 打标签写 `decision_outcome`

幂等与重试：
- 以 `(tenant_id, channel_id, dt)` 作为幂等键；可重放不产生重复
- `sync_run_log` 记录范围、耗时、错误码，便于告警与补跑

### 7.2 Weekly pipeline（每频道）

1. 基于近 8–12 周：生成候选参数 `policy_params(candidate)`
2. replay gate：对历史回放并写 `policy_eval_report`
3. 若通过：激活 `active_version`；若失败：拒绝并保留报告

### 7.3 为什么必须 fan-out（为适配 Serverless）

- daily/weekly 的复杂度会随“租户数 × 频道数”线性增长  
- 需要把 job 拆成“调度器 + 单频道 worker”，才能在超时/限流下稳定扩展

---

## 8. Chat 交互与安全边界（防胡说 / 防注入）

强约束（必须实现）：

- Chat **不得**计算方向，只能读 `decision_daily`
- 输出必须模板化；action 必须先映射到 `action_type` 枚举；映射不了必须澄清
- 工具调用白名单：只允许读必要表/写审计与用量，不允许任意 SQL
- 把外部输入（含 DB 文本）当“不可信输入”处理，防 prompt injection

输出模板（沿用设计）见 `docs/design/Summarize.md` 第 5 节。

---

## 9. 可观测性、SLO 与降级（商用必备）

### 9.1 SLO（建议先定，避免后期扯皮）

- 每日决策准时：本地时区 08:00 前生成（可配置）
- 数据新鲜度：`video_daily_metrics.dt >= D-1`
- webhook 处理：T+5min 内完成状态机落库

### 9.2 关键指标（每频道 + 每租户聚合）

- `catastrophic_error_rate`（硬指标）
- `protect_rate`（过保守）
- `direction_switch_rate`（抖动）
- `confidence_calibration_error`
- `sync_success_rate` / `sync_latency`
- `budget_spend_usd_per_day`（YouTube/LLM/compute 拆分）

### 9.3 自动降级（必须）

- sync 失败/数据缺失 → 强制 PROTECT + 提示数据未更新
- 预算耗尽/异常用量 → 降级为“只读 + PROTECT-only”（并给出恢复路径）

---

## 10. 已确认与待确认的关键问题

### 10.1 已确认

- v1 交付优先级：A Creator（1 channel）先 PLG 跑通 ✅
- 订阅/结算：Shopify（source of truth）✅
- Backend：Vercel Functions（job fan-out）✅
- fan-out 派发方式：TiDB `job_tasks` 任务表 ✅
- worker 模式：每分钟 tick + claim N + 顺序处理（简单版）✅

### 10.2 待确认

（暂无）

---

## 附：与现有设计文档的关系

- 算法/规则/学习闸门的细节（signals、方向规则、replay gate、Chat 模板）以 `docs/design/Summarize.md` 为准；本文负责“工程化落地与对客技术栈”。

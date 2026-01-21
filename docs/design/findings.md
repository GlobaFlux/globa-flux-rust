# Findings & Decisions

## Requirements
- 用户希望：从商业、技术、市场、规模化角度补齐 `Summarize.md` 作为“可商用 Agent”需要补充的点。
- 用户补充：对客 Web 使用 Shopify Hydrogen；数据库使用 TiDB Serverless；必要时使用 Vercel Rust Functions 承载后端/Jobs。
- 用户确认：同时使用 Shopify 做订阅/结算（source of truth）。
- 用户确认：后端尽量全部跑在 Vercel Functions（job fan-out）。
- 用户选择：fan-out 派发用 TiDB 任务表（不引入外部队列）。
- 用户选择：worker 用简单模式（每分钟 tick，claim N 条任务顺序处理）。
- ICP 优先级确认：A 单创作者 + B 团队/MCN（都要覆盖）。
- v1 主线：优先 A Creator（自助付费转化 / PLG）✅（用户确认）。
- 约束：当前工作目录可写为 `docs/design/`（规划文件放在此目录）。

## Research Findings
- `docs/design/` 内不存在 `Summary.md`，只有 `Summarize.md`（364 行）。
- `Summarize.md` 主要覆盖：目标、分层架构、数据表、决策规则、Chat 约束、受控学习与 replay gate、监控、路线图、验收标准；但缺“商用工程化/商业化/市场与规模化运营”细节。
- 已在 `Summarize.md` 追加商用补齐章节（第 12–15 节）：商业化与商用边界、GTM、商用工程化（多租户/安全/SLO/可观测）、Scale（队列化/多频道运营/交付规模化）。
- 为了将工程化落地与对客技术栈表述清晰，新增 `docs/design/Technical_PRD.md` 作为可执行技术 PRD（保留 `Summarize.md` 为算法/规则主线参考）。

## Technical Decisions
| Decision | Rationale |
|----------|-----------|
| 新增 `Technical_PRD.md`（不重写 `Summarize.md`） | 保持算法/规则主线稳定，把工程化落地层（Web/API/Jobs/SLO/计费）单独成文 |
| 对客 Web：Shopify Hydrogen | 与用户选型一致；适合 SSR/对客体验与后续计费/电商整合 |
| DB：TiDB Serverless | 与现有表设计一致；作为唯一真相源（事实/决策/学习/商用） |
| 后端/Jobs：Vercel Functions（job fan-out） | 通过“调度器 + 单频道 worker”拆分任务，避免单次函数超时；Rust 仅作为极端情况下的兜底 |
| fan-out 派发：TiDB `job_tasks` 任务表 | 依赖最少、可重放；通过幂等键 + claim/lock + backoff 字段实现重试与并发控制 |
| worker 模式：每分钟 tick + claim N + 顺序处理 | 依赖最少、实现最快；先满足 A Creator v1，后续再按规模做并行与队列化升级 |
| v1 优先 A Creator（PLG） | 先把单创作者闭环跑通（OAuth→TTV→trial→paid），再扩展 B 的 RBAC/Portfolio/批量能力 |
| Billing：Shopify（source of truth） | 付费状态统一由 Shopify 驱动；后端通过 webhook 验签与幂等落库，更新本地 subscriptions/entitlements |
| 以“商用补齐清单”驱动文档增补 | 商用落地依赖：多租户/安全/成本/SLO/交付与合规 |
| 先写通用可配置版 ICP/定价/运营 | 用户尚未确认 ICP，但文档可先提供可选项与护栏 |
| A 的转化主线选 Hybrid→Free Trial | 先低摩擦交付 1 次价值，再用试用期培养习惯并转化 |
| A 试用期不绑定支付方式（no-CC trial） | 降低试用摩擦；通过限额/资格/风控在不绑卡前提下控成本与防滥用 |
| A 试用期 30 天（no-CC） | 更长窗口建立习惯与证明价值；通过阶梯权益+限额+触达提升转化并控成本 |
| 阶梯权益：前 7 天高、后 23 天严 | 兼顾激活与成本：前 7 天让用户更密集体验价值，后 23 天用限额与提示促进升级 |
| A 试用可变成本上限：$5 / trial | 约束 `budget_usd_per_day` 与限额设计，确保 30 天 no-CC 试用可规模化 |
| Trial 配额默认值（v1） | Boost（Day1–7）：10 次/天 + `$0.45/day`；Sustain（Day8–30）：3 次/天 + `$0.08/day` |
| A Creator 定价：$19/月 | PLG 入门价；配合 30 天 no-CC trial 与限额控成本，先验证转化再做年付/提价实验 |
| A Creator 年付：$190/年（不默认） | 抓高意愿用户、提高现金回收；默认仍展示月付以降低决策负担 |
| 年付退款政策（v1）：不退款 | 不做主动退款（除法律强制），减少争议与运营成本；用 30 天试用降低“买错”风险 |

## Issues Encountered
| Issue | Resolution |
|-------|------------|
|       |            |

## Resources
- `docs/design/Summarize.md`
- `docs/design/Technical_PRD.md`
- planning-with-files 模板：`~/.codex/skills/planning-with-files/templates/`

## Visual/Browser Findings
- 本次无

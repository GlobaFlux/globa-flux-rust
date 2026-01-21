# Task Plan: Package into a Technical PRD (Hydrogen + TiDB + Vercel)

## Goal
基于 `Summarize.md` 产出新的 `Technical_PRD.md`，将系统设计整理成“可直接开工”的技术 PRD，并明确对客与基础设施选型：Shopify Hydrogen（对客 Web）、TiDB Serverless（数据库）、Vercel Functions（job fan-out；Rust 仅兜底）。

## Current Phase
Complete

## Deliverable
- `docs/design/Technical_PRD.md`

## Phases

### Phase 1: Requirements & Discovery
- [x] 确认对客技术栈（Hydrogen）与 DB（TiDB Serverless）
- [x] 确认后端/Jobs：Vercel Functions（job fan-out）
- [x] 盘点 `Summarize.md` 可复用内容与需要补齐的工程化章节
- **Status:** complete

### Phase 2: Planning & Structure
- [x] 决定新增文档（避免打乱 `Summarize.md` 的算法/规则主线）
- [x] 定义 PRD 章节结构：目标/范围/架构/数据/API/Jobs/SLO/计费/风险/待确认问题
- **Status:** complete

### Phase 3: Implementation
- [x] 新增 `Technical_PRD.md` 并写入：Hydrogen + TiDB + Vercel 的可落地方案
- **Status:** complete

### Phase 4: Testing & Verification
- [x] 交叉检查：PRD 不改变受控学习与 replay gate 的原则，仅补齐工程化落地层
- [x] 检查可执行性：API/表/Job/鉴权/降级均给出最小闭环
- **Status:** complete

### Phase 5: Delivery
- [x] 输出 `Technical_PRD.md`
- [x] 给出需要用户确认的关键问题（Hydrogen 是否绑定 Shopify 计费、Backend 形态、v1 ICP 优先级）
- **Status:** complete

## Key Questions
1. 计费/订阅：已确认使用 Shopify（source of truth）✅
2. Backend：已确认 Vercel Functions（job fan-out）✅；fan-out 派发方式：TiDB `job_tasks` ✅；worker 模式：每分钟 tick + claim N + 顺序处理 ✅
3. v1 交付优先级：已确认优先 A Creator（PLG）✅

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| 新增 `Technical_PRD.md` 而不重写 `Summarize.md` | 保持算法/规则主线稳定，把工程化落地层单独成文 |
| 对客 Web 使用 Shopify Hydrogen | 用户确认的技术选型，适合 SSR/对客体验与后续电商/计费整合 |
| 数据库使用 TiDB Serverless | 与现有表设计一致，作为唯一真相源（事实/决策/学习/商用） |
| Backend 使用 Vercel Functions（job fan-out） | 通过“调度器 + 单频道 worker”拆分任务，避免单次函数超时；更符合 v1 轻量交付 |
| fan-out 派发使用 TiDB `job_tasks` | 依赖最少；通过幂等键与任务 claim/lock/backoff 实现重试与并发控制 |
| worker 模式：每分钟 tick + claim N + 顺序处理 | 最简单的可靠实现：每分钟处理一批任务，接近超时预算就退出，下一分钟继续 |
| v1 优先 A Creator（PLG） | 先跑通单创作者闭环（OAuth→TTV→trial→paid），再扩展 B（MCN）的 Portfolio/RBAC/批量运营能力 |
| A 试用可变成本上限：$5 / trial | 指导 Trial 的 `budget_usd_per_day` 与限额（Boost 0.45 / Sustain 0.08；30 天总计≈$4.99） |
| Trial 默认配额：Boost 10 / Sustain 3 | 与 `$5 / trial` 成本上限匹配，并保证前 7 天体验强、后 23 天促升级 |
| A Creator 定价：$19/mo | 作为 PLG 的入门价，先验证 trial→paid 与留存，再做年付/提价实验 |
| A Creator 年付：$190/yr（不默认） | 年付作为可选切换抓高意愿用户，默认仍展示月付以降低决策成本 |
| 年付退款政策（v1）：不退款 | 不做主动退款（除法律强制），用 30 天试用降低购买风险并减少运营摩擦 |
| Billing 使用 Shopify（source of truth） | 付费状态由 Shopify 驱动；通过 webhook 验签/幂等落库更新本地 subscriptions/entitlements |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| `zsh: command not found`（反引号被当作命令替换） | 1 | shell 命令里避免未转义的反引号；用单引号/转义反引号 |

## Notes
- 算法/规则/学习闸门细节以 `docs/design/Summarize.md` 为准；本文 PRD 负责“工程化落地与对客技术栈”。
- 遵循 planning-with-files：重要结论写入 findings.md，进度写入 progress.md

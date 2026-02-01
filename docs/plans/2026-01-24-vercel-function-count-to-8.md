# Reduce Vercel Rust API Functions to 8 (Hobby Plan) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task.

**Goal:** Reduce `globa-flux-rust/api/**/*.rs` function entrypoints from 12 → 8 while keeping all existing public/internal API routes working via `vercel.json` rewrites.

**Architecture:** Consolidate endpoints by adding an `action=` query param router into existing functions (so we don’t add new API entrypoint files):
- `jobs`: reuse `/api/jobs/worker/tick` for both `tick` (default) and `dispatch` (`action=dispatch`)
- `tenants`: reuse `/api/tenants/llm_settings` for both `llm_settings` (default) and `ensure_trial` (`action=ensure_trial`)
- `billing`: reuse `/api/webhooks/billing` for both webhook ingest (default) and subscription status (`action=subscription_status`)
- `geo`: merge `geo_monitor_dispatch` into `geo_monitor`, selecting dispatch mode via `op=dispatch` (query param) to preserve the existing dispatch request body

**Tech Stack:** Rust, `vercel_runtime`, `hyper`, `serde`, `sqlx`, `vercel.json` rewrites.

---

## Task 1: Jobs router (dispatch + tick)

**Files:**
- Modify: `globa-flux-rust/api/jobs/worker/tick.rs`
- Delete: `globa-flux-rust/api/jobs/daily/dispatch.rs`
- Modify: `globa-flux-rust/Cargo.toml`
- Modify: `globa-flux-rust/vercel.json`

**Step 1: Add router entrypoint**
- Add `handle_dispatch(...)` into `tick.rs` and route by query param:
  - default (no `action`) → `handle_tick`
  - `action=dispatch` → `handle_dispatch`

**Step 2: Update rewrites**
- Rewrite `/api/jobs/daily/dispatch` → `/api/jobs/worker/tick?action=dispatch`
- Rewrite `/api/jobs/weekly/dispatch` → `/api/jobs/worker/tick?action=dispatch&schedule=weekly`

**Step 3: Targeted tests**
- Add tests that hit the “unauthorized” fast path for both actions (no DB required).
- Run: `cargo test --bin jobs_worker_tick -- --nocapture`

---

## Task 2: Tenants router (ensure_trial + llm_settings)

**Files:**
- Delete: `globa-flux-rust/api/tenants/ensure_trial.rs`
- Modify: `globa-flux-rust/api/tenants/llm_settings.rs`
- Modify: `globa-flux-rust/Cargo.toml`
- Modify: `globa-flux-rust/vercel.json`

**Step 1: Add router entrypoint**
- Add `handle_ensure_trial(...)` into `llm_settings.rs` and route by query param:
  - default (no `action`) → existing `llm_settings` behavior
  - `action=ensure_trial` → `handle_ensure_trial(...)`

**Step 2: Update rewrites**
- Rewrite `/api/tenants/ensure_trial` → `/api/tenants/llm_settings?action=ensure_trial`

**Step 3: Targeted tests**
- Unauthorized fast-path tests for both actions.
- Run: `cargo test --bin tenants_llm_settings -- --nocapture`

---

## Task 3: Billing router (webhook ingest + subscription status)

**Files:**
- Modify: `globa-flux-rust/api/webhooks/billing.rs`
- Delete: `globa-flux-rust/api/billing/subscription/status.rs`
- Modify: `globa-flux-rust/Cargo.toml`
- Modify: `globa-flux-rust/vercel.json`

**Step 1: Add router entrypoint**
- Add `handle_subscription_status(...)` into `webhooks/billing.rs` and route by query param:
  - default (no `action`) → webhook ingest
  - `action=subscription_status` → `handle_subscription_status(...)`

**Step 2: Update rewrites**
- Rewrite `/api/billing/subscription/status` → `/api/webhooks/billing?action=subscription_status`

**Step 3: Targeted tests**
- Unauthorized fast-path tests for both actions.
- Run: `cargo test --bin webhooks_billing -- --nocapture`

---

## Task 4: Merge geo monitor dispatch into geo monitor

**Files:**
- Modify: `globa-flux-rust/api/geo_monitor.rs`
- Delete: `globa-flux-rust/api/geo_monitor_dispatch.rs`
- Modify: `globa-flux-rust/Cargo.toml`
- Modify: `globa-flux-rust/vercel.json`

**Step 1: Add dispatch mode**
- Accept `op=dispatch` from query string (so the old dispatch body keeps working).
- Reuse the existing dispatch logic from `geo_monitor_dispatch.rs` inside a new `dispatch` branch.

**Step 2: Rewrite old route**
- Rewrite `/api/geo_monitor_dispatch` → `/api/geo_monitor?op=dispatch`

---

## Task 5: Verification

**Step 1: Confirm function count**
- Run: `find globa-flux-rust/api -name '*.rs' | wc -l`
- Expected: `8`

**Step 2: Compile/test (targeted)**
- Run: `cargo test --bin oauth_youtube_router -- --nocapture`
- Run: `cargo test --bin jobs_worker_tick -- --nocapture`
- Run: `cargo test --bin tenants_llm_settings -- --nocapture`
- Run: `cargo test --bin webhooks_billing -- --nocapture`
- Run: `cargo test --bin geo_monitor -- --nocapture`

> Note: `cargo test` for the whole crate may fail in sandboxed environments due to restricted loopback binds; stick to bin-scoped tests for routing/auth fast paths.

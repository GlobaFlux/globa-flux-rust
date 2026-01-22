# YouTube → TiDB Backfill (v1) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** After YouTube OAuth connect, automatically enqueue a historical backfill so `video_daily_metrics` in TiDB Serverless is populated for the last N days (default 12 weeks), and is kept fresh by existing daily jobs.

**Architecture:** Reuse the existing `job_tasks` fan-out and the existing `daily_channel` worker path. Implement a pure helper that computes the sequence of `run_for_dt` values needed to cover the backfill range with 7-day chunks (possible small overshoot for non-multiple day counts).

**Tech Stack:** Rust, `sqlx` (MySQL/TiDB), Vercel Rust Functions, YouTube Analytics API.

---

## Task 1: Add pure backfill scheduling helper (unit-tested)

**Files:**
- Create: `globa-flux-rust/src/backfill.rs`
- Modify: `globa-flux-rust/src/lib.rs`

**Step 1: Write failing tests (RED)**

Add tests for:
- 14-day backfill yields 2 weekly chunks
- 10-day backfill yields 2 weekly chunks (overshoot ok)
- returned dates are sorted and unique

Run: `cargo test backfill -- --nocapture`  
Expected: FAIL (module/functions missing).

**Step 2: Implement helper (GREEN)**

Implement `compute_backfill_run_for_dates(end_dt, backfill_days, chunk_days) -> Vec<NaiveDate>`.

**Step 3: Re-run tests (GREEN verify)**

Run: `cargo test backfill -- --nocapture`  
Expected: PASS.

---

## Task 2: Enqueue backfill jobs on OAuth exchange (no new DB schema)

**Files:**
- Modify: `globa-flux-rust/api/oauth/youtube/exchange.rs`

**Step 1: Decide defaults + env overrides**

Use env var (safe defaults):
- `YOUTUBE_BACKFILL_DAYS` (default `84`, clamp `7..=365`)

Use `chunk_days=7` (matches `daily_channel` 7-day fetch window).

**Step 2: Enqueue `daily_channel` tasks**

For each computed `run_for_dt`:
- `dedupe_key = "{tenant_id}:daily_channel:{channel_id}:{run_for_dt}"`
- `INSERT ... ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3)`

Return `backfill_enqueued` count in JSON response (additive field).

**Step 3: Verify compile/tests**

Run: `cargo test oauth_youtube_exchange -- --nocapture`  
Expected: PASS.

---

## Task 3: (Optional) Add manual backfill endpoint

**Files:**
- Create: `globa-flux-rust/api/jobs/backfill/dispatch.rs`

Allow internal callers to trigger backfill for a tenant/channel without redoing OAuth.


# Weekly Replay Gate (v1) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a weekly replay-gate evaluation that computes safety/stability metrics from historical `decision_daily` + `decision_outcome` and stores a `policy_eval_report` for the generated candidate policy version.

**Architecture:** Keep the gate math pure/testable (`src/replay_gate.rs`), and keep DB I/O in `src/db.rs` + orchestration in `api/jobs/worker/tick.rs` (`weekly_channel` branch). Default behavior is “compute & store report”; optional auto-activate is behind an env flag.

**Tech Stack:** Rust, `sqlx` (MySQL/TiDB), Vercel Rust Functions.

---

## Task 1: Add replay-gate metrics module (pure + unit-tested)

**Files:**
- Create: `globa-flux-rust/src/replay_gate.rs`
- Modify: `globa-flux-rust/src/lib.rs`
- Test: `globa-flux-rust/src/replay_gate.rs` (unit tests in-module)

**Step 1: Write failing unit tests (RED)**

Add tests that assert:
- `switch_rate` is `0` when directions constant
- `switch_rate` counts direction changes correctly
- `protect_rate` counts PROTECT days correctly
- `catastrophic_rate` counts catastrophic days when direction != PROTECT

Run: `cargo test -p globa-flux-rust replay_gate -- --nocapture`  
Expected: FAIL (module/functions missing).

**Step 2: Implement minimal metrics (GREEN)**

Implement:
- `ReplayDecision { as_of_dt, direction }`
- `ReplayOutcome { decision_dt, catastrophic_flag }`
- `ReplayGateMetrics { days, protect_rate, switch_rate, catastrophic_rate }`
- `compute_metrics(decisions, outcomes_by_dt) -> ReplayGateMetrics`

**Step 3: Re-run tests (GREEN verify)**

Run: `cargo test -p globa-flux-rust replay_gate -- --nocapture`  
Expected: PASS.

**Step 4: Export module**

In `globa-flux-rust/src/lib.rs`, export `pub mod replay_gate;`.

---

## Task 2: Add DB fetch helpers for replay window

**Files:**
- Modify: `globa-flux-rust/src/db.rs`

**Step 1: Add typed rows**

Add:
- `DecisionDailyRowLite { as_of_dt, direction, confidence }`
- `DecisionOutcomeRowLite { decision_dt, outcome_dt, catastrophic_flag }`

**Step 2: Add fetch functions**

Add:
- `fetch_decision_daily_range(pool, tenant_id, channel_id, start_dt, end_dt)`
- `fetch_decision_outcome_range(pool, tenant_id, channel_id, start_dt, end_dt)`

**Step 3: (Optional) spot-check compile**

Run: `cargo test -p globa-flux-rust`  
Expected: PASS (no DB required for compile).

---

## Task 3: Implement weekly job replay gate evaluation

**Files:**
- Modify: `globa-flux-rust/api/jobs/worker/tick.rs`

**Step 1: Decide replay window and minimum data**

Use env vars (with safe defaults):
- `REPLAY_GATE_EVAL_DAYS` (default `56`)
- `REPLAY_GATE_MIN_DAYS` (default `14`)
- `REPLAY_GATE_AUTO_ACTIVATE` (default `0`)

Derive:
- `eval_end = run_for_dt - 7 days` (needs outcomes)
- `eval_start = eval_end - (eval_days-1)`

**Step 2: Fetch decisions + outcomes**

- Load `decision_daily` rows in `[eval_start, eval_end]`.
- Load `decision_outcome` rows in `[eval_start, eval_end]` (by `decision_dt`).
- Build `outcomes_by_decision_dt` for the metrics module.

**Step 3: Compute metrics & store report**

- Candidate version remains `candidate-{run_for_dt}` (as existing code).
- Compute metrics from historical decisions/outcomes and store:
  - `replay_metrics_json` should include window, counts, rates, and a note that v1 uses recorded decisions (training/simulation TBD).
- Compute `approved` using conservative checks (documented in code):
  - enough days (`>= REPLAY_GATE_MIN_DAYS`)
  - catastrophic rate not worse than baseline (v1 baseline can be same)
  - switch rate not worse

**Step 4: Optional auto-activate**

If `REPLAY_GATE_AUTO_ACTIVATE=1` and `approved=true`:
- Copy current `"active"` params to `active-prev-{candidate_version}` (idempotent).
- Upsert `"active"` params_json to candidate params_json.

**Step 5: Run tests**

Run: `cargo test -p globa-flux-rust`  
Expected: PASS.

---

## Task 4: Update documentation (optional but recommended)

**Files:**
- Modify: `globa-flux-rust/README.md`

Update the “stub handler” note to reflect current reality and point to the weekly replay gate as the remaining v1 learning scaffold.


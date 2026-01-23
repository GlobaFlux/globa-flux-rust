# Gemini DB-only Global Model Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enforce a single Gemini model sourced only from `tenant_llm_settings` with `tenant_id="global"`, remove env defaults/allowlists, and eliminate OpenAI fallback.

**Architecture:** Keep Gemini runtime config in Rust, but source model from the DB-only global record. The LLM settings API accepts only `tenant_id="global"`. Risk-check requests short-circuit with `config_error` if Gemini key/model is missing.

**Tech Stack:** Rust, Vercel Serverless Functions, sqlx (MySQL), hyper, serde

---

### Task 1: Add failing tests for DB-only global model behavior

**Files:**
- Modify: `src/providers/gemini.rs`
- Modify: `api/tenants/llm_settings.rs`
- Modify: `api/chat/risk_check.rs`

**Step 1: Write failing test for `GeminiConfig::from_env_optional` (ignores GEMINI_MODEL)**

```rust
#[test]
fn from_env_optional_ignores_gemini_model_env() {
    std::env::set_var("GEMINI_API_KEY", "k");
    std::env::set_var("GEMINI_MODEL", "gemini-2.5-flash");

    let cfg = GeminiConfig::from_env_optional().unwrap().unwrap();
    assert_eq!(cfg.model, "");

    std::env::remove_var("GEMINI_API_KEY");
    std::env::remove_var("GEMINI_MODEL");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test providers::gemini::tests::from_env_optional_ignores_gemini_model_env`
Expected: FAIL (model is not empty)

**Step 3: Write failing test for LLM settings global enforcement**

```rust
#[tokio::test]
async fn rejects_non_global_tenant_id_even_when_tidb_missing() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
    std::env::remove_var("TIDB_DATABASE_URL");
    std::env::remove_var("DATABASE_URL");

    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer secret".parse().unwrap());
    let uri: hyper::Uri = "/api/tenants/llm_settings?tenant_id=foo".parse().unwrap();

    let response = handle_llm_settings(&Method::GET, &headers, &uri, Bytes::new())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
```

**Step 4: Run test to verify it fails**

Run: `cargo test tenants::llm_settings::tests::rejects_non_global_tenant_id_even_when_tidb_missing`
Expected: FAIL (currently returns NOT_IMPLEMENTED)

**Step 5: Write failing test for risk_check GEMINI_API_KEY config error**

```rust
#[tokio::test]
async fn returns_config_error_when_gemini_key_missing() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
    std::env::remove_var("GEMINI_API_KEY");
    std::env::remove_var("TIDB_DATABASE_URL");
    std::env::remove_var("DATABASE_URL");

    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer secret".parse().unwrap());
    headers.insert("content-type", "application/json".parse().unwrap());
    headers.insert("x-idempotency-key", "k1".parse().unwrap());

    let uri: hyper::Uri = "/api/chat/risk_check".parse().unwrap();
    let body = Bytes::from(r#"{"request_id":"r1","tenant_id":"t1","trial_started_at_ms":0,"budget_usd_per_day":1,"action_type":"change_title"}"#);

    let response = handle_risk_check(&Method::POST, &headers, &uri, body)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
```

**Step 6: Run test to verify it fails**

Run: `cargo test chat::risk_check::tests::returns_config_error_when_gemini_key_missing`
Expected: FAIL (no handle_risk_check or returns error)

**Step 7: Commit**

```bash
git add src/providers/gemini.rs api/tenants/llm_settings.rs api/chat/risk_check.rs
git commit -m "test: add failing tests for db-only global gemini"
```

---

### Task 2: Implement GeminiConfig env changes

**Files:**
- Modify: `src/providers/gemini.rs`

**Step 1: Implement minimal change**
- Stop reading `GEMINI_MODEL` env.
- Set `GeminiConfig.model` to empty string.

**Step 2: Run targeted test**

Run: `cargo test providers::gemini::tests::from_env_optional_ignores_gemini_model_env`
Expected: PASS

**Step 3: Commit**

```bash
git add src/providers/gemini.rs
git commit -m "fix: remove GEMINI_MODEL env default"
```

---

### Task 3: Enforce global tenant_id and remove allowlist in llm_settings

**Files:**
- Modify: `api/tenants/llm_settings.rs`

**Step 1: Implement minimal change**
- Reject any `tenant_id` != "global" with BAD_REQUEST.
- Remove allowlist/default handling and related response fields.

**Step 2: Run targeted test**

Run: `cargo test tenants::llm_settings::tests::rejects_non_global_tenant_id_even_when_tidb_missing`
Expected: PASS

**Step 3: Commit**

```bash
git add api/tenants/llm_settings.rs
git commit -m "fix: enforce global gemini model setting"
```

---

### Task 4: Enforce DB-only global model in risk_check (Gemini-only)

**Files:**
- Modify: `api/chat/risk_check.rs`

**Step 1: Implement minimal change**
- Add `handle_risk_check` that accepts method/headers/uri/body.
- Check `GEMINI_API_KEY` first; if missing, return `config_error`.
- Fetch `tenant_llm_settings` using fixed key `global` and require non-empty model.
- Remove OpenAI fallback path.

**Step 2: Run targeted test**

Run: `cargo test chat::risk_check::tests::returns_config_error_when_gemini_key_missing`
Expected: PASS

**Step 3: Commit**

```bash
git add api/chat/risk_check.rs
git commit -m "fix: require global gemini model in risk_check"
```

---

### Task 5: Update documentation

**Files:**
- Modify: `README.md`

**Step 1: Update env list and add global model setup note**

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document global gemini model setting"
```

---

### Task 6: Verify

**Step 1: Run targeted tests**

Run: `cargo test providers::gemini::tests::from_env_optional_ignores_gemini_model_env tenants::llm_settings::tests::rejects_non_global_tenant_id_even_when_tidb_missing chat::risk_check::tests::returns_config_error_when_gemini_key_missing`
Expected: PASS

**Step 2: (Optional) Run full test suite**

Run: `cargo test`
Expected: Existing youtube_api test may fail with PermissionDenied (pre-existing)

**Step 3: Commit final (if needed)**

```bash
git status --porcelain
```

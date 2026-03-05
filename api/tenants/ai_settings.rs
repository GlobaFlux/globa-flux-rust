use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode};
use serde::Deserialize;
use serde_json::Value;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::db::{
    ensure_trial_started, fetch_tenant_ai_provider_setting, fetch_tenant_ai_provider_settings,
    fetch_tenant_ai_routing_policy, get_pool, insert_tenant_ai_provider_audit,
    set_tenant_ai_provider_status, update_tenant_ai_provider_test_status, upsert_tenant_ai_provider_setting,
    upsert_tenant_ai_routing_policy,
};
use globa_flux_rust::providers::gemini::{generate_text as gemini_generate_text, GeminiConfig};
use globa_flux_rust::secrets::{decrypt_secret, encrypt_secret};

fn bearer_token(header_value: Option<&str>) -> Option<&str> {
    let value = header_value?;
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
}

fn json_response(
    status: StatusCode,
    value: serde_json::Value,
) -> Result<Response<ResponseBody>, Error> {
    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8")
        .body(ResponseBody::from(value))?)
}

fn has_tidb_url() -> bool {
    std::env::var("TIDB_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map(|v| !v.is_empty())
        .unwrap_or(false)
}

fn query_param(query: Option<&str>, key: &str) -> Option<String> {
    let q = query?;
    for pair in q.split('&') {
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("");
        let v = it.next().unwrap_or("");
        if k == key {
            return Some(v.replace('+', " "));
        }
    }
    None
}

fn normalize_provider(provider: &str) -> String {
    provider.trim().to_ascii_lowercase()
}

fn is_supported_provider(provider: &str) -> bool {
    matches!(provider, "openai" | "anthropic" | "gemini" | "bedrock")
}

fn normalize_status(status: Option<&str>) -> String {
    let v = status.unwrap_or("active").trim().to_ascii_lowercase();
    match v.as_str() {
        "active" | "inactive" | "revoked" | "error" => v,
        _ => "active".to_string(),
    }
}

fn mask_key_hint(fingerprint: &str) -> String {
    let fp = fingerprint.trim();
    if fp.len() <= 10 {
        return format!("fp:{fp}");
    }
    format!("fp:{}...{}", &fp[..6], &fp[fp.len().saturating_sub(4)..])
}

fn trim_or_none(value: Option<&str>) -> Option<String> {
    let v = value?.trim();
    if v.is_empty() {
        None
    } else {
        Some(v.to_string())
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut out = String::new();
    for (idx, ch) in value.chars().enumerate() {
        if idx >= max_chars {
            break;
        }
        out.push(ch);
    }
    out
}

fn provider_v1_endpoint(base_url: &str, path: &str) -> String {
    let trimmed = base_url.trim().trim_end_matches('/');
    if trimmed.ends_with("/v1") {
        format!("{trimmed}/{path}")
    } else {
        format!("{trimmed}/v1/{path}")
    }
}

fn provider_error_message(json: &Value) -> Option<String> {
    if let Some(msg) = json
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|v| v.as_str())
    {
        return Some(msg.to_string());
    }
    if let Some(msg) = json.get("error").and_then(|v| v.as_str()) {
        return Some(msg.to_string());
    }
    if let Some(msg) = json.get("message").and_then(|v| v.as_str()) {
        return Some(msg.to_string());
    }
    None
}

async fn openai_test_connection(api_key: &str, model: &str) -> Result<(), Error> {
    let api_base_url = std::env::var("OPENAI_API_BASE_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "https://api.openai.com/v1".to_string());
    let url = provider_v1_endpoint(&api_base_url, "responses");
    let payload = serde_json::json!({
      "model": model,
      "temperature": 0.0,
      "max_output_tokens": 8,
      "input": [
        {
          "role": "system",
          "content": [{"type":"input_text","text":"You are a connectivity checker. Reply with OK."}]
        },
        {
          "role": "user",
          "content": [{"type":"input_text","text":"Ping"}]
        }
      ]
    });

    let resp = reqwest::Client::new()
        .post(url)
        .bearer_auth(api_key)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(reqwest::header::ACCEPT, "application/json")
        .json(&payload)
        .send()
        .await
        .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?;
    if !status.is_success() {
        let message = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|v| provider_error_message(&v))
            .unwrap_or_else(|| truncate_chars(body.trim(), 300));
        return Err(Box::new(std::io::Error::other(format!(
            "OpenAI connectivity test failed (status {}): {message}",
            status.as_u16()
        ))));
    }
    Ok(())
}

async fn anthropic_test_connection(api_key: &str, model: &str) -> Result<(), Error> {
    let api_base_url = std::env::var("ANTHROPIC_API_BASE_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "https://api.anthropic.com/v1".to_string());
    let url = provider_v1_endpoint(&api_base_url, "messages");
    let payload = serde_json::json!({
      "model": model,
      "system": "You are a connectivity checker. Reply with OK.",
      "max_tokens": 8,
      "temperature": 0.0,
      "messages": [{"role":"user","content":"Ping"}]
    });

    let resp = reqwest::Client::new()
        .post(url)
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(reqwest::header::ACCEPT, "application/json")
        .json(&payload)
        .send()
        .await
        .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?;
    if !status.is_success() {
        let message = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|v| provider_error_message(&v))
            .unwrap_or_else(|| truncate_chars(body.trim(), 300));
        return Err(Box::new(std::io::Error::other(format!(
            "Anthropic connectivity test failed (status {}): {message}",
            status.as_u16()
        ))));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct SecretMaterial {
    encrypted_api_key: String,
    encrypted_dek: Option<String>,
    key_version: String,
    key_fingerprint: String,
}

fn resolve_secret_material(
    api_key_plaintext: &str,
    existing: Option<&SecretMaterial>,
) -> Result<SecretMaterial, Error> {
    let api_key_plaintext = api_key_plaintext.trim();
    if !api_key_plaintext.is_empty() {
        let encrypted = encrypt_secret(api_key_plaintext)?;
        return Ok(SecretMaterial {
            encrypted_api_key: encrypted.ciphertext,
            encrypted_dek: None,
            key_version: encrypted.key_version,
            key_fingerprint: encrypted.fingerprint,
        });
    }

    if let Some(existing) = existing {
        return Ok(existing.clone());
    }

    Err(Box::new(std::io::Error::other(
        "api_key_plaintext is required for new provider setting",
    )))
}

fn row_to_audit_json(row: &globa_flux_rust::db::TenantAiProviderSettingRow) -> serde_json::Value {
    serde_json::json!({
      "tenant_id": row.tenant_id,
      "provider": row.provider,
      "status": row.status,
      "default_model": row.default_model,
      "model_allowlist_json": row.model_allowlist_json,
      "key_version": row.key_version,
      "key_fingerprint": row.key_fingerprint,
      "last_test_status": row.last_test_status,
      "updated_by": row.updated_by,
      "updated_at": row.updated_at,
    })
}

#[derive(Deserialize)]
struct UpsertProviderRequest {
    tenant_id: String,
    provider: String,
    api_key_plaintext: String,
    default_model: String,
    #[serde(default)]
    model_allowlist: Option<Vec<String>>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    updated_by: Option<String>,
}

#[derive(Deserialize)]
struct ProviderActionRequest {
    tenant_id: String,
    provider: String,
    #[serde(default)]
    updated_by: Option<String>,
}

#[derive(Deserialize)]
struct RotateProviderRequest {
    tenant_id: String,
    provider: String,
    new_api_key_plaintext: String,
    #[serde(default)]
    updated_by: Option<String>,
}

#[derive(Deserialize)]
struct RoutingPolicyRequest {
    tenant_id: String,
    default_provider: String,
    #[serde(default)]
    monthly_budget_usd: Option<f64>,
    #[serde(default)]
    updated_by: Option<String>,
}

#[derive(Deserialize)]
struct EnsureTrialRequest {
    tenant_id: String,
    now_ms: i64,
}

async fn handle_query(
    headers: &HeaderMap,
    uri: &hyper::Uri,
) -> Result<Response<ResponseBody>, Error> {
    let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
    let provided =
        bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

    if expected.is_empty() || provided != expected {
        return json_response(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"ok": false, "error": "unauthorized"}),
        );
    }

    let tenant_id = query_param(uri.query(), "tenant_id").unwrap_or_default();
    if tenant_id.trim().is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
        );
    }

    if !has_tidb_url() {
        return json_response(
            StatusCode::NOT_IMPLEMENTED,
            serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
        );
    }

    let pool = get_pool().await?;
    let settings = fetch_tenant_ai_provider_settings(pool, tenant_id.trim()).await?;
    let policy = fetch_tenant_ai_routing_policy(pool, tenant_id.trim()).await?;

    let settings_json = settings
        .into_iter()
        .map(|row| {
            serde_json::json!({
              "tenant_id": row.tenant_id,
              "provider": row.provider,
              "status": row.status,
              "default_model": row.default_model,
              "model_allowlist_json": row.model_allowlist_json,
              "key_version": row.key_version,
              "key_hint": mask_key_hint(&row.key_fingerprint),
              "last_test_status": row.last_test_status,
              "last_test_error": row.last_test_error,
              "last_test_at": row.last_test_at,
              "created_by": row.created_by,
              "updated_by": row.updated_by,
              "created_at": row.created_at,
              "updated_at": row.updated_at
            })
        })
        .collect::<Vec<_>>();

    let policy_json = policy.map(|p| {
        serde_json::json!({
          "tenant_id": p.tenant_id,
          "default_provider": p.default_provider,
          "monthly_budget_usd": p.monthly_budget_usd,
          "updated_by": p.updated_by,
          "updated_at": p.updated_at
        })
    });

    json_response(
        StatusCode::OK,
        serde_json::json!({
          "ok": true,
          "tenant_id": tenant_id.trim(),
          "settings": settings_json,
          "routing_policy": policy_json
        }),
    )
}

async fn handle_upsert(headers: &HeaderMap, body: Bytes) -> Result<Response<ResponseBody>, Error> {
    let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
    let provided =
        bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

    if expected.is_empty() || provided != expected {
        return json_response(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"ok": false, "error": "unauthorized"}),
        );
    }

    let parsed: UpsertProviderRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid json body: {e}")))
    })?;

    let tenant_id = parsed.tenant_id.trim().to_string();
    if tenant_id.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
        );
    }

    let provider = normalize_provider(&parsed.provider);
    if !is_supported_provider(&provider) {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "unsupported provider"}),
        );
    }

    let default_model = parsed.default_model.trim().to_string();
    if default_model.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "default_model is required"}),
        );
    }

    if !has_tidb_url() {
        return json_response(
            StatusCode::NOT_IMPLEMENTED,
            serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
        );
    }

    let updated_by =
        trim_or_none(parsed.updated_by.as_deref()).unwrap_or_else(|| "system".to_string());

    let pool = get_pool().await?;
    let before = fetch_tenant_ai_provider_setting(pool, &tenant_id, &provider).await?;
    let existing_secret = before.as_ref().map(|row| SecretMaterial {
        encrypted_api_key: row.encrypted_api_key.clone(),
        encrypted_dek: row.encrypted_dek.clone(),
        key_version: row.key_version.clone(),
        key_fingerprint: row.key_fingerprint.clone(),
    });
    let secret = resolve_secret_material(parsed.api_key_plaintext.trim(), existing_secret.as_ref())?;

    let status = if let Some(status_raw) = parsed.status.as_deref() {
        normalize_status(Some(status_raw))
    } else if let Some(before) = before.as_ref() {
        normalize_status(Some(before.status.as_str()))
    } else {
        "active".to_string()
    };

    let allowlist_json = if let Some(list) = parsed.model_allowlist.as_ref() {
        serde_json::to_string(list).ok()
    } else {
        before.as_ref().and_then(|row| row.model_allowlist_json.clone())
    };

    let created_by = before
        .as_ref()
        .map(|row| row.created_by.as_str())
        .unwrap_or(updated_by.as_str());

    upsert_tenant_ai_provider_setting(
        pool,
        &tenant_id,
        &provider,
        &status,
        &default_model,
        allowlist_json.as_deref(),
        &secret.encrypted_api_key,
        secret.encrypted_dek.as_deref(),
        &secret.key_version,
        &secret.key_fingerprint,
        created_by,
        &updated_by,
    )
    .await?;

    let after = fetch_tenant_ai_provider_setting(pool, &tenant_id, &provider).await?;
    let before_json = before.as_ref().map(row_to_audit_json);
    let after_json = after.as_ref().map(row_to_audit_json);
    let before_str = before_json.as_ref().map(|v| v.to_string());
    let after_str = after_json.as_ref().map(|v| v.to_string());

    insert_tenant_ai_provider_audit(
        pool,
        &tenant_id,
        &provider,
        "upsert",
        &updated_by,
        None,
        before_str.as_deref(),
        after_str.as_deref(),
    )
    .await?;

    json_response(
        StatusCode::OK,
        serde_json::json!({
          "ok": true,
          "tenant_id": tenant_id,
          "provider": provider,
          "status": status,
          "default_model": default_model,
          "key_version": secret.key_version,
          "key_hint": mask_key_hint(&secret.key_fingerprint)
        }),
    )
}

async fn handle_test_action(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
    let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
    let provided =
        bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

    if expected.is_empty() || provided != expected {
        return json_response(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"ok": false, "error": "unauthorized"}),
        );
    }

    let parsed: ProviderActionRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid json body: {e}")))
    })?;

    let tenant_id = parsed.tenant_id.trim().to_string();
    let provider = normalize_provider(&parsed.provider);
    if tenant_id.is_empty() || provider.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id and provider are required"}),
        );
    }

    if !is_supported_provider(&provider) {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "unsupported provider"}),
        );
    }

    if !has_tidb_url() {
        return json_response(
            StatusCode::NOT_IMPLEMENTED,
            serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
        );
    }

    let pool = get_pool().await?;
    let setting = fetch_tenant_ai_provider_setting(pool, &tenant_id, &provider).await?;
    let Some(setting) = setting else {
        return json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({"ok": false, "error": "not_found", "message": "provider setting not found"}),
        );
    };

    let api_key = decrypt_secret(&setting.encrypted_api_key, &setting.key_version)?;

    let result = match provider.as_str() {
        "gemini" => {
            let api_base_url = std::env::var("GEMINI_API_BASE_URL")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| "https://generativelanguage.googleapis.com/v1".to_string());
            let cfg = GeminiConfig {
                api_key,
                model: setting.default_model.clone(),
                api_base_url,
            };
            gemini_generate_text(
                &cfg,
                "You are a connectivity checker. Reply with OK.",
                "Ping",
                0.0,
                8,
            )
            .await
            .map(|_| ())
        }
        "openai" => openai_test_connection(&api_key, &setting.default_model).await,
        "anthropic" => anthropic_test_connection(&api_key, &setting.default_model).await,
        "bedrock" => Err(Box::new(std::io::Error::other(
            "test action is not implemented for bedrock yet",
        )) as Error),
        _ => Err(Box::new(std::io::Error::other("unsupported provider")) as Error),
    };

    match result {
        Ok(()) => {
            update_tenant_ai_provider_test_status(pool, &tenant_id, &provider, "ok", None).await?;
            json_response(
                StatusCode::OK,
                serde_json::json!({"ok": true, "tenant_id": tenant_id, "provider": provider, "test_status": "ok"}),
            )
        }
        Err(err) => {
            let msg = truncate_chars(&err.to_string(), 500);
            update_tenant_ai_provider_test_status(pool, &tenant_id, &provider, "error", Some(&msg))
                .await?;
            json_response(
                StatusCode::BAD_GATEWAY,
                serde_json::json!({"ok": false, "error": "provider_test_failed", "message": msg}),
            )
        }
    }
}

async fn handle_rotate_action(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
    let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
    let provided =
        bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

    if expected.is_empty() || provided != expected {
        return json_response(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"ok": false, "error": "unauthorized"}),
        );
    }

    let parsed: RotateProviderRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid json body: {e}")))
    })?;

    let tenant_id = parsed.tenant_id.trim().to_string();
    let provider = normalize_provider(&parsed.provider);
    if tenant_id.is_empty() || provider.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id and provider are required"}),
        );
    }
    if parsed.new_api_key_plaintext.trim().is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "new_api_key_plaintext is required"}),
        );
    }

    if !has_tidb_url() {
        return json_response(
            StatusCode::NOT_IMPLEMENTED,
            serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
        );
    }

    let updated_by =
        trim_or_none(parsed.updated_by.as_deref()).unwrap_or_else(|| "system".to_string());
    let pool = get_pool().await?;
    let before = fetch_tenant_ai_provider_setting(pool, &tenant_id, &provider).await?;
    let Some(before_row) = before.clone() else {
        return json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({"ok": false, "error": "not_found", "message": "provider setting not found"}),
        );
    };

    let encrypted = encrypt_secret(parsed.new_api_key_plaintext.trim())?;

    upsert_tenant_ai_provider_setting(
        pool,
        &tenant_id,
        &provider,
        &before_row.status,
        &before_row.default_model,
        before_row.model_allowlist_json.as_deref(),
        &encrypted.ciphertext,
        before_row.encrypted_dek.as_deref(),
        &encrypted.key_version,
        &encrypted.fingerprint,
        &before_row.created_by,
        &updated_by,
    )
    .await?;

    let after = fetch_tenant_ai_provider_setting(pool, &tenant_id, &provider).await?;
    let before_json = before
        .as_ref()
        .map(row_to_audit_json)
        .map(|v| v.to_string());
    let after_json = after.as_ref().map(row_to_audit_json).map(|v| v.to_string());

    insert_tenant_ai_provider_audit(
        pool,
        &tenant_id,
        &provider,
        "rotate",
        &updated_by,
        None,
        before_json.as_deref(),
        after_json.as_deref(),
    )
    .await?;

    json_response(
        StatusCode::OK,
        serde_json::json!({
          "ok": true,
          "tenant_id": tenant_id,
          "provider": provider,
          "key_version": encrypted.key_version,
          "key_hint": mask_key_hint(&encrypted.fingerprint)
        }),
    )
}

async fn handle_revoke_action(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
    let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
    let provided =
        bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

    if expected.is_empty() || provided != expected {
        return json_response(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"ok": false, "error": "unauthorized"}),
        );
    }

    let parsed: ProviderActionRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid json body: {e}")))
    })?;

    let tenant_id = parsed.tenant_id.trim().to_string();
    let provider = normalize_provider(&parsed.provider);
    if tenant_id.is_empty() || provider.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id and provider are required"}),
        );
    }

    if !has_tidb_url() {
        return json_response(
            StatusCode::NOT_IMPLEMENTED,
            serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
        );
    }

    let updated_by =
        trim_or_none(parsed.updated_by.as_deref()).unwrap_or_else(|| "system".to_string());
    let pool = get_pool().await?;
    let before = fetch_tenant_ai_provider_setting(pool, &tenant_id, &provider).await?;
    if before.is_none() {
        return json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({"ok": false, "error": "not_found", "message": "provider setting not found"}),
        );
    }

    set_tenant_ai_provider_status(pool, &tenant_id, &provider, "revoked", &updated_by).await?;
    let after = fetch_tenant_ai_provider_setting(pool, &tenant_id, &provider).await?;

    let before_json = before
        .as_ref()
        .map(row_to_audit_json)
        .map(|v| v.to_string());
    let after_json = after.as_ref().map(row_to_audit_json).map(|v| v.to_string());

    insert_tenant_ai_provider_audit(
        pool,
        &tenant_id,
        &provider,
        "revoke",
        &updated_by,
        None,
        before_json.as_deref(),
        after_json.as_deref(),
    )
    .await?;

    json_response(
        StatusCode::OK,
        serde_json::json!({
          "ok": true,
          "tenant_id": tenant_id,
          "provider": provider,
          "status": "revoked"
        }),
    )
}

async fn handle_routing_policy_action(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
    let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
    let provided =
        bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

    if expected.is_empty() || provided != expected {
        return json_response(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"ok": false, "error": "unauthorized"}),
        );
    }

    let parsed: RoutingPolicyRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid json body: {e}")))
    })?;

    let tenant_id = parsed.tenant_id.trim().to_string();
    if tenant_id.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
        );
    }

    let default_provider = normalize_provider(&parsed.default_provider);
    if !is_supported_provider(&default_provider) {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "unsupported default_provider"}),
        );
    }

    let updated_by =
        trim_or_none(parsed.updated_by.as_deref()).unwrap_or_else(|| "system".to_string());

    if !has_tidb_url() {
        return json_response(
            StatusCode::NOT_IMPLEMENTED,
            serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
        );
    }

    let pool = get_pool().await?;
    upsert_tenant_ai_routing_policy(
        pool,
        &tenant_id,
        &default_provider,
        parsed.monthly_budget_usd,
        &updated_by,
    )
    .await?;

    json_response(
        StatusCode::OK,
        serde_json::json!({
          "ok": true,
          "tenant_id": tenant_id,
          "default_provider": default_provider,
          "monthly_budget_usd": parsed.monthly_budget_usd
        }),
    )
}

async fn handle_ensure_trial_action(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
    let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
    let provided =
        bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

    if expected.is_empty() || provided != expected {
        return json_response(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"ok": false, "error": "unauthorized"}),
        );
    }

    let parsed: EnsureTrialRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid json body: {e}")))
    })?;

    if parsed.tenant_id.trim().is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
        );
    }

    if parsed.now_ms <= 0 {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "now_ms is required"}),
        );
    }

    if !has_tidb_url() {
        return json_response(
            StatusCode::NOT_IMPLEMENTED,
            serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
        );
    }

    let pool = get_pool().await?;
    let trial_started_at_ms =
        ensure_trial_started(pool, parsed.tenant_id.trim(), parsed.now_ms).await?;

    json_response(
        StatusCode::OK,
        serde_json::json!({"ok": true, "trial_started_at_ms": trial_started_at_ms}),
    )
}

async fn handle_router(
    method: &Method,
    headers: &HeaderMap,
    uri: &hyper::Uri,
    body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
    let action = query_param(uri.query(), "action").unwrap_or_default();
    match action.as_str() {
        "" => match *method {
            Method::GET => handle_query(headers, uri).await,
            Method::PUT | Method::POST => handle_upsert(headers, body).await,
            _ => json_response(
                StatusCode::METHOD_NOT_ALLOWED,
                serde_json::json!({"ok": false, "error": "method_not_allowed"}),
            ),
        },
        "test" => {
            if *method != Method::POST {
                return json_response(
                    StatusCode::METHOD_NOT_ALLOWED,
                    serde_json::json!({"ok": false, "error": "method_not_allowed"}),
                );
            }
            handle_test_action(headers, body).await
        }
        "rotate" => {
            if *method != Method::POST {
                return json_response(
                    StatusCode::METHOD_NOT_ALLOWED,
                    serde_json::json!({"ok": false, "error": "method_not_allowed"}),
                );
            }
            handle_rotate_action(headers, body).await
        }
        "revoke" => {
            if *method != Method::POST {
                return json_response(
                    StatusCode::METHOD_NOT_ALLOWED,
                    serde_json::json!({"ok": false, "error": "method_not_allowed"}),
                );
            }
            handle_revoke_action(headers, body).await
        }
        "routing_policy" => {
            if *method != Method::PUT && *method != Method::POST {
                return json_response(
                    StatusCode::METHOD_NOT_ALLOWED,
                    serde_json::json!({"ok": false, "error": "method_not_allowed"}),
                );
            }
            handle_routing_policy_action(headers, body).await
        }
        "ensure_trial" => {
            if *method != Method::POST {
                return json_response(
                    StatusCode::METHOD_NOT_ALLOWED,
                    serde_json::json!({"ok": false, "error": "method_not_allowed"}),
                );
            }
            handle_ensure_trial_action(headers, body).await
        }
        _ => json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({"ok": false, "error": "not_found"}),
        ),
    }
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
    let method = req.method().clone();
    let headers = req.headers().clone();
    let uri = req.uri().clone();
    let bytes = req.into_body().collect().await?.to_bytes();
    handle_router(&method, &headers, &uri, bytes).await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    run(service_fn(handler)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_provider_validation_works() {
        assert!(is_supported_provider("openai"));
        assert!(is_supported_provider("anthropic"));
        assert!(is_supported_provider("gemini"));
        assert!(is_supported_provider("bedrock"));
        assert!(!is_supported_provider("unknown"));
    }

    #[test]
    fn mask_key_hint_redacts() {
        let masked = mask_key_hint("1234567890abcdef");
        assert_eq!(masked, "fp:123456...cdef");
    }

    #[test]
    fn resolve_secret_material_reuses_existing_when_api_key_missing() {
        let existing = SecretMaterial {
            encrypted_api_key: "enc-existing".to_string(),
            encrypted_dek: Some("dek-existing".to_string()),
            key_version: "v1".to_string(),
            key_fingerprint: "fp-existing".to_string(),
        };

        let resolved = resolve_secret_material("", Some(&existing)).expect("should reuse existing");
        assert_eq!(resolved.encrypted_api_key, "enc-existing");
        assert_eq!(resolved.encrypted_dek.as_deref(), Some("dek-existing"));
        assert_eq!(resolved.key_version, "v1");
        assert_eq!(resolved.key_fingerprint, "fp-existing");
    }

    #[test]
    fn resolve_secret_material_requires_key_for_new_setting() {
        let err = resolve_secret_material("", None).expect_err("new setting requires api key");
        assert!(
            err.to_string().contains("api_key_plaintext is required"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn provider_v1_endpoint_handles_both_base_shapes() {
        assert_eq!(
            provider_v1_endpoint("https://api.openai.com", "responses"),
            "https://api.openai.com/v1/responses"
        );
        assert_eq!(
            provider_v1_endpoint("https://api.openai.com/v1", "responses"),
            "https://api.openai.com/v1/responses"
        );
    }

    #[tokio::test]
    async fn returns_unauthorized_when_missing_internal_token() {
        std::env::set_var("RUST_INTERNAL_TOKEN", "secret");

        let headers = HeaderMap::new();
        let uri: hyper::Uri = "/api/tenants/ai_settings?tenant_id=t1".parse().unwrap();
        let response = handle_router(&Method::GET, &headers, &uri, Bytes::new())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejects_unknown_provider_before_tidb_lookup() {
        std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
        std::env::remove_var("TIDB_DATABASE_URL");
        std::env::remove_var("DATABASE_URL");

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret".parse().unwrap());
        let body = serde_json::to_vec(&serde_json::json!({
          "tenant_id": "t1",
          "provider": "invalid",
          "api_key_plaintext": "x",
          "default_model": "m1"
        }))
        .unwrap();
        let uri: hyper::Uri = "/api/tenants/ai_settings".parse().unwrap();
        let response = handle_router(&Method::POST, &headers, &uri, Bytes::from(body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn ensure_trial_requires_internal_token() {
        std::env::set_var("RUST_INTERNAL_TOKEN", "secret");

        let headers = HeaderMap::new();
        let uri: hyper::Uri = "/api/tenants/ai_settings?action=ensure_trial"
            .parse()
            .unwrap();
        let body = serde_json::to_vec(&serde_json::json!({
          "tenant_id": "t1",
          "now_ms": 1700000000000i64
        }))
        .unwrap();

        let response = handle_router(&Method::POST, &headers, &uri, Bytes::from(body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}

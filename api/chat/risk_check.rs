use http_body_util::BodyExt;
use http_body_util::StreamBody;
use hyper::body::Frame;
use hyper::{HeaderMap, Method, StatusCode};
use serde::{Deserialize, Serialize};
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use bytes::Bytes;
use globa_flux_rust::cost::{compute_cost_usd, ModelPricingUsdPerMToken};
use globa_flux_rust::db::{
    consume_daily_usage_event, fetch_active_tenant_ai_provider_setting,
    fetch_tenant_ai_routing_policy, fetch_usage_event, get_pool, insert_usage_event,
    sum_spent_usd_today,
};
use globa_flux_rust::providers::gemini::{
    generate_text as gemini_generate_text, pricing_for_model as gemini_pricing_for_model,
    stream_generate as gemini_stream_generate, GeminiConfig, GeminiStreamEvent,
};
use globa_flux_rust::providers::openai::{
    build_risk_check_prompt, pricing_for_model as openai_pricing_for_model, RiskCheckMessageArgs,
};
use globa_flux_rust::secrets::decrypt_secret;
use globa_flux_rust::sse::sse_event;
use sqlx::MySqlPool;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const DAILY_RISK_CHECK_EVENT_TYPE: &str = "chat_risk_check_count";

#[derive(Clone)]
struct TtlEntry<T> {
    value: T,
    expires_at: Instant,
}

#[derive(Clone)]
struct DailyLimitStatus {
    day_key: String,
    used: i64,
    limit: i64,
    allowed: bool,
}

static SPENT_TODAY_CACHE: OnceLock<Mutex<HashMap<String, TtlEntry<f64>>>> = OnceLock::new();

fn ttl_from_env_ms(key: &str, default_ms: u64) -> Duration {
    let ms = std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default_ms);
    Duration::from_millis(ms)
}

fn cache_lookup<T: Clone>(cache: &Mutex<HashMap<String, TtlEntry<T>>>, key: &str) -> Option<T> {
    let mut guard = cache.lock().ok()?;
    let now = Instant::now();
    if let Some(entry) = guard.get(key) {
        if entry.expires_at > now {
            return Some(entry.value.clone());
        }
    }
    guard.remove(key);
    None
}

fn cache_store<T: Clone>(
    cache: &Mutex<HashMap<String, TtlEntry<T>>>,
    key: String,
    value: T,
    ttl: Duration,
) {
    if ttl.is_zero() {
        return;
    }
    if let Ok(mut guard) = cache.lock() {
        guard.insert(
            key,
            TtlEntry {
                value,
                expires_at: Instant::now() + ttl,
            },
        );
    }
}

async fn sum_spent_usd_today_cached(
    pool: &MySqlPool,
    tenant_id: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<f64, Error> {
    let ttl = ttl_from_env_ms("CHAT_SPENT_USD_CACHE_TTL_MS", 15 * 1000);
    let cache = SPENT_TODAY_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let day_key = now.format("%Y-%m-%d").to_string();
    let key = format!("{tenant_id}:{day_key}");

    if let Some(v) = cache_lookup(cache, &key) {
        return Ok(v);
    }

    let spent = sum_spent_usd_today(pool, tenant_id, now).await?;
    cache_store(cache, key, spent, ttl);
    Ok(spent)
}

#[derive(Clone)]
enum ResolvedProviderConfig {
    Gemini(GeminiConfig),
    OpenAi {
        api_key: String,
        api_base_url: String,
    },
    Anthropic {
        api_key: String,
        api_base_url: String,
    },
}

#[derive(Clone)]
struct ResolvedAiRuntime {
    provider: String,
    model: String,
    cfg: ResolvedProviderConfig,
}

fn pricing_for_resolved_runtime(runtime: &ResolvedAiRuntime) -> Option<ModelPricingUsdPerMToken> {
    match runtime.provider.as_str() {
        "gemini" => gemini_pricing_for_model(&runtime.model),
        "openai" => openai_pricing_for_model(&runtime.model),
        "anthropic" => {
            if let (Ok(prompt), Ok(completion)) = (
                std::env::var("ANTHROPIC_PRICE_PROMPT_USD_PER_M_TOKEN"),
                std::env::var("ANTHROPIC_PRICE_COMPLETION_USD_PER_M_TOKEN"),
            ) {
                if let (Ok(prompt), Ok(completion)) =
                    (prompt.parse::<f64>(), completion.parse::<f64>())
                {
                    return Some(ModelPricingUsdPerMToken { prompt, completion });
                }
            }
            None
        }
        _ => None,
    }
}

fn openai_extract_text(json: &serde_json::Value) -> String {
    if let Some(text) = json.get("output_text").and_then(|v| v.as_str()) {
        return text.to_string();
    }

    let mut out = String::new();
    let output = json
        .get("output")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for item in output {
        let parts = item
            .get("content")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        for part in parts {
            if let Some(text) = part.get("text").and_then(|v| v.as_str()) {
                out.push_str(text);
            }
        }
    }
    out
}

fn openai_extract_usage(json: &serde_json::Value) -> Option<Usage> {
    let usage = json.get("usage")?;
    let prompt_tokens = usage
        .get("input_tokens")
        .or_else(|| usage.get("prompt_tokens"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let completion_tokens = usage
        .get("output_tokens")
        .or_else(|| usage.get("completion_tokens"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    Some(Usage {
        prompt_tokens,
        completion_tokens,
    })
}

fn anthropic_extract_text(json: &serde_json::Value) -> String {
    let mut out = String::new();
    let content = json
        .get("content")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for part in content {
        if let Some(text) = part.get("text").and_then(|v| v.as_str()) {
            out.push_str(text);
        }
    }
    out
}

fn anthropic_extract_usage(json: &serde_json::Value) -> Option<Usage> {
    let usage = json.get("usage")?;
    let prompt_tokens = usage
        .get("input_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let completion_tokens = usage
        .get("output_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    Some(Usage {
        prompt_tokens,
        completion_tokens,
    })
}

fn provider_v1_endpoint(base_url: &str, path: &str) -> String {
    let trimmed = base_url.trim().trim_end_matches('/');
    if trimmed.ends_with("/v1") {
        format!("{trimmed}/{path}")
    } else {
        format!("{trimmed}/v1/{path}")
    }
}

fn normalize_supported_provider(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase();
    if matches!(normalized.as_str(), "gemini" | "openai" | "anthropic") {
        Some(normalized)
    } else {
        None
    }
}

async fn openai_generate_text(
    api_key: &str,
    api_base_url: &str,
    model: &str,
    system: &str,
    user: &str,
    temperature: f64,
    max_output_tokens: u32,
    idempotency_key: Option<&str>,
) -> Result<(String, Option<Usage>), Error> {
    let url = provider_v1_endpoint(api_base_url, "responses");

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(
            |e| -> Error { Box::new(std::io::Error::other(format!("invalid openai key: {e}"))) },
        )?,
    );
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        reqwest::header::HeaderValue::from_static("application/json"),
    );
    headers.insert(
        reqwest::header::ACCEPT,
        reqwest::header::HeaderValue::from_static("application/json"),
    );
    if let Some(key) = idempotency_key.filter(|v| !v.trim().is_empty()) {
        headers.insert(
            "Idempotency-Key",
            reqwest::header::HeaderValue::from_str(key).map_err(|e| -> Error {
                Box::new(std::io::Error::other(format!("invalid idempotency key: {e}")))
            })?,
        );
    }

    let payload = serde_json::json!({
      "model": model,
      "temperature": temperature,
      "max_output_tokens": max_output_tokens,
      "input": [
        {
          "role": "system",
          "content": [{"type":"input_text","text": system}]
        },
        {
          "role": "user",
          "content": [{"type":"input_text","text": user}]
        }
      ]
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(url)
        .headers(headers)
        .json(&payload)
        .send()
        .await
        .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?;
    let status = resp.status();
    let json = resp
        .json::<serde_json::Value>()
        .await
        .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?;

    if !status.is_success() {
        let message = json
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown_openai_error");
        return Err(Box::new(std::io::Error::other(format!(
            "OpenAI error (status {}): {}",
            status.as_u16(),
            message
        ))));
    }

    Ok((openai_extract_text(&json), openai_extract_usage(&json)))
}

async fn anthropic_generate_text(
    api_key: &str,
    api_base_url: &str,
    model: &str,
    system: &str,
    user: &str,
    temperature: f64,
    max_output_tokens: u32,
) -> Result<(String, Option<Usage>), Error> {
    let url = provider_v1_endpoint(api_base_url, "messages");

    let payload = serde_json::json!({
      "model": model,
      "system": system,
      "max_tokens": max_output_tokens,
      "temperature": temperature,
      "messages": [{"role":"user","content": user}]
    });

    let client = reqwest::Client::new();
    let resp = client
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
    let json = resp
        .json::<serde_json::Value>()
        .await
        .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?;

    if !status.is_success() {
        let message = json
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown_anthropic_error");
        return Err(Box::new(std::io::Error::other(format!(
            "Anthropic error (status {}): {}",
            status.as_u16(),
            message
        ))));
    }

    Ok((anthropic_extract_text(&json), anthropic_extract_usage(&json)))
}

async fn generate_text_for_runtime(
    runtime: &ResolvedAiRuntime,
    system: &str,
    user: &str,
    temperature: f64,
    max_output_tokens: u32,
    idempotency_key: Option<&str>,
) -> Result<(String, Usage), Error> {
    let (text, usage_opt) = match &runtime.cfg {
        ResolvedProviderConfig::Gemini(cfg) => {
            let (text, usage) =
                gemini_generate_text(cfg, system, user, temperature, max_output_tokens).await?;
            let usage = usage.map(|u| Usage {
                prompt_tokens: u.prompt_tokens,
                completion_tokens: u.completion_tokens,
            });
            (text, usage)
        }
        ResolvedProviderConfig::OpenAi {
            api_key,
            api_base_url,
        } => {
            openai_generate_text(
                api_key,
                api_base_url,
                &runtime.model,
                system,
                user,
                temperature,
                max_output_tokens,
                idempotency_key,
            )
            .await?
        }
        ResolvedProviderConfig::Anthropic {
            api_key,
            api_base_url,
        } => {
            anthropic_generate_text(
                api_key,
                api_base_url,
                &runtime.model,
                system,
                user,
                temperature,
                max_output_tokens,
            )
            .await?
        }
    };

    let usage = usage_opt.unwrap_or(Usage {
        prompt_tokens: 0,
        completion_tokens: 0,
    });
    Ok((text, usage))
}

async fn resolve_runtime_from_active_setting(
    pool: &MySqlPool,
    tenant_id: &str,
    provider: &str,
) -> Result<Option<ResolvedAiRuntime>, Error> {
    let Some(setting) = fetch_active_tenant_ai_provider_setting(pool, tenant_id, Some(provider)).await?
    else {
        return Ok(None);
    };

    let api_key = decrypt_secret(&setting.encrypted_api_key, &setting.key_version)?;
    if api_key.trim().is_empty() {
        return Err(Box::new(std::io::Error::other(
            "configured provider api_key is empty",
        )));
    }

    let model = setting.default_model.trim().to_string();
    if model.is_empty() {
        return Err(Box::new(std::io::Error::other(
            "configured default_model is empty",
        )));
    }

    let cfg = match provider {
        "gemini" => {
            let api_base_url = std::env::var("GEMINI_API_BASE_URL")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| "https://generativelanguage.googleapis.com/v1".to_string());
            ResolvedProviderConfig::Gemini(GeminiConfig {
                api_key,
                model: model.clone(),
                api_base_url,
            })
        }
        "openai" => {
            let api_base_url = std::env::var("OPENAI_API_BASE_URL")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| "https://api.openai.com/v1".to_string());
            ResolvedProviderConfig::OpenAi {
                api_key,
                api_base_url,
            }
        }
        "anthropic" => {
            let api_base_url = std::env::var("ANTHROPIC_API_BASE_URL")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| "https://api.anthropic.com/v1".to_string());
            ResolvedProviderConfig::Anthropic {
                api_key,
                api_base_url,
            }
        }
        _ => {
            return Err(Box::new(std::io::Error::other(format!(
                "provider '{}' is not supported in chat runtime yet",
                provider
            ))));
        }
    };

    Ok(Some(ResolvedAiRuntime {
        provider: provider.to_string(),
        model,
        cfg,
    }))
}

async fn resolve_ai_runtime(
    pool: &MySqlPool,
    tenant_id: &str,
) -> Result<ResolvedAiRuntime, Error> {
    let policy = fetch_tenant_ai_routing_policy(pool, tenant_id).await?;
    let preferred_provider = policy
        .as_ref()
        .map(|p| p.default_provider.as_str())
        .and_then(normalize_supported_provider)
        .unwrap_or_else(|| "gemini".to_string());
    if let Some(raw_default) = policy
        .as_ref()
        .map(|p| p.default_provider.trim().to_ascii_lowercase())
    {
        if !raw_default.is_empty() && normalize_supported_provider(&raw_default).is_none() {
            return Err(Box::new(std::io::Error::other(format!(
                "default provider '{}' is not supported in chat runtime yet",
                raw_default
            ))));
        }
    }

    match resolve_runtime_from_active_setting(pool, tenant_id, &preferred_provider).await {
        Ok(Some(runtime)) => Ok(runtime),
        Ok(None) => Err(Box::new(std::io::Error::other(format!(
            "missing active tenant {} provider config",
            preferred_provider
        )))),
        Err(err) => Err(err),
    }
}

fn daily_limit_exceeded_response(
    stream: bool,
    status: DailyLimitStatus,
) -> Result<Response<ResponseBody>, Error> {
    if stream {
        return sse_response(
            StatusCode::OK,
            sse_event(
                "error",
                &serde_json::json!({
                  "code": "entitlement_exceeded",
                  "message": "Daily chat_risk_checks_per_day limit exceeded",
                  "day_key": status.day_key,
                  "used": status.used,
                  "limit": status.limit,
                })
                .to_string(),
            ),
        );
    }

    json_response(
        StatusCode::TOO_MANY_REQUESTS,
        serde_json::json!({
          "ok": false,
          "error": "entitlement_exceeded",
          "message": "Daily chat_risk_checks_per_day limit exceeded",
          "day_key": status.day_key,
          "used": status.used,
          "limit": status.limit,
        }),
    )
}

async fn enforce_daily_chat_risk_limit(
    pool: &MySqlPool,
    tenant_id: &str,
    idempotency_key: &str,
    limit: Option<i64>,
) -> Result<Option<DailyLimitStatus>, Error> {
    let Some(limit) = limit else {
        return Ok(None);
    };

    let day_key = chrono::Utc::now().format("%Y-%m-%d").to_string();
    if limit <= 0 {
        return Ok(Some(DailyLimitStatus {
            day_key,
            used: 0,
            limit,
            allowed: false,
        }));
    }

    let result = consume_daily_usage_event(
        pool,
        tenant_id,
        DAILY_RISK_CHECK_EVENT_TYPE,
        idempotency_key,
        limit,
        chrono::Utc::now(),
    )
    .await?;

    Ok(Some(DailyLimitStatus {
        day_key: result.day_key,
        used: result.used,
        limit,
        allowed: result.allowed,
    }))
}

#[derive(Deserialize)]
struct RiskCheckRequest {
    request_id: String,
    tenant_id: String,
    trial_started_at_ms: i64,
    budget_usd_per_day: f64,
    #[serde(default)]
    chat_risk_checks_per_day: Option<i64>,
    action_type: String,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Serialize, Clone)]
struct Usage {
    prompt_tokens: i32,
    completion_tokens: i32,
}

#[derive(Deserialize)]
struct AgentRequest {
    request_id: String,
    tenant_id: String,
    trial_started_at_ms: i64,
    budget_usd_per_day: f64,
    #[serde(default)]
    chat_risk_checks_per_day: Option<i64>,
    message: String,
    #[serde(default)]
    video_context: Option<VideoContext>,
}

#[derive(Deserialize, Clone)]
struct VideoContext {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    target_audience: Option<String>,
    #[serde(default)]
    new_angle: Option<String>,
    #[serde(default)]
    existing_description: Option<String>,
    #[serde(default)]
    outline: Option<String>,
    #[serde(default)]
    top_comments: Option<String>,
    #[serde(default)]
    notes: Option<String>,
}

fn bearer_token(header_value: Option<&str>) -> Option<&str> {
    let value = header_value?;
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
}

fn query_param<'a>(uri: &'a hyper::Uri, key: &str) -> Option<&'a str> {
    let q = uri.query()?;
    for part in q.split('&') {
        let (k, v) = part.split_once('=')?;
        if k == key {
            return Some(v);
        }
        if part == key {
            return Some("");
        }
    }
    None
}

fn is_agent_mode(uri: &hyper::Uri) -> bool {
    matches!(query_param(uri, "mode"), Some("agent"))
}

fn wants_stream(uri: &hyper::Uri, headers: &HeaderMap) -> bool {
    if let Some(q) = uri.query() {
        for part in q.split('&') {
            if part == "stream=1" {
                return true;
            }
        }
    }

    headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("text/event-stream"))
}

fn sse_response(status: StatusCode, body: String) -> Result<Response<ResponseBody>, Error> {
    Ok(Response::builder()
        .status(status)
        .header("content-type", "text/event-stream; charset=utf-8")
        .header("cache-control", "no-cache, no-transform")
        .body(ResponseBody::from(body))?)
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

fn config_error_response(stream: bool, message: &str) -> Result<Response<ResponseBody>, Error> {
    if stream {
        return sse_response(
            StatusCode::OK,
            sse_event(
                "error",
                &serde_json::json!({"code":"config_error","message": message}).to_string(),
            ),
        );
    }

    json_response(
        StatusCode::OK,
        serde_json::json!({"ok": false, "error": "config_error", "message": message}),
    )
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

fn normalize_model_json_text(raw: &str) -> String {
    let mut text = raw.trim().to_string();
    if !text.starts_with("```") {
        return text;
    }

    if let Some(first_nl) = text.find('\n') {
        text = text[(first_nl + 1)..].to_string();
    }
    if let Some(end_fence) = text.rfind("```") {
        text = text[..end_fence].to_string();
    }
    text.trim().to_string()
}

fn parse_model_json(raw: &str) -> Option<serde_json::Value> {
    let normalized = normalize_model_json_text(raw);
    if normalized.is_empty() {
        return None;
    }

    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&normalized) {
        return Some(v);
    }

    let start = normalized.find('{')?;
    let end = normalized.rfind('}')?;
    if end <= start {
        return None;
    }
    serde_json::from_str::<serde_json::Value>(&normalized[start..=end]).ok()
}

fn coerce_risk_check_result(raw_output: &str) -> serde_json::Value {
    let raw_output = raw_output.trim();

    let parsed = parse_model_json(raw_output);
    let mut v = match parsed {
        Some(v) => v,
        None => {
            let snippet = truncate_chars(raw_output, 600);
            let reason = if snippet.is_empty() {
                "Cannot assess risk: empty model output.".to_string()
            } else {
                format!("Cannot assess risk: model returned non-JSON output. Output: {snippet}")
            };
            return serde_json::json!({"risk_level":"medium","allowed":false,"reason":reason});
        }
    };

    let obj = match v.as_object_mut() {
        Some(obj) => obj,
        None => {
            let snippet = truncate_chars(raw_output, 600);
            let reason = if snippet.is_empty() {
                "Cannot assess risk: empty model output.".to_string()
            } else {
                format!("Cannot assess risk: model returned non-object JSON. Output: {snippet}")
            };
            return serde_json::json!({"risk_level":"medium","allowed":false,"reason":reason});
        }
    };

    match obj.get("risk_level") {
        Some(serde_json::Value::String(_)) => {}
        _ => {
            obj.insert(
                "risk_level".to_string(),
                serde_json::Value::String("medium".to_string()),
            );
        }
    }

    match obj.get("allowed") {
        Some(serde_json::Value::Bool(_)) => {}
        _ => {
            obj.insert("allowed".to_string(), serde_json::Value::Bool(false));
        }
    }

    match obj.get("reason") {
        Some(serde_json::Value::String(_)) => {}
        _ => {
            obj.insert(
                "reason".to_string(),
                serde_json::Value::String("No reason provided.".to_string()),
            );
        }
    }

    v
}

fn trim_to_option(value: Option<&str>) -> Option<String> {
    let v = value?.trim();
    if v.is_empty() {
        None
    } else {
        Some(v.to_string())
    }
}

fn build_agent_prompt(message: &str, video_context: Option<&VideoContext>) -> (String, String) {
    let title = video_context.and_then(|c| trim_to_option(c.title.as_deref()));
    let topic = video_context.and_then(|c| trim_to_option(c.topic.as_deref()));
    let target_audience = video_context.and_then(|c| trim_to_option(c.target_audience.as_deref()));
    let new_angle = video_context.and_then(|c| trim_to_option(c.new_angle.as_deref()));
    let existing_description =
        video_context.and_then(|c| trim_to_option(c.existing_description.as_deref()));
    let outline = video_context.and_then(|c| trim_to_option(c.outline.as_deref()));
    let top_comments = video_context.and_then(|c| trim_to_option(c.top_comments.as_deref()));
    let notes = video_context.and_then(|c| trim_to_option(c.notes.as_deref()));

    let mut missing_required: Vec<&str> = Vec::new();
    if title.is_none() {
        missing_required.push("title");
    }
    if topic.is_none() {
        missing_required.push("topic");
    }
    if target_audience.is_none() {
        missing_required.push("target_audience");
    }
    if new_angle.is_none() {
        missing_required.push("new_angle");
    }

    let system = r#"You are GF Agent: a YouTube-first Content Pack Generator.

Reply naturally in Chinese.

Rules:
- Suggestions only. Do NOT claim you changed anything on YouTube.
- Do NOT propose running internal jobs unless the user explicitly asks to "刷新/发任务/跑任务/执行任务/Run now".
- If required context is missing (title/topic/target audience/new angle), ask at most 1–3 short questions and stop.
- If required context is present, output a complete content pack using EXACTLY these Markdown sections/headings:

## Quick Summary
## Title Options (10)
## Description
### Hooks (3)
### Full Description
## Pinned Comment
## Hook / Script Outline
## A/B Tip

Content requirements (when generating a content pack):
- Quick Summary: 1–2 sentences confirming understanding of the video + audience.
- Title Options (10): 10 short, clear, high-CTR options.
- Description: Hooks (3) are the first 2 lines; Full Description is one complete description (include CTA + placeholders as needed).
- Pinned Comment: 1 pinned comment that drives engagement.
- Hook / Script Outline: 5–10 bullets, with the first 30s emphasized.
- A/B Tip: 1–2 lines on how to compare two variants.

Formatting rules (when generating a content pack):
- Use a numbered list 1–10 for Title Options.
- Use 3 bullets for Hooks (3).
- Keep it copy/paste friendly; no extra preface, no apology.
"#
    .to_string();

    let mut user = String::new();
    user.push_str("Video context (single video):\n");
    user.push_str(&format!(
        "- Title: {}\n",
        title.clone().unwrap_or_else(|| "（缺失）".to_string())
    ));
    user.push_str(&format!(
        "- Topic: {}\n",
        topic.clone().unwrap_or_else(|| "（缺失）".to_string())
    ));
    user.push_str(&format!(
        "- Target audience: {}\n",
        target_audience
            .clone()
            .unwrap_or_else(|| "（缺失）".to_string())
    ));
    user.push_str(&format!(
        "- New/controversial angle: {}\n",
        new_angle.clone().unwrap_or_else(|| "（缺失）".to_string())
    ));

    if let Some(v) = existing_description.as_ref() {
        user.push_str(&format!("\nExisting description (optional):\n{}\n", v));
    }
    if let Some(v) = outline.as_ref() {
        user.push_str(&format!("\nOutline / notes (optional):\n{}\n", v));
    }
    if let Some(v) = top_comments.as_ref() {
        user.push_str(&format!("\nTop comments / objections (optional):\n{}\n", v));
    }
    if let Some(v) = notes.as_ref() {
        user.push_str(&format!("\nExtra context (optional):\n{}\n", v));
    }

    if !missing_required.is_empty() {
        user.push_str(&format!(
            "\nMissing required context: {}\n",
            missing_required.join(", ")
        ));
    }

    user.push_str("\nUser message:\n");
    user.push_str(message);

    (system, user)
}

fn agent_wants_run_task(message: &str) -> bool {
    let m = message.to_lowercase();
    m.contains("发任务")
        || m.contains("跑任务")
        || m.contains("执行任务")
        || m.contains("刷新")
        || m.contains("run now")
        || m.contains("refresh")
        || m.contains("dispatch")
        || m.contains("tick")
        || m.contains("worker")
}

async fn handle_agent(
    stream: bool,
    idempotency_key: &str,
    body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
    let parsed: AgentRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid json body: {e}")))
    })?;

    let pool = match get_pool().await {
        Ok(pool) => pool,
        Err(err) => {
            return config_error_response(stream, &err.to_string());
        }
    };

    match enforce_daily_chat_risk_limit(
        pool,
        &parsed.tenant_id,
        &idempotency_key,
        parsed.chat_risk_checks_per_day,
    )
    .await?
    {
        Some(status) if !status.allowed => {
            return daily_limit_exceeded_response(stream, status);
        }
        _ => {}
    }

    const EVENT_TYPE: &str = "chat_agent";

    if let Some(existing) =
        fetch_usage_event(pool, &parsed.tenant_id, EVENT_TYPE, idempotency_key).await?
    {
        let replay_message =
            "Replay (idempotency): request already processed. Please send a new message.";

        if stream {
            return sse_response(
                StatusCode::OK,
                sse_event(
                    "final",
                    &serde_json::json!({
                      "message": replay_message,
                      "usage": {"prompt_tokens": existing.prompt_tokens, "completion_tokens": existing.completion_tokens},
                      "cost_usd": existing.cost_usd
                    })
                    .to_string(),
                ),
            );
        }

        return json_response(
            StatusCode::OK,
            serde_json::json!({
              "ok": true,
              "message": replay_message,
              "usage": {"prompt_tokens": existing.prompt_tokens, "completion_tokens": existing.completion_tokens},
              "cost_usd": existing.cost_usd
            }),
        );
    }

    let spent_usd_today =
        sum_spent_usd_today_cached(pool, &parsed.tenant_id, chrono::Utc::now()).await?;

    let runtime = match resolve_ai_runtime(pool, &parsed.tenant_id).await {
        Ok(resolved) => resolved,
        Err(err) => {
            return config_error_response(stream, &err.to_string());
        }
    };
    let model = runtime.model.clone();
    let max_output_tokens: u32 = std::env::var("GEMINI_MAX_OUTPUT_TOKENS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(900);
    let prompt_reserve_tokens: u32 = std::env::var("GEMINI_PROMPT_TOKEN_RESERVE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2500);

    let provider = runtime.provider.clone();
    let pricing = pricing_for_resolved_runtime(&runtime);

    let reserved_cost_usd = pricing
        .map(|p| compute_cost_usd(p, prompt_reserve_tokens, max_output_tokens))
        .unwrap_or(0.0);

    if spent_usd_today + reserved_cost_usd > parsed.budget_usd_per_day {
        if stream {
            return sse_response(
                StatusCode::OK,
                sse_event(
                    "error",
                    &serde_json::json!({
                      "code": "budget_exceeded",
                      "message": "Daily LLM budget exceeded (precheck + reserve)",
                      "spent_usd_today": spent_usd_today,
                      "reserved_cost_usd": reserved_cost_usd,
                      "budget_usd_per_day": parsed.budget_usd_per_day
                    })
                    .to_string(),
                ),
            );
        }

        return json_response(
            StatusCode::TOO_MANY_REQUESTS,
            serde_json::json!({
              "ok": false,
              "error": "budget_exceeded",
              "message": "Daily LLM budget exceeded (precheck + reserve)",
              "spent_usd_today": spent_usd_today,
              "reserved_cost_usd": reserved_cost_usd,
              "budget_usd_per_day": parsed.budget_usd_per_day
            }),
        );
    }

    let temperature: f64 = 0.6;
    let (system, user) = build_agent_prompt(&parsed.message, parsed.video_context.as_ref());

    if stream {
        let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Error>>(32);

        let tenant_id = parsed.tenant_id.clone();
        let request_id = parsed.request_id.clone();
        let message = parsed.message.clone();
        let trial_started_at_ms = parsed.trial_started_at_ms;
        let budget_usd_per_day = parsed.budget_usd_per_day;
        let idempotency_key2 = idempotency_key.to_string();
        let model2 = model.clone();
        let provider2 = provider.clone();
        let pricing2 = pricing;
        let runtime2 = runtime.clone();
        let system2 = system.clone();
        let user2 = user.clone();
        let max_output_tokens2 = max_output_tokens;
        let temperature2 = temperature;

        tokio::spawn(async move {
            let output_shared = std::sync::Arc::new(tokio::sync::Mutex::new(String::new()));
            let usage_shared = std::sync::Arc::new(tokio::sync::Mutex::new(None::<Usage>));

            let output_shared2 = std::sync::Arc::clone(&output_shared);
            let usage_shared2 = std::sync::Arc::clone(&usage_shared);
            let tx2 = tx.clone();

            let res = match &runtime2.cfg {
                ResolvedProviderConfig::Gemini(gemini_cfg2) => {
                    gemini_stream_generate(
                        gemini_cfg2,
                        &system2,
                        &user2,
                        temperature2,
                        max_output_tokens2,
                        move |event| {
                            let output_shared2 = std::sync::Arc::clone(&output_shared2);
                            let usage_shared2 = std::sync::Arc::clone(&usage_shared2);
                            let tx2 = tx2.clone();

                            async move {
                                match event {
                                    GeminiStreamEvent::Usage(u) => {
                                        *usage_shared2.lock().await = Some(Usage {
                                            prompt_tokens: u.prompt_tokens,
                                            completion_tokens: u.completion_tokens,
                                        });
                                        Ok(())
                                    }
                                    GeminiStreamEvent::Delta(delta) => {
                                        {
                                            let mut out = output_shared2.lock().await;
                                            out.push_str(&delta);
                                        }

                                        tx2.send(Ok(Frame::data(Bytes::from(sse_event(
                                            "token",
                                            &serde_json::json!({"text": delta}).to_string(),
                                        )))))
                                        .await
                                        .map_err(|e| {
                                            Box::new(std::io::Error::other(e.to_string())) as Error
                                        })?;
                                        Ok(())
                                    }
                                }
                            }
                        },
                    )
                    .await
                }
                _ => match generate_text_for_runtime(
                    &runtime2,
                    &system2,
                    &user2,
                    temperature2,
                    max_output_tokens2,
                    Some(&idempotency_key2),
                )
                .await
                {
                    Ok((text, usage)) => {
                        {
                            let mut out = output_shared.lock().await;
                            out.push_str(&text);
                        }
                        *usage_shared.lock().await = Some(usage);
                        let _ = tx
                            .send(Ok(Frame::data(Bytes::from(sse_event(
                                "token",
                                &serde_json::json!({"text": text}).to_string(),
                            )))))
                            .await;
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
            };

            if let Err(e) = res {
                let _ = tx
                    .send(Ok(Frame::data(Bytes::from(sse_event(
                        "error",
                        &serde_json::json!({"code":"upstream_error","message":e.to_string()})
                            .to_string(),
                    )))))
                    .await;
                return;
            }

            let output_text = output_shared.lock().await.clone();
            let usage = usage_shared.lock().await.clone().unwrap_or(Usage {
                prompt_tokens: 0,
                completion_tokens: 0,
            });

            let cost_usd = pricing2
                .map(|p| {
                    compute_cost_usd(
                        p,
                        usage.prompt_tokens as u32,
                        usage.completion_tokens as u32,
                    )
                })
                .unwrap_or(0.0);

            let insert_result = insert_usage_event(
                pool,
                &tenant_id,
                EVENT_TYPE,
                &idempotency_key2,
                &provider2,
                &model2,
                usage.prompt_tokens,
                usage.completion_tokens,
                cost_usd,
            )
            .await;

            if let Err(err) = insert_result {
                if err
                    .as_database_error()
                    .is_some_and(|e| e.is_unique_violation())
                {
                    // ok
                } else {
                    let _ = tx
                        .send(Ok(Frame::data(Bytes::from(sse_event(
                            "error",
                            &serde_json::json!({"code":"tidb_error","message":err.to_string()})
                                .to_string(),
                        )))))
                        .await;
                    return;
                }
            }

            let tool_proposal = if agent_wants_run_task(&message) {
                Some(serde_json::json!({
                  "type": "jobs_run_now",
                  "schedule": "daily",
                  "limit": 10,
                  "requires_confirmation": true,
                  "note": "Will dispatch and run queued jobs for this tenant."
                }))
            } else {
                None
            };

            let final_payload = serde_json::json!({
              "message": output_text.trim(),
              "tool_proposal": tool_proposal,
              "usage": usage,
              "cost_usd": cost_usd,
              "request_id": request_id,
              "tenant_id": tenant_id,
              "trial_started_at_ms": trial_started_at_ms,
              "budget_usd_per_day": budget_usd_per_day
            });

            let _ = tx
                .send(Ok(Frame::data(Bytes::from(sse_event(
                    "final",
                    &final_payload.to_string(),
                )))))
                .await;
        });

        let body = StreamBody::new(ReceiverStream::new(rx));
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream; charset=utf-8")
            .header("cache-control", "no-cache, no-transform")
            .body(ResponseBody::from(body))?);
    }

    let (text, usage) = generate_text_for_runtime(
        &runtime,
        &system,
        &user,
        temperature,
        max_output_tokens,
        Some(idempotency_key),
    )
    .await?;
    let cost_usd = pricing
        .map(|p| {
            compute_cost_usd(
                p,
                usage.prompt_tokens as u32,
                usage.completion_tokens as u32,
            )
        })
        .unwrap_or(0.0);

    let insert_result = insert_usage_event(
        pool,
        &parsed.tenant_id,
        EVENT_TYPE,
        idempotency_key,
        &provider,
        &model,
        usage.prompt_tokens,
        usage.completion_tokens,
        cost_usd,
    )
    .await;

    if let Err(err) = insert_result {
        if !err
            .as_database_error()
            .is_some_and(|e| e.is_unique_violation())
        {
            return Err(Box::new(err));
        }
    }

    let tool_proposal = if agent_wants_run_task(&parsed.message) {
        Some(serde_json::json!({
          "type": "jobs_run_now",
          "schedule": "daily",
          "limit": 10,
          "requires_confirmation": true,
          "note": "Will dispatch and run queued jobs for this tenant."
        }))
    } else {
        None
    };

    json_response(
        StatusCode::OK,
        serde_json::json!({
          "ok": true,
          "message": text.trim(),
          "tool_proposal": tool_proposal,
          "usage": usage,
          "cost_usd": cost_usd
        }),
    )
}

async fn handle_risk_check(
    method: &Method,
    headers: &HeaderMap,
    uri: &hyper::Uri,
    body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
    if method != Method::POST {
        return json_response(
            StatusCode::METHOD_NOT_ALLOWED,
            serde_json::json!({"ok": false, "error": "method_not_allowed"}),
        );
    }

    let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
    let provided =
        bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

    if expected.is_empty() || provided != expected {
        return json_response(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"ok": false, "error": "unauthorized"}),
        );
    }

    let stream = wants_stream(uri, headers);

    let idempotency_key = headers
        .get("x-idempotency-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if idempotency_key.is_empty() {
        if stream {
            return sse_response(
                StatusCode::OK,
                sse_event(
                    "error",
                    r#"{"code":"missing_idempotency_key","message":"Missing X-Idempotency-Key"}"#,
                ),
            );
        }

        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "missing_idempotency_key"}),
        );
    }

    if is_agent_mode(uri) {
        return handle_agent(stream, &idempotency_key, body).await;
    }

    let parsed: RiskCheckRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid json body: {e}")))
    })?;

    // TiDB budget precheck + idempotent usage accounting.
    // Note: Hydrogen passes `budget_usd_per_day` (trial entitlements), Rust enforces it here.
    let pool = match get_pool().await {
        Ok(pool) => pool,
        Err(err) => {
            return config_error_response(stream, &err.to_string());
        }
    };

    match enforce_daily_chat_risk_limit(
        pool,
        &parsed.tenant_id,
        &idempotency_key,
        parsed.chat_risk_checks_per_day,
    )
    .await?
    {
        Some(status) if !status.allowed => {
            return daily_limit_exceeded_response(stream, status);
        }
        _ => {}
    }

    const EVENT_TYPE: &str = "chat_risk_check";
    if let Some(existing) =
        fetch_usage_event(pool, &parsed.tenant_id, EVENT_TYPE, &idempotency_key).await?
    {
        if stream {
            return sse_response(
                StatusCode::OK,
                sse_event(
                    "final",
                    &serde_json::json!({
                      "result": {
                        "risk_level": "low",
                        "allowed": true,
                        "reason": "Replay (idempotency): returning previously recorded result"
                      },
                      "usage": {
                        "prompt_tokens": existing.prompt_tokens,
                        "completion_tokens": existing.completion_tokens
                      },
                      "cost_usd": existing.cost_usd
                    })
                    .to_string(),
                ),
            );
        }

        return json_response(
            StatusCode::OK,
            serde_json::json!({
              "ok": true,
              "result": {"risk_level":"low","allowed":true,"reason":"Replay (idempotency): returning previously recorded result"},
              "usage": {"prompt_tokens": existing.prompt_tokens, "completion_tokens": existing.completion_tokens},
              "cost_usd": existing.cost_usd
            }),
        );
    }

    let spent_usd_today =
        sum_spent_usd_today_cached(pool, &parsed.tenant_id, chrono::Utc::now()).await?;
    let temperature: f64 = 0.2;

    let runtime = match resolve_ai_runtime(pool, &parsed.tenant_id).await {
        Ok(resolved) => resolved,
        Err(err) => {
            return config_error_response(stream, &err.to_string());
        }
    };
    let model = runtime.model.clone();
    let max_output_tokens: u32 = std::env::var("GEMINI_MAX_OUTPUT_TOKENS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);
    let prompt_reserve_tokens: u32 = std::env::var("GEMINI_PROMPT_TOKEN_RESERVE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);

    let provider = runtime.provider.clone();
    let pricing = pricing_for_resolved_runtime(&runtime);

    let reserved_cost_usd = pricing
        .map(|p| compute_cost_usd(p, prompt_reserve_tokens, max_output_tokens))
        .unwrap_or(0.0);

    if spent_usd_today + reserved_cost_usd > parsed.budget_usd_per_day {
        if stream {
            return sse_response(
                StatusCode::OK,
                sse_event(
                    "error",
                    &serde_json::json!({
                      "code": "budget_exceeded",
                      "message": "Daily LLM budget exceeded (precheck + reserve)",
                      "spent_usd_today": spent_usd_today,
                      "reserved_cost_usd": reserved_cost_usd,
                      "budget_usd_per_day": parsed.budget_usd_per_day
                    })
                    .to_string(),
                ),
            );
        }

        return json_response(
            StatusCode::TOO_MANY_REQUESTS,
            serde_json::json!({
              "ok": false,
              "error": "budget_exceeded",
              "message": "Daily LLM budget exceeded (precheck + reserve)",
              "spent_usd_today": spent_usd_today,
              "reserved_cost_usd": reserved_cost_usd,
              "budget_usd_per_day": parsed.budget_usd_per_day
            }),
        );
    }

    if stream {
        let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Error>>(32);

        let tenant_id = parsed.tenant_id.clone();
        let request_id = parsed.request_id.clone();
        let action_type = parsed.action_type.clone();
        let note = parsed.note.clone();
        let trial_started_at_ms = parsed.trial_started_at_ms;
        let budget_usd_per_day = parsed.budget_usd_per_day;
        let idempotency_key2 = idempotency_key.clone();
        let model2 = model.clone();
        let provider2 = provider.clone();
        let pricing2 = pricing;
        let runtime2 = runtime.clone();
        let max_output_tokens2 = max_output_tokens;
        let temperature2 = temperature;

        tokio::spawn(async move {
            let prompt = build_risk_check_prompt(RiskCheckMessageArgs {
                action_type: &action_type,
                note: note.as_deref(),
            });

            let output_shared = std::sync::Arc::new(tokio::sync::Mutex::new(String::new()));
            let usage_shared = std::sync::Arc::new(tokio::sync::Mutex::new(None::<Usage>));

            let output_shared2 = std::sync::Arc::clone(&output_shared);
            let usage_shared2 = std::sync::Arc::clone(&usage_shared);
            let tx2 = tx.clone();

            let res = match &runtime2.cfg {
                ResolvedProviderConfig::Gemini(gemini_cfg2) => {
                    gemini_stream_generate(
                        gemini_cfg2,
                        &prompt.system,
                        &prompt.user,
                        temperature2,
                        max_output_tokens2,
                        move |event| {
                            let output_shared2 = std::sync::Arc::clone(&output_shared2);
                            let usage_shared2 = std::sync::Arc::clone(&usage_shared2);
                            let tx2 = tx2.clone();

                            async move {
                                match event {
                                    GeminiStreamEvent::Usage(u) => {
                                        *usage_shared2.lock().await = Some(Usage {
                                            prompt_tokens: u.prompt_tokens,
                                            completion_tokens: u.completion_tokens,
                                        });
                                        Ok(())
                                    }
                                    GeminiStreamEvent::Delta(delta) => {
                                        {
                                            let mut out = output_shared2.lock().await;
                                            out.push_str(&delta);
                                        }

                                        tx2.send(Ok(Frame::data(Bytes::from(sse_event(
                                            "token",
                                            &serde_json::json!({"text": delta}).to_string(),
                                        )))))
                                        .await
                                        .map_err(|e| {
                                            Box::new(std::io::Error::other(e.to_string())) as Error
                                        })?;
                                        Ok(())
                                    }
                                }
                            }
                        },
                    )
                    .await
                }
                _ => match generate_text_for_runtime(
                    &runtime2,
                    &prompt.system,
                    &prompt.user,
                    temperature2,
                    max_output_tokens2,
                    Some(&idempotency_key2),
                )
                .await
                {
                    Ok((text, usage)) => {
                        {
                            let mut out = output_shared.lock().await;
                            out.push_str(&text);
                        }
                        *usage_shared.lock().await = Some(usage);
                        let _ = tx
                            .send(Ok(Frame::data(Bytes::from(sse_event(
                                "token",
                                &serde_json::json!({"text": text}).to_string(),
                            )))))
                            .await;
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
            };

            if let Err(e) = res {
                let _ = tx
                    .send(Ok(Frame::data(Bytes::from(sse_event(
                        "error",
                        &serde_json::json!({"code":"upstream_error","message":e.to_string()})
                            .to_string(),
                    )))))
                    .await;
                return;
            }

            let output_text = output_shared.lock().await.clone();
            let usage = usage_shared.lock().await.clone();

            let usage = match usage {
                Some(u) => u,
                None => Usage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                },
            };

            let cost_usd = pricing2
                .map(|p| {
                    compute_cost_usd(
                        p,
                        usage.prompt_tokens as u32,
                        usage.completion_tokens as u32,
                    )
                })
                .unwrap_or(0.0);

            let result_json = coerce_risk_check_result(&output_text);

            let insert_result = insert_usage_event(
                pool,
                &tenant_id,
                EVENT_TYPE,
                &idempotency_key2,
                &provider2,
                &model2,
                usage.prompt_tokens,
                usage.completion_tokens,
                cost_usd,
            )
            .await;

            if let Err(err) = insert_result {
                if err
                    .as_database_error()
                    .is_some_and(|e| e.is_unique_violation())
                {
                    // ok
                } else {
                    let _ = tx
                        .send(Ok(Frame::data(Bytes::from(sse_event(
                            "error",
                            &serde_json::json!({"code":"tidb_error","message":err.to_string()})
                                .to_string(),
                        )))))
                        .await;
                    return;
                }
            }

            let final_payload = serde_json::json!({
              "result": result_json,
              "usage": usage,
              "cost_usd": cost_usd,
              "request_id": request_id,
              "tenant_id": tenant_id,
              "action_type": action_type,
              "note": note,
              "trial_started_at_ms": trial_started_at_ms,
              "budget_usd_per_day": budget_usd_per_day
            });

            let _ = tx
                .send(Ok(Frame::data(Bytes::from(sse_event(
                    "final",
                    &final_payload.to_string(),
                )))))
                .await;
        });

        let body = StreamBody::new(ReceiverStream::new(rx));
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream; charset=utf-8")
            .header("cache-control", "no-cache, no-transform")
            .body(ResponseBody::from(body))?);
    }

    // Non-streaming mode: call provider once and return JSON.
    let prompt = build_risk_check_prompt(RiskCheckMessageArgs {
        action_type: &parsed.action_type,
        note: parsed.note.as_deref(),
    });
    let (text, usage) = generate_text_for_runtime(
        &runtime,
        &prompt.system,
        &prompt.user,
        temperature,
        max_output_tokens,
        Some(&idempotency_key),
    )
    .await?;
    let cost_usd = pricing
        .map(|p| {
            compute_cost_usd(
                p,
                usage.prompt_tokens as u32,
                usage.completion_tokens as u32,
            )
        })
        .unwrap_or(0.0);
    let (content, usage, cost_usd) = (text, usage, cost_usd);

    let content = content.trim().to_string();

    let result_json = coerce_risk_check_result(&content);

    let insert_result = insert_usage_event(
        pool,
        &parsed.tenant_id,
        EVENT_TYPE,
        &idempotency_key,
        &provider,
        &model,
        usage.prompt_tokens,
        usage.completion_tokens,
        cost_usd,
    )
    .await;

    if let Err(err) = insert_result {
        if !err
            .as_database_error()
            .is_some_and(|e| e.is_unique_violation())
        {
            return Err(Box::new(err));
        }
    }

    json_response(
        StatusCode::OK,
        serde_json::json!({"ok": true, "result": result_json, "usage": usage, "cost_usd": cost_usd}),
    )
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
    let method = req.method().clone();
    let headers = req.headers().clone();
    let uri = req.uri().clone();
    let bytes = req.into_body().collect().await?.to_bytes();
    handle_risk_check(&method, &headers, &uri, bytes).await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    run(service_fn(handler)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coerce_risk_check_result_parses_fenced_json() {
        let raw = r#"```json
{"risk_level":"low","allowed":true,"reason":"ok"}
```"#;
        let v = coerce_risk_check_result(raw);
        assert_eq!(v["risk_level"], "low");
        assert_eq!(v["allowed"], true);
        assert_eq!(v["reason"], "ok");
    }

    #[test]
    fn coerce_risk_check_result_defaults_to_disallow_on_invalid_output() {
        let raw = r#"{"risk_level":"medium","allowed":false,"reason":"Cannot assess"#;
        let v = coerce_risk_check_result(raw);
        assert_eq!(v["risk_level"], "medium");
        assert_eq!(v["allowed"], false);
    }

    #[test]
    fn agent_mode_parses_mode_query() {
        let uri: hyper::Uri = "/api/chat/risk_check?mode=agent".parse().unwrap();
        assert!(is_agent_mode(&uri));
    }

    #[test]
    fn build_agent_prompt_includes_stable_markdown_headings() {
        let (system, _user) = build_agent_prompt("hello", None);
        for heading in [
            "## Quick Summary",
            "## Title Options (10)",
            "## Description",
            "### Hooks (3)",
            "### Full Description",
            "## Pinned Comment",
            "## Hook / Script Outline",
            "## A/B Tip",
        ] {
            assert!(
                system.contains(heading),
                "expected system prompt to contain heading: {heading}"
            );
        }
    }

    #[test]
    fn build_agent_prompt_marks_missing_required_context() {
        let (_system, user) = build_agent_prompt("hello", None);
        assert!(user.contains("- Title: （缺失）"));
        assert!(user.contains("- Topic: （缺失）"));
        assert!(user.contains("- Target audience: （缺失）"));
        assert!(user.contains("- New/controversial angle: （缺失）"));
        assert!(user.contains("Missing required context: title, topic, target_audience, new_angle"));
    }

    #[test]
    fn build_agent_prompt_does_not_mark_missing_when_required_present() {
        let vc = VideoContext {
            title: Some("Working title".to_string()),
            topic: Some("Topic".to_string()),
            target_audience: Some("Audience".to_string()),
            new_angle: Some("Angle".to_string()),
            existing_description: None,
            outline: None,
            top_comments: None,
            notes: None,
        };
        let (_system, user) = build_agent_prompt("hello", Some(&vc));
        assert!(!user.contains("Missing required context:"));
        assert!(!user.contains("（缺失）"));
    }

    #[test]
    fn agent_wants_run_task_keyword_gating() {
        assert!(agent_wants_run_task("刷新"));
        assert!(agent_wants_run_task("Run now"));
        assert!(agent_wants_run_task("dispatch the worker tick"));
        assert!(!agent_wants_run_task(
            "Just generate a content pack please."
        ));
    }

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
        let body = Bytes::from(
            r#"{"request_id":"r1","tenant_id":"t1","trial_started_at_ms":0,"budget_usd_per_day":1,"action_type":"change_title"}"#,
        );

        let response = handle_risk_check(&Method::POST, &headers, &uri, body).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            json.get("error").and_then(|v| v.as_str()),
            Some("config_error")
        );
    }

    #[test]
    fn provider_v1_endpoint_handles_both_base_shapes() {
        assert_eq!(
            provider_v1_endpoint("https://api.openai.com/v1", "responses"),
            "https://api.openai.com/v1/responses"
        );
        assert_eq!(
            provider_v1_endpoint("https://api.openai.com", "responses"),
            "https://api.openai.com/v1/responses"
        );
    }

    #[test]
    fn extracts_openai_text_and_usage() {
        let payload = serde_json::json!({
          "output":[{"content":[{"type":"output_text","text":"hello world"}]}],
          "usage":{"input_tokens":12,"output_tokens":34}
        });
        assert_eq!(openai_extract_text(&payload), "hello world");
        let usage = openai_extract_usage(&payload).expect("usage");
        assert_eq!(usage.prompt_tokens, 12);
        assert_eq!(usage.completion_tokens, 34);
    }

    #[test]
    fn extracts_anthropic_text_and_usage() {
        let payload = serde_json::json!({
          "content":[{"type":"text","text":"risk ok"}],
          "usage":{"input_tokens":7,"output_tokens":9}
        });
        assert_eq!(anthropic_extract_text(&payload), "risk ok");
        let usage = anthropic_extract_usage(&payload).expect("usage");
        assert_eq!(usage.prompt_tokens, 7);
        assert_eq!(usage.completion_tokens, 9);
    }
}

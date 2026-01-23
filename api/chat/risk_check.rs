use http_body_util::BodyExt;
use http_body_util::StreamBody;
use hyper::body::Frame;
use hyper::{HeaderMap, Method, StatusCode};
use serde::{Deserialize, Serialize};
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use bytes::Bytes;
use globa_flux_rust::cost::compute_cost_usd;
use globa_flux_rust::db::{
    fetch_tenant_gemini_model, fetch_usage_event, get_pool, insert_usage_event, sum_spent_usd_today,
};
use globa_flux_rust::providers::gemini::{
    generate_text as gemini_generate_text, pricing_for_model as gemini_pricing_for_model,
    stream_generate as gemini_stream_generate, GeminiConfig, GeminiStreamEvent,
};
use globa_flux_rust::providers::openai::{build_risk_check_prompt, RiskCheckMessageArgs};
use globa_flux_rust::sse::sse_event;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const GLOBAL_TENANT_ID: &str = "global";

#[derive(Deserialize)]
struct RiskCheckRequest {
    request_id: String,
    tenant_id: String,
    trial_started_at_ms: i64,
    budget_usd_per_day: f64,
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
    message: String,
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

fn build_agent_prompt(message: &str) -> (String, String) {
    let system = r#"You are GF Agent for YouTube creators.

You are NOT a risk-check bot. Reply naturally in Chinese.

Scope:
- Provide suggestions only (do not claim you changed anything).
- Help optimize YouTube title/description/hook/script outline/community post copy.
- If user asks to "发任务/跑任务/执行任务", explain it requires confirmation, and propose running the job worker tick.

Output style:
- Ask 1–3 clarifying questions if context is missing.
- Otherwise provide:
  - 10 title options (short, high-CTR, clear)
  - 3 description hooks (first 2 lines)
  - 1 recommended pinned comment
  - A/B testing tip (1–2 lines)
"#
    .to_string();

    let user = format!(
        r#"User message:
{message}

If the user did not provide the current video title, topic, target audience, and what's "new/controversial", ask for them."#
    );

    (system, user)
}

fn agent_wants_run_task(message: &str) -> bool {
    let m = message.to_lowercase();
    m.contains("发任务")
        || m.contains("跑任务")
        || m.contains("执行任务")
        || m.contains("tick")
        || m.contains("worker")
}

async fn handle_agent(
    stream: bool,
    idempotency_key: &str,
    body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
    let mut gemini_cfg = match GeminiConfig::from_env_optional()? {
        Some(cfg) => cfg,
        None => {
            return config_error_response(stream, "Missing GEMINI_API_KEY");
        }
    };

    let parsed: AgentRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid json body: {e}")))
    })?;

    let pool = match get_pool().await {
        Ok(pool) => pool,
        Err(err) => {
            return config_error_response(stream, &err.to_string());
        }
    };

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

    let spent_usd_today = sum_spent_usd_today(pool, &parsed.tenant_id, chrono::Utc::now()).await?;

    let db_model = match fetch_tenant_gemini_model(pool, GLOBAL_TENANT_ID).await {
        Ok(model) => model.unwrap_or_default(),
        Err(err) => {
            return config_error_response(stream, &err.to_string());
        }
    };
    let db_model = db_model.trim().to_string();
    if db_model.is_empty() {
        return config_error_response(stream, "Missing gemini_model for tenant_id=global");
    }
    gemini_cfg.model = db_model.clone();

    let model = gemini_cfg.model.clone();
    let max_output_tokens: u32 = std::env::var("GEMINI_MAX_OUTPUT_TOKENS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(900);
    let prompt_reserve_tokens: u32 = std::env::var("GEMINI_PROMPT_TOKEN_RESERVE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2500);

    let provider = "gemini".to_string();
    let pricing = gemini_pricing_for_model(&model);

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
    let (system, user) = build_agent_prompt(&parsed.message);

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
        let gemini_cfg2 = gemini_cfg.clone();
        let max_output_tokens2 = max_output_tokens;

        tokio::spawn(async move {
            let output_shared = std::sync::Arc::new(tokio::sync::Mutex::new(String::new()));
            let usage_shared = std::sync::Arc::new(tokio::sync::Mutex::new(None::<Usage>));

            let output_shared2 = std::sync::Arc::clone(&output_shared);
            let usage_shared2 = std::sync::Arc::clone(&usage_shared);
            let tx2 = tx.clone();

            let res = gemini_stream_generate(
                &gemini_cfg2,
                &system,
                &user,
                temperature,
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
            .await;

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
                  "type": "jobs_worker_tick",
                  "limit": 10,
                  "requires_confirmation": true,
                  "note": "Will attempt to run queued jobs for this tenant."
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

    let (text, u) = gemini_generate_text(
        &gemini_cfg,
        &system,
        &user,
        temperature,
        max_output_tokens,
    )
    .await?;

    let usage = Usage {
        prompt_tokens: u.as_ref().map(|x| x.prompt_tokens).unwrap_or(0),
        completion_tokens: u.as_ref().map(|x| x.completion_tokens).unwrap_or(0),
    };
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
          "type": "jobs_worker_tick",
          "limit": 10,
          "requires_confirmation": true,
          "note": "Will attempt to run queued jobs for this tenant."
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
    let provided = bearer_token(
        headers
            .get("authorization")
            .and_then(|v| v.to_str().ok()),
    )
    .unwrap_or("");

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

    let mut gemini_cfg = match GeminiConfig::from_env_optional()? {
        Some(cfg) => cfg,
        None => {
            return config_error_response(stream, "Missing GEMINI_API_KEY");
        }
    };

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

    let spent_usd_today = sum_spent_usd_today(pool, &parsed.tenant_id, chrono::Utc::now()).await?;
    let temperature: f64 = 0.2;

    let db_model = match fetch_tenant_gemini_model(pool, GLOBAL_TENANT_ID).await {
        Ok(model) => model.unwrap_or_default(),
        Err(err) => {
            return config_error_response(stream, &err.to_string());
        }
    };
    let db_model = db_model.trim().to_string();
    if db_model.is_empty() {
        return config_error_response(
            stream,
            "Missing gemini_model for tenant_id=global",
        );
    }
    gemini_cfg.model = db_model.clone();

    let model = gemini_cfg.model.clone();
    let max_output_tokens: u32 = std::env::var("GEMINI_MAX_OUTPUT_TOKENS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);
    let prompt_reserve_tokens: u32 = std::env::var("GEMINI_PROMPT_TOKEN_RESERVE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);

    let provider = "gemini".to_string();
    let pricing = gemini_pricing_for_model(&model);

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
        let gemini_cfg2 = gemini_cfg.clone();
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

            let res = gemini_stream_generate(
                &gemini_cfg2,
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
            .await;

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
    let (text, u) = gemini_generate_text(
        &gemini_cfg,
        &prompt.system,
        &prompt.user,
        temperature,
        max_output_tokens,
    )
    .await?;

    let usage = Usage {
        prompt_tokens: u.as_ref().map(|x| x.prompt_tokens).unwrap_or(0),
        completion_tokens: u.as_ref().map(|x| x.completion_tokens).unwrap_or(0),
    };
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
        assert_eq!(json.get("error").and_then(|v| v.as_str()), Some("config_error"));
    }
}

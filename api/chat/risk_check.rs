use http_body_util::BodyExt;
use http_body_util::StreamBody;
use hyper::body::Frame;
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use bytes::Bytes;
use globa_flux_rust::cost::{compute_cost_usd, ModelPricingUsdPerMToken};
use globa_flux_rust::db::{
    fetch_tenant_gemini_model, fetch_usage_event, get_pool, insert_usage_event, sum_spent_usd_today,
};
use globa_flux_rust::providers::gemini::{
    generate_text as gemini_generate_text, pricing_for_model as gemini_pricing_for_model,
    stream_generate as gemini_stream_generate, GeminiConfig, GeminiStreamEvent,
};
use globa_flux_rust::providers::openai::{
    build_chat_completions_request, build_risk_check_messages, build_risk_check_prompt,
    openai_client, openai_client_with_idempotency, pricing_for_model as openai_pricing_for_model,
    ChatCompletionsPayloadArgs, RiskCheckMessageArgs,
};
use globa_flux_rust::sse::sse_event;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

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

fn bearer_token(header_value: Option<&str>) -> Option<&str> {
    let value = header_value?;
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
}

fn wants_stream(req: &Request) -> bool {
    if let Some(q) = req.uri().query() {
        for part in q.split('&') {
            if part == "stream=1" {
                return true;
            }
        }
    }

    req.headers()
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

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
    if req.method() != "POST" {
        return json_response(
            StatusCode::METHOD_NOT_ALLOWED,
            serde_json::json!({"ok": false, "error": "method_not_allowed"}),
        );
    }

    let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
    let provided = bearer_token(
        req.headers()
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

    let stream = wants_stream(&req);

    let idempotency_key = req
        .headers()
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

    let bytes = req.into_body().collect().await?.to_bytes();
    let parsed: RiskCheckRequest = serde_json::from_slice(&bytes).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid json body: {e}")))
    })?;

    // TiDB budget precheck + idempotent usage accounting.
    // Note: Hydrogen passes `budget_usd_per_day` (trial entitlements), Rust enforces it here.
    let pool = get_pool().await?;

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

    let mut gemini_cfg = GeminiConfig::from_env_optional()?;
    if let Some(cfg) = gemini_cfg.as_mut() {
        if let Some(model) = fetch_tenant_gemini_model(pool, &parsed.tenant_id).await? {
            let model = model.trim();
            if !model.is_empty() {
                cfg.model = model.to_string();
            }
        }
    }
    let use_gemini = gemini_cfg.is_some();

    let (provider, model, max_output_tokens, prompt_reserve_tokens, pricing) = if use_gemini {
        let model = gemini_cfg
            .as_ref()
            .map(|c| c.model.clone())
            .unwrap_or_else(|| "gemini-1.5-flash".to_string());
        let max_output_tokens: u32 = std::env::var("GEMINI_MAX_OUTPUT_TOKENS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(600);
        let prompt_reserve_tokens: u32 = std::env::var("GEMINI_PROMPT_TOKEN_RESERVE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2000);

        (
            "gemini".to_string(),
            model.clone(),
            max_output_tokens,
            prompt_reserve_tokens,
            gemini_pricing_for_model(&model),
        )
    } else {
        let model = std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string());
        let max_output_tokens: u32 = std::env::var("OPENAI_MAX_COMPLETION_TOKENS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(600);
        let prompt_reserve_tokens: u32 = std::env::var("OPENAI_PROMPT_TOKEN_RESERVE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2000);

        let pricing: ModelPricingUsdPerMToken = openai_pricing_for_model(&model).ok_or_else(|| {
      Box::new(std::io::Error::other(format!(
        "Missing pricing for model {model}. Set OPENAI_PRICE_PROMPT_USD_PER_M_TOKEN and OPENAI_PRICE_COMPLETION_USD_PER_M_TOKEN."
      ))) as Error
    })?;

        (
            "openai".to_string(),
            model,
            max_output_tokens,
            prompt_reserve_tokens,
            Some(pricing),
        )
    };

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
        let use_gemini2 = use_gemini;
        let gemini_cfg2 = gemini_cfg.clone();
        let max_output_tokens2 = max_output_tokens;
        let temperature2 = temperature;

        tokio::spawn(async move {
            let mut output_text = String::new();
            let mut usage: Option<Usage> = None;

            if use_gemini2 {
                let cfg = match gemini_cfg2 {
                    Some(c) => c,
                    None => {
                        let _ = tx
                            .send(Ok(Frame::data(Bytes::from(sse_event(
                                "error",
                                r#"{"code":"config_error","message":"Missing GEMINI_API_KEY"}"#,
                            )))))
                            .await;
                        return;
                    }
                };

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
                    &cfg,
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

                output_text = output_shared.lock().await.clone();
                usage = usage_shared.lock().await.clone();
            } else {
                let api_key = match std::env::var("OPENAI_API_KEY") {
                    Ok(v) => v,
                    Err(_) => {
                        let _ = tx
                            .send(Ok(Frame::data(Bytes::from(sse_event(
                                "error",
                                r#"{"code":"config_error","message":"Missing OPENAI_API_KEY"}"#,
                            )))))
                            .await;
                        return;
                    }
                };

                let client = match openai_client_with_idempotency(&api_key, &idempotency_key2) {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = tx
                            .send(Ok(Frame::data(Bytes::from(sse_event(
                                "error",
                                &serde_json::json!({"code":"config_error","message":e.to_string()})
                                    .to_string(),
                            )))))
                            .await;
                        return;
                    }
                };

                let messages = match build_risk_check_messages(RiskCheckMessageArgs {
                    action_type: &action_type,
                    note: note.as_deref(),
                }) {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = tx
                            .send(Ok(Frame::data(Bytes::from(sse_event(
                                "error",
                                &serde_json::json!({"code":"bad_request","message":e.to_string()})
                                    .to_string(),
                            )))))
                            .await;
                        return;
                    }
                };

                let request = match build_chat_completions_request(ChatCompletionsPayloadArgs {
                    model: &model2,
                    messages,
                    max_tokens: max_output_tokens2,
                    stream: true,
                }) {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx
                            .send(Ok(Frame::data(Bytes::from(sse_event(
                                "error",
                                &serde_json::json!({"code":"bad_request","message":e.to_string()})
                                    .to_string(),
                            )))))
                            .await;
                        return;
                    }
                };

                let mut stream = match client.chat().create_stream(request).await {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = tx
              .send(Ok(Frame::data(Bytes::from(sse_event(
                "error",
                &serde_json::json!({"code":"upstream_error","message":e.to_string()}).to_string(),
              )))))
              .await;
                        return;
                    }
                };

                while let Some(next) = stream.next().await {
                    match next {
                        Ok(chunk) => {
                            if let Some(u) = chunk.usage {
                                usage = Some(Usage {
                                    prompt_tokens: u.prompt_tokens as i32,
                                    completion_tokens: u.completion_tokens as i32,
                                });
                                continue;
                            }

                            if let Some(token) = chunk
                                .choices
                                .get(0)
                                .and_then(|c| c.delta.content.clone())
                                .filter(|t| !t.is_empty())
                            {
                                output_text.push_str(&token);
                                let _ = tx
                                    .send(Ok(Frame::data(Bytes::from(sse_event(
                                        "token",
                                        &serde_json::json!({"text": token}).to_string(),
                                    )))))
                                    .await;
                            }
                        }
                        Err(e) => {
                            let _ = tx
                .send(Ok(Frame::data(Bytes::from(sse_event(
                  "error",
                  &serde_json::json!({"code":"upstream_stream_error","message":e.to_string()}).to_string(),
                )))))
                .await;
                            return;
                        }
                    }
                }
            }

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

            let result_json = serde_json::from_str::<serde_json::Value>(output_text.trim())
                .unwrap_or_else(|_| {
                    serde_json::json!({
                      "risk_level": "medium",
                      "allowed": true,
                      "reason": output_text.trim()
                    })
                });

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
    let (content, usage, cost_usd) = if use_gemini {
        let cfg = gemini_cfg
            .ok_or_else(|| Box::new(std::io::Error::other("Missing GEMINI_API_KEY")) as Error)?;
        let prompt = build_risk_check_prompt(RiskCheckMessageArgs {
            action_type: &parsed.action_type,
            note: parsed.note.as_deref(),
        });
        let (text, u) = gemini_generate_text(
            &cfg,
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
        (text, usage, cost_usd)
    } else {
        let api_key = std::env::var("OPENAI_API_KEY")
            .map_err(|_| Box::new(std::io::Error::other("Missing OPENAI_API_KEY")) as Error)?;
        let client = openai_client(&api_key);
        let messages = build_risk_check_messages(RiskCheckMessageArgs {
            action_type: &parsed.action_type,
            note: parsed.note.as_deref(),
        })?;
        let request = build_chat_completions_request(ChatCompletionsPayloadArgs {
            model: &model,
            messages,
            max_tokens: max_output_tokens,
            stream: false,
        })?;

        let response = client
            .chat()
            .create(request)
            .await
            .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?;

        let content = response
            .choices
            .get(0)
            .and_then(|c| c.message.content.clone())
            .unwrap_or_default()
            .trim()
            .to_string();

        let usage = Usage {
            prompt_tokens: response
                .usage
                .as_ref()
                .map(|u| u.prompt_tokens)
                .unwrap_or(0) as i32,
            completion_tokens: response
                .usage
                .as_ref()
                .map(|u| u.completion_tokens)
                .unwrap_or(0) as i32,
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
        (content, usage, cost_usd)
    };

    let content = content.trim().to_string();

    let result_json = serde_json::from_str::<serde_json::Value>(&content).unwrap_or_else(
        |_| serde_json::json!({"risk_level":"medium","allowed":true,"reason":content}),
    );

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

#[tokio::main]
async fn main() -> Result<(), Error> {
    run(service_fn(handler)).await
}

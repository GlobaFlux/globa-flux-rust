use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{ACCEPT, CONTENT_TYPE};
use hyper::{Method, Request, StatusCode};
use serde_json::Value;
use std::future::Future;
use vercel_runtime::Error;

use crate::cost::ModelPricingUsdPerMToken;

#[derive(Debug, Clone)]
pub struct GeminiUsage {
    pub prompt_tokens: i32,
    pub completion_tokens: i32,
}

#[derive(Debug, Clone)]
pub enum GeminiStreamEvent {
    Delta(String),
    Usage(GeminiUsage),
}

#[derive(Debug, Clone)]
pub struct GeminiConfig {
    pub api_key: String,
    pub model: String,
    pub api_base_url: String,
}

impl GeminiConfig {
    pub fn from_env_optional() -> Result<Option<Self>, Error> {
        let api_key = std::env::var("GEMINI_API_KEY").ok().unwrap_or_default();
        if api_key.trim().is_empty() {
            return Ok(None);
        }

        let model = std::env::var("GEMINI_MODEL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "gemini-1.5-flash-latest".to_string());

        let api_base_url = std::env::var("GEMINI_API_BASE_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "https://generativelanguage.googleapis.com/v1".to_string());

        Ok(Some(Self {
            api_key: api_key.trim().to_string(),
            model: model.trim().to_string(),
            api_base_url: api_base_url.trim().to_string(),
        }))
    }
}

pub fn pricing_for_model(model: &str) -> Option<ModelPricingUsdPerMToken> {
    // Gemini pricing varies by model and region; default to "unknown" unless overridden.
    // Allow overriding pricing without code changes (USD per 1M tokens).
    if let (Ok(prompt), Ok(completion)) = (
        std::env::var("GEMINI_PRICE_PROMPT_USD_PER_M_TOKEN"),
        std::env::var("GEMINI_PRICE_COMPLETION_USD_PER_M_TOKEN"),
    ) {
        if let (Ok(prompt), Ok(completion)) = (prompt.parse::<f64>(), completion.parse::<f64>()) {
            return Some(ModelPricingUsdPerMToken { prompt, completion });
        }
    }

    match model {
        // Keep empty by default; use env overrides to enforce budget/cost accounting.
        _ => None,
    }
}

fn model_path(model: &str) -> String {
    let m = model.trim();
    if m.starts_with("models/") {
        m.to_string()
    } else {
        format!("models/{m}")
    }
}

fn build_url(cfg: &GeminiConfig, method: &str, streaming: bool) -> String {
    let base = cfg.api_base_url.trim_end_matches('/');
    let model = model_path(&cfg.model);
    if streaming {
        // `alt=sse` is supported by Google APIs for SSE streaming in many endpoints.
        // If not supported, the response still streams JSON chunks; we parse both formats.
        format!("{base}/{model}:{method}?alt=sse&key={}", cfg.api_key)
    } else {
        format!("{base}/{model}:{method}?key={}", cfg.api_key)
    }
}

fn build_request_json(system: &str, user: &str, temperature: f64, max_output_tokens: u32) -> Value {
    serde_json::json!({
      "systemInstruction": {"parts":[{"text": system}]},
      "contents":[{"role":"user","parts":[{"text": user}]}],
      "generationConfig": {
        "temperature": temperature,
        "maxOutputTokens": max_output_tokens
      }
    })
}

fn extract_text_from_response_json(json: &Value) -> String {
    let mut out = String::new();
    let candidates = json
        .get("candidates")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for cand in candidates {
        let parts = cand
            .get("content")
            .and_then(|v| v.get("parts"))
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

fn extract_usage(json: &Value) -> Option<GeminiUsage> {
    let usage = json.get("usageMetadata")?;
    let prompt = usage.get("promptTokenCount")?.as_i64()? as i32;
    let completion = usage
        .get("candidatesTokenCount")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    Some(GeminiUsage {
        prompt_tokens: prompt,
        completion_tokens: completion,
    })
}

pub async fn generate_text(
    cfg: &GeminiConfig,
    system: &str,
    user: &str,
    temperature: f64,
    max_output_tokens: u32,
) -> Result<(String, Option<GeminiUsage>), Error> {
    let url = build_url(cfg, "generateContent", false);
    let payload = build_request_json(system, user, temperature, max_output_tokens);
    let body = serde_json::to_vec(&payload)?;

    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?
        .https_or_http()
        .enable_http1()
        .build();
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build(connector);

    let req = Request::builder()
        .method(Method::POST)
        .uri(url)
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json")
        .body(Full::new(Bytes::from(body)))
        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

    let resp = client
        .request(req)
        .await
        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

    let status = resp.status();
    let body_bytes = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?
        .to_bytes();

    if status != StatusCode::OK {
        let msg = String::from_utf8_lossy(&body_bytes).to_string();
        return Err(Box::new(std::io::Error::other(format!(
            "Gemini error (status {}): {msg}",
            status.as_u16()
        ))));
    }

    let json: Value = serde_json::from_slice(&body_bytes).map_err(|e| {
        Box::new(std::io::Error::other(format!("invalid json response: {e}"))) as Error
    })?;

    let text = extract_text_from_response_json(&json);
    let usage = extract_usage(&json);
    Ok((text, usage))
}

pub async fn stream_generate<F, Fut>(
    cfg: &GeminiConfig,
    system: &str,
    user: &str,
    temperature: f64,
    max_output_tokens: u32,
    mut on_event: F,
) -> Result<(), Error>
where
    F: FnMut(GeminiStreamEvent) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let url = build_url(cfg, "streamGenerateContent", true);
    let payload = build_request_json(system, user, temperature, max_output_tokens);
    let body = serde_json::to_vec(&payload)?;

    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?
        .https_or_http()
        .enable_http1()
        .build();
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build(connector);

    let req = Request::builder()
        .method(Method::POST)
        .uri(url)
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "text/event-stream")
        .body(Full::new(Bytes::from(body)))
        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

    let resp = client
        .request(req)
        .await
        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

    let status = resp.status();
    if status != StatusCode::OK {
        let body_bytes = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?
            .to_bytes();
        let msg = String::from_utf8_lossy(&body_bytes).to_string();
        return Err(Box::new(std::io::Error::other(format!(
            "Gemini stream error (status {}): {msg}",
            status.as_u16()
        ))));
    }

    let mut body = resp.into_body();
    let mut buf: Vec<u8> = Vec::new();

    while let Some(next) = body.frame().await {
        let frame = next.map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;
        let data = match frame.into_data() {
            Ok(d) => d,
            Err(_) => continue,
        };

        buf.extend_from_slice(&data);
        while let Some(pos) = buf.iter().position(|b| *b == b'\n') {
            let mut line = buf.drain(..=pos).collect::<Vec<u8>>();
            if line.ends_with(b"\n") {
                line.pop();
            }
            if line.ends_with(b"\r") {
                line.pop();
            }

            if line.is_empty() {
                continue;
            }

            let line_str = match std::str::from_utf8(&line) {
                Ok(s) => s.trim(),
                Err(_) => continue,
            };

            let payload_str = if let Some(rest) = line_str.strip_prefix("data:") {
                rest.trim()
            } else {
                line_str
            };

            if payload_str.is_empty() || payload_str == "[DONE]" {
                continue;
            }

            // Handle both SSE (`data: {json}`) and JSON-lines (`{json}`) streaming formats.
            if !payload_str.starts_with('{') {
                continue;
            }

            let json: Value = match serde_json::from_str(payload_str) {
                Ok(v) => v,
                Err(_) => continue,
            };

            if let Some(usage) = extract_usage(&json) {
                on_event(GeminiStreamEvent::Usage(usage)).await?;
            }

            let delta = extract_text_from_response_json(&json);
            if !delta.is_empty() {
                on_event(GeminiStreamEvent::Delta(delta)).await?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_path_prefixes_models() {
        assert_eq!(model_path("gemini-1.5-flash"), "models/gemini-1.5-flash");
        assert_eq!(
            model_path("models/gemini-1.5-flash"),
            "models/gemini-1.5-flash"
        );
    }

    #[test]
    fn extract_text_pulls_candidate_parts() {
        let json: Value = serde_json::from_str(
            r#"{"candidates":[{"content":{"parts":[{"text":"a"},{"text":"b"}]}}]}"#,
        )
        .unwrap();
        assert_eq!(extract_text_from_response_json(&json), "ab");
    }
}

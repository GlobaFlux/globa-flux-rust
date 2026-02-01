use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode, Uri};
use serde::Deserialize;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use chrono::{Duration, Utc};

use globa_flux_rust::db::{
  fetch_youtube_channel_id, get_pool, upsert_video_daily_metric, upsert_youtube_connection,
};
use globa_flux_rust::decision_engine::{compute_decision, DecisionEngineConfig};
use globa_flux_rust::providers::youtube::{
  build_authorize_url, exchange_code_for_tokens, youtube_oauth_client_from_env,
};
use globa_flux_rust::providers::youtube_analytics::{
  fetch_video_daily_metrics, youtube_analytics_error_to_vercel_error,
};
use globa_flux_rust::providers::youtube_api::fetch_my_channel_id;

fn bearer_token(header_value: Option<&str>) -> Option<&str> {
  let value = header_value?;
  value.strip_prefix("Bearer ").or_else(|| value.strip_prefix("bearer "))
}

fn json_response(status: StatusCode, value: serde_json::Value) -> Result<Response<ResponseBody>, Error> {
  Ok(
    Response::builder()
      .status(status)
      .header("content-type", "application/json; charset=utf-8")
      .body(ResponseBody::from(value))?,
  )
}

fn decode_hex_digit(b: u8) -> Option<u8> {
  match b {
    b'0'..=b'9' => Some(b - b'0'),
    b'a'..=b'f' => Some(b - b'a' + 10),
    b'A'..=b'F' => Some(b - b'A' + 10),
    _ => None,
  }
}

fn percent_decode(input: &str) -> Option<String> {
  let bytes = input.as_bytes();
  let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
  let mut i = 0;
  while i < bytes.len() {
    match bytes[i] {
      b'%' if i + 2 < bytes.len() => {
        let hi = decode_hex_digit(bytes[i + 1])?;
        let lo = decode_hex_digit(bytes[i + 2])?;
        out.push((hi << 4) | lo);
        i += 3;
      }
      b'+' => {
        out.push(b' ');
        i += 1;
      }
      b => {
        out.push(b);
        i += 1;
      }
    }
  }
  String::from_utf8(out).ok()
}

fn get_query_param(uri: &Uri, key: &str) -> Option<String> {
  let query = uri.query()?;
  for part in query.split('&') {
    let mut it = part.splitn(2, '=');
    let k = it.next().unwrap_or("");
    if k != key {
      continue;
    }
    let v = it.next().unwrap_or("");
    return percent_decode(v).or_else(|| Some(v.to_string()));
  }
  None
}

#[derive(Deserialize)]
struct StartRequest {
  state: String,
}

async fn handle_start(method: &Method, headers: &HeaderMap, body: Bytes) -> Result<Response<ResponseBody>, Error> {
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

  let parsed: StartRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

  if parsed.state.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "state is required"}),
    );
  }

  let (client, _redirect) = youtube_oauth_client_from_env()?;
  let (authorize_url, state) = build_authorize_url(&client, Some(parsed.state));

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "authorize_url": authorize_url, "state": state}),
  )
}

#[derive(Deserialize)]
struct ExchangeRequest {
  tenant_id: String,
  code: String,
}

async fn handle_exchange(method: &Method, headers: &HeaderMap, body: Bytes) -> Result<Response<ResponseBody>, Error> {
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

  let has_tidb_url = std::env::var("TIDB_DATABASE_URL")
    .or_else(|_| std::env::var("DATABASE_URL"))
    .map(|v| !v.is_empty())
    .unwrap_or(false);
  if !has_tidb_url {
    return json_response(
      StatusCode::NOT_IMPLEMENTED,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
    );
  }

  let parsed: ExchangeRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

  if parsed.tenant_id.is_empty() || parsed.code.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id and code are required"}),
    );
  }

  let (client, _redirect) = youtube_oauth_client_from_env()?;
  let tokens = exchange_code_for_tokens(&client, &parsed.code).await?;
  let channel_id = fetch_my_channel_id(&tokens.access_token).await?;

  let pool = get_pool().await?;
  upsert_youtube_connection(pool, &parsed.tenant_id, &channel_id, &tokens)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

  // Hybrid onboarding: generate the first decision quickly after OAuth connect.
  // Uses the last 7 completed days (ending yesterday) as the decision window.
  let as_of_dt = Utc::now().date_naive();
  let start_dt = as_of_dt - Duration::days(7);
  let end_dt = as_of_dt - Duration::days(1);

  let metrics = fetch_video_daily_metrics(&tokens.access_token, start_dt, end_dt)
    .await
    .map_err(youtube_analytics_error_to_vercel_error)?;

  for row in metrics.iter() {
    upsert_video_daily_metric(
      pool,
      &parsed.tenant_id,
      &channel_id,
      row.dt,
      &row.video_id,
      row.estimated_revenue_usd,
      row.impressions,
      row.views,
    )
    .await?;
  }

  let decision = compute_decision(
    metrics.as_slice(),
    as_of_dt,
    start_dt,
    end_dt,
    DecisionEngineConfig::default(),
  );

  let evidence_json = serde_json::to_string(&decision.evidence).unwrap_or_else(|_| "[]".to_string());
  let forbidden_json = serde_json::to_string(&decision.forbidden).unwrap_or_else(|_| "[]".to_string());
  let reevaluate_json = serde_json::to_string(&decision.reevaluate).unwrap_or_else(|_| "[]".to_string());

  sqlx::query(
    r#"
      INSERT INTO decision_daily (
        tenant_id, channel_id, as_of_dt,
        direction, confidence,
        evidence_json, forbidden_json, reevaluate_json
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        direction = VALUES(direction),
        confidence = VALUES(confidence),
        evidence_json = VALUES(evidence_json),
        forbidden_json = VALUES(forbidden_json),
        reevaluate_json = VALUES(reevaluate_json),
        updated_at = CURRENT_TIMESTAMP(3);
    "#,
  )
  .bind(&parsed.tenant_id)
  .bind(&channel_id)
  .bind(as_of_dt)
  .bind(&decision.direction)
  .bind(decision.confidence)
  .bind(evidence_json)
  .bind(forbidden_json)
  .bind(reevaluate_json)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "channel_id": channel_id, "first_decision_as_of_dt": as_of_dt.to_string()}),
  )
}

async fn handle_status(method: &Method, headers: &HeaderMap, uri: &Uri) -> Result<Response<ResponseBody>, Error> {
  if method != Method::GET {
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

  let tenant_id = get_query_param(uri, "tenant_id").unwrap_or_default();
  if tenant_id.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
    );
  }

  let has_tidb_url = std::env::var("TIDB_DATABASE_URL")
    .or_else(|_| std::env::var("DATABASE_URL"))
    .map(|v| !v.is_empty())
    .unwrap_or(false);
  if !has_tidb_url {
    return json_response(
      StatusCode::NOT_IMPLEMENTED,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
    );
  }

  let pool = get_pool().await?;
  let channel_id = fetch_youtube_channel_id(pool, &tenant_id).await?;
  let connected = channel_id.is_some();

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "connected": connected, "channel_id": channel_id}),
  )
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let action = get_query_param(req.uri(), "action").unwrap_or_default();

  match action.as_str() {
    "status" => handle_status(req.method(), req.headers(), req.uri()).await,
    "start" => {
      let method = req.method().clone();
      let headers = req.headers().clone();
      let bytes = req.into_body().collect().await?.to_bytes();
      handle_start(&method, &headers, bytes).await
    }
    "exchange" => {
      let method = req.method().clone();
      let headers = req.headers().clone();
      let bytes = req.into_body().collect().await?.to_bytes();
      handle_exchange(&method, &headers, bytes).await
    }
    "" => json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "action is required"}),
    ),
    _ => json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_found"}),
    ),
  }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  run(service_fn(handler)).await
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn start_returns_authorize_url_with_provided_state() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
    std::env::set_var("YOUTUBE_CLIENT_ID", "id");
    std::env::set_var("YOUTUBE_CLIENT_SECRET", "secret2");
    std::env::set_var("YOUTUBE_REDIRECT_URI", "https://example.com/cb");

    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer secret".parse().unwrap());
    headers.insert("content-type", "application/json".parse().unwrap());

    let body = Bytes::from(r#"{"state":"state123"}"#);
    let response = handle_start(&Method::POST, &headers, body).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed.get("ok").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
      parsed.get("state").and_then(|v| v.as_str()),
      Some("state123")
    );
    let url = parsed.get("authorize_url").and_then(|v| v.as_str()).unwrap();
    assert!(url.contains("accounts.google.com/o/oauth2/v2/auth"));
    assert!(url.contains("state=state123"));
  }

  #[tokio::test]
  async fn status_returns_unauthorized_when_missing_internal_token() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
    let headers = HeaderMap::new();
    let uri: Uri = "/api/oauth/youtube/status?tenant_id=t1".parse().unwrap();
    let response = handle_status(&Method::GET, &headers, &uri).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  }
}


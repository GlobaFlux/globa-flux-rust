use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode};
use serde::Deserialize;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::db::{consume_daily_usage_event, get_pool};

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

#[derive(Deserialize)]
struct ConsumeRequest {
  tenant_id: String,
  idempotency_key: String,
  limit: i64,
}

async fn handle_consume(method: &Method, headers: &HeaderMap, body: Bytes) -> Result<Response<ResponseBody>, Error> {
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

  let parsed: ConsumeRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

  if parsed.tenant_id.is_empty() || parsed.idempotency_key.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id and idempotency_key are required"}),
    );
  }

  const EVENT_TYPE: &str = "chat_risk_check_count";

  if parsed.limit <= 0 {
    return json_response(
      StatusCode::TOO_MANY_REQUESTS,
      serde_json::json!({"ok": false, "error": "entitlement_exceeded", "message": "Daily limit is 0", "day_key": chrono::Utc::now().format("%Y-%m-%d").to_string(), "used": 0, "limit": parsed.limit}),
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
  let now = chrono::Utc::now();

  let result = consume_daily_usage_event(
    pool,
    &parsed.tenant_id,
    EVENT_TYPE,
    &parsed.idempotency_key,
    parsed.limit,
    now,
  )
  .await?;

  if !result.allowed {
    return json_response(
      StatusCode::TOO_MANY_REQUESTS,
      serde_json::json!({"ok": false, "error": "entitlement_exceeded", "message": "Daily chat_risk_checks_per_day limit exceeded", "day_key": result.day_key, "used": result.used, "limit": parsed.limit}),
    );
  }

  let remaining = std::cmp::max(0, parsed.limit - result.used);

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "day_key": result.day_key, "used": result.used, "limit": parsed.limit, "remaining": remaining}),
  )
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let method = req.method().clone();
  let headers = req.headers().clone();
  let bytes = req.into_body().collect().await?.to_bytes();
  handle_consume(&method, &headers, bytes).await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  run(service_fn(handler)).await
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn returns_unauthorized_when_missing_internal_token() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");

    let headers = HeaderMap::new();
    let response = handle_consume(&Method::POST, &headers, Bytes::new())
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  }

  #[tokio::test]
  async fn returns_not_configured_when_tidb_env_missing() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
    std::env::remove_var("TIDB_DATABASE_URL");
    std::env::remove_var("DATABASE_URL");

    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer secret".parse().unwrap());
    headers.insert("content-type", "application/json".parse().unwrap());

    let body = Bytes::from(r#"{"tenant_id":"t1","idempotency_key":"k1","limit":3}"#);
    let response = handle_consume(&Method::POST, &headers, body).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
  }

  #[tokio::test]
  async fn returns_429_when_limit_is_zero() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");

    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer secret".parse().unwrap());
    headers.insert("content-type", "application/json".parse().unwrap());

    let body = Bytes::from(r#"{"tenant_id":"t1","idempotency_key":"k1","limit":0}"#);
    let response = handle_consume(&Method::POST, &headers, body).await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
  }
}

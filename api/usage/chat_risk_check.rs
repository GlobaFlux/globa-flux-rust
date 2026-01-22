use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode, Uri};
use serde::Deserialize;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::db::{consume_daily_usage_event, fetch_daily_usage_used, get_pool};

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

fn require_internal_token(headers: &HeaderMap) -> Result<(), Response<ResponseBody>> {
  let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
  let provided = bearer_token(
    headers
      .get("authorization")
      .and_then(|v| v.to_str().ok()),
  )
  .unwrap_or("");

  if expected.is_empty() || provided != expected {
    return Err(
      Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("content-type", "application/json; charset=utf-8")
        .body(ResponseBody::from(serde_json::json!({"ok": false, "error": "unauthorized"})))
        .unwrap(),
    );
  }

  Ok(())
}

fn require_tidb_configured() -> Result<(), Response<ResponseBody>> {
  let has_tidb_url = std::env::var("TIDB_DATABASE_URL")
    .or_else(|_| std::env::var("DATABASE_URL"))
    .map(|v| !v.is_empty())
    .unwrap_or(false);
  if !has_tidb_url {
    return Err(
      Response::builder()
        .status(StatusCode::NOT_IMPLEMENTED)
        .header("content-type", "application/json; charset=utf-8")
        .body(ResponseBody::from(
          serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
        ))
        .unwrap(),
    );
  }
  Ok(())
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

  if let Err(resp) = require_internal_token(headers) {
    return Ok(resp);
  }
  if let Err(resp) = require_tidb_configured() {
    return Ok(resp);
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

async fn handle_today(method: &Method, headers: &HeaderMap, uri: &Uri) -> Result<Response<ResponseBody>, Error> {
  if method != Method::GET {
    return json_response(
      StatusCode::METHOD_NOT_ALLOWED,
      serde_json::json!({"ok": false, "error": "method_not_allowed"}),
    );
  }

  if let Err(resp) = require_internal_token(headers) {
    return Ok(resp);
  }
  if let Err(resp) = require_tidb_configured() {
    return Ok(resp);
  }

  let tenant_id = get_query_param(uri, "tenant_id").unwrap_or_default();
  if tenant_id.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
    );
  }

  const EVENT_TYPE: &str = "chat_risk_check_count";
  let now = chrono::Utc::now();
  let used = fetch_daily_usage_used(get_pool().await?, &tenant_id, EVENT_TYPE, now.date_naive()).await?;

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "day_key": now.format("%Y-%m-%d").to_string(), "used": used}),
  )
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  match *req.method() {
    Method::GET => handle_today(req.method(), req.headers(), req.uri()).await,
    Method::POST => {
      let method = req.method().clone();
      let headers = req.headers().clone();
      let bytes = req.into_body().collect().await?.to_bytes();
      handle_consume(&method, &headers, bytes).await
    }
    _ => json_response(
      StatusCode::METHOD_NOT_ALLOWED,
      serde_json::json!({"ok": false, "error": "method_not_allowed"}),
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
  async fn consume_returns_unauthorized_when_missing_internal_token() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");

    let headers = HeaderMap::new();
    let response = handle_consume(&Method::POST, &headers, Bytes::new())
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  }

  #[tokio::test]
  async fn consume_returns_not_configured_when_tidb_env_missing() {
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
  async fn today_returns_unauthorized_when_missing_internal_token() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
    let headers = HeaderMap::new();
    let uri: Uri = "/api/usage/chat_risk_check?tenant_id=t1".parse().unwrap();
    let response = handle_today(&Method::GET, &headers, &uri).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  }
}

use hyper::{HeaderMap, Method, StatusCode, Uri};
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::db::{fetch_daily_usage_used, get_pool};

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

async fn handle_today(method: &Method, headers: &HeaderMap, uri: &Uri) -> Result<Response<ResponseBody>, Error> {
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

  const EVENT_TYPE: &str = "chat_risk_check_count";
  let now = chrono::Utc::now();
  let used = fetch_daily_usage_used(get_pool().await?, &tenant_id, EVENT_TYPE, now.date_naive()).await?;

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "day_key": now.format("%Y-%m-%d").to_string(), "used": used}),
  )
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  handle_today(req.method(), req.headers(), req.uri()).await
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
    let uri: Uri = "/api/usage/chat_risk_check/today?tenant_id=t1".parse().unwrap();
    let response = handle_today(&Method::GET, &headers, &uri).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  }
}

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode};
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::db::{fetch_subscription, get_pool};

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

async fn handle_status(method: &Method, headers: &HeaderMap, uri: &hyper::Uri, _body: Bytes) -> Result<Response<ResponseBody>, Error> {
  if method != Method::GET {
    return json_response(
      StatusCode::METHOD_NOT_ALLOWED,
      serde_json::json!({"ok": false, "error": "method_not_allowed"}),
    );
  }

  let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
  let provided = bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

  if expected.is_empty() || provided != expected {
    return json_response(
      StatusCode::UNAUTHORIZED,
      serde_json::json!({"ok": false, "error": "unauthorized"}),
    );
  }

  if !has_tidb_url() {
    return json_response(
      StatusCode::NOT_IMPLEMENTED,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
    );
  }

  let tenant_id = query_param(uri.query(), "tenant_id").unwrap_or_default();
  if tenant_id.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
    );
  }

  let pool = get_pool().await?;
  let sub = fetch_subscription(pool, &tenant_id).await?;

  let payload = sub.map(|s| {
    serde_json::json!({
      "status": s.status,
      "current_period_end_ms": s.current_period_end.map(|t| t.timestamp_millis()),
    })
  });

  json_response(StatusCode::OK, serde_json::json!({"ok": true, "subscription": payload}))
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let method = req.method().clone();
  let headers = req.headers().clone();
  let uri = req.uri().clone();
  let bytes = req.into_body().collect().await?.to_bytes();
  handle_status(&method, &headers, &uri, bytes).await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  run(service_fn(handler)).await
}


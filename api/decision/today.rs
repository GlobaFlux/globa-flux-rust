use chrono::{NaiveDate, TimeZone, Utc};
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode};
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::db::get_pool;

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

fn default_decision(as_of_dt: NaiveDate) -> serde_json::Value {
  serde_json::json!({
    "asOfDate": as_of_dt.to_string(),
    "direction": "PROTECT",
    "confidence": 0.6,
    "evidence": ["MVP stub: no channel data synced yet"],
    "forbidden": ["High-risk strategy changes without evidence"],
    "reevaluate": ["After OAuth connect + first metrics sync"],
  })
}

async fn handle_today(method: &Method, headers: &HeaderMap, uri: &hyper::Uri) -> Result<Response<ResponseBody>, Error> {
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
  let channel_id = query_param(uri.query(), "channel_id").unwrap_or_default();
  if tenant_id.is_empty() || channel_id.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id and channel_id are required"}),
    );
  }

  let now_ms = Utc::now().timestamp_millis();
  let today = Utc
    .timestamp_millis_opt(now_ms)
    .single()
    .unwrap_or_else(Utc::now)
    .date_naive();
  let as_of_dt = query_param(uri.query(), "as_of_dt")
    .and_then(|v| NaiveDate::parse_from_str(&v, "%Y-%m-%d").ok())
    .unwrap_or(today);

  let pool = get_pool().await?;

  let row = sqlx::query_as::<_, (String, f64, String, String, String)>(
    r#"
      SELECT direction,
             CAST(confidence AS DOUBLE) AS confidence,
             evidence_json,
             forbidden_json,
             reevaluate_json
      FROM decision_daily
      WHERE tenant_id = ? AND channel_id = ? AND as_of_dt = ?
      LIMIT 1;
    "#,
  )
  .bind(&tenant_id)
  .bind(&channel_id)
  .bind(as_of_dt)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let decision = if let Some((direction, confidence, evidence_json, forbidden_json, reevaluate_json)) = row {
    let evidence = serde_json::from_str::<Vec<String>>(&evidence_json).unwrap_or_default();
    let forbidden = serde_json::from_str::<Vec<String>>(&forbidden_json).unwrap_or_default();
    let reevaluate = serde_json::from_str::<Vec<String>>(&reevaluate_json).unwrap_or_default();

    serde_json::json!({
      "asOfDate": as_of_dt.to_string(),
      "direction": direction,
      "confidence": confidence,
      "evidence": evidence,
      "forbidden": forbidden,
      "reevaluate": reevaluate,
    })
  } else {
    default_decision(as_of_dt)
  };

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "decision": decision}),
  )
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let method = req.method().clone();
  let headers = req.headers().clone();
  let uri = req.uri().clone();
  let _bytes = req.into_body().collect().await?.to_bytes();
  handle_today(&method, &headers, &uri).await
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
    let uri: hyper::Uri = "/api/decision/today?tenant_id=t1&channel_id=c1"
      .parse()
      .unwrap();
    let response = handle_today(&Method::GET, &headers, &uri).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  }
}

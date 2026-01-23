use bytes::Bytes;
use chrono::{TimeZone, Utc};
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode};
use serde::Deserialize;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DispatchSchedule {
  Daily,
  Weekly,
}

impl DispatchSchedule {
  fn from_query(query: Option<&str>) -> Self {
    let value = query_value(query, "schedule").unwrap_or("");
    match value {
      "weekly" | "Weekly" | "WEEKLY" => DispatchSchedule::Weekly,
      _ => DispatchSchedule::Daily,
    }
  }

  fn job_type(&self) -> &'static str {
    match self {
      DispatchSchedule::Daily => "daily_channel",
      DispatchSchedule::Weekly => "weekly_channel",
    }
  }
}

fn query_value<'a>(query: Option<&'a str>, key: &str) -> Option<&'a str> {
  let query = query?;
  for part in query.split('&') {
    let (k, v) = part.split_once('=')?;
    if k == key {
      return Some(v);
    }
  }
  None
}

#[derive(Deserialize)]
struct DispatchRequest {
  now_ms: i64,
}

async fn handle_dispatch(
  schedule: DispatchSchedule,
  method: &Method,
  headers: &HeaderMap,
  body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
  if method != Method::POST {
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

  let parsed: DispatchRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

  if parsed.now_ms <= 0 {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "now_ms is required"}),
    );
  }

  let now = Utc
    .timestamp_millis_opt(parsed.now_ms)
    .single()
    .unwrap_or_else(Utc::now);
  let run_for_dt = now.date_naive();

  let pool = get_pool().await?;

  let channels: Vec<(String, String)> = sqlx::query_as(
    r#"
      SELECT tenant_id, channel_id
      FROM channel_connections
      WHERE oauth_provider = 'youtube'
        AND channel_id IS NOT NULL
        AND channel_id <> '';
    "#,
  )
  .fetch_all(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let job_type = schedule.job_type();

  for (tenant_id, channel_id) in channels.iter() {
    let dedupe_key = format!("{tenant_id}:{job_type}:{channel_id}:{run_for_dt}");
    sqlx::query(
      r#"
        INSERT INTO job_tasks (tenant_id, job_type, channel_id, run_for_dt, dedupe_key, status)
        VALUES (?, ?, ?, ?, ?, 'pending')
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
      "#,
    )
    .bind(tenant_id)
    .bind(job_type)
    .bind(channel_id)
    .bind(run_for_dt)
    .bind(dedupe_key)
    .execute(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;
  }

  json_response(
    StatusCode::OK,
    serde_json::json!({
      "ok": true,
      "job_type": job_type,
      "run_for_dt": run_for_dt.to_string(),
      "candidates": channels.len()
    }),
  )
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let schedule = DispatchSchedule::from_query(req.uri().query());
  let method = req.method().clone();
  let headers = req.headers().clone();
  let bytes = req.into_body().collect().await?.to_bytes();
  handle_dispatch(schedule, &method, &headers, bytes).await
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
    let response = handle_dispatch(DispatchSchedule::Daily, &Method::POST, &headers, Bytes::new())
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

    let body = Bytes::from(r#"{"now_ms":1700000000000}"#);
    let response = handle_dispatch(DispatchSchedule::Daily, &Method::POST, &headers, body)
      .await
      .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
  }

  #[test]
  fn schedule_from_query_defaults_daily() {
    assert_eq!(DispatchSchedule::from_query(None), DispatchSchedule::Daily);
    assert_eq!(
      DispatchSchedule::from_query(Some("foo=bar")),
      DispatchSchedule::Daily
    );
  }

  #[test]
  fn schedule_from_query_weekly() {
    assert_eq!(
      DispatchSchedule::from_query(Some("schedule=weekly")),
      DispatchSchedule::Weekly
    );
    assert_eq!(
      DispatchSchedule::from_query(Some("a=b&schedule=weekly&c=d")),
      DispatchSchedule::Weekly
    );
  }

  #[tokio::test]
  async fn returns_not_configured_when_tidb_env_missing_for_weekly() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
    std::env::remove_var("TIDB_DATABASE_URL");
    std::env::remove_var("DATABASE_URL");

    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer secret".parse().unwrap());
    headers.insert("content-type", "application/json".parse().unwrap());

    let body = Bytes::from(r#"{"now_ms":1700000000000}"#);
    let response = handle_dispatch(DispatchSchedule::Weekly, &Method::POST, &headers, body)
      .await
      .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
  }
}

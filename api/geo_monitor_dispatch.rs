use bytes::Bytes;
use chrono::{TimeZone, Utc};
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode};
use serde::Deserialize;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::db::{
  enqueue_geo_monitor_prompt_tasks, ensure_geo_monitor_run, fetch_tenant_gemini_model, get_pool,
  list_geo_monitor_prompts,
};

const GLOBAL_TENANT_ID: &str = "global";

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

fn schedule_from_request(uri: &hyper::Uri) -> &'static str {
  let value = query_value(uri.query(), "schedule").unwrap_or("");
  match value {
    "daily" | "Daily" | "DAILY" => "daily",
    _ => "weekly",
  }
}

#[derive(Deserialize)]
struct DispatchRequest {
  now_ms: i64,
  #[serde(default)]
  tenant_id: Option<String>,
}

async fn handle_dispatch(
  schedule: &str,
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

  let tenant_filter = parsed
    .tenant_id
    .as_deref()
    .map(str::trim)
    .filter(|v| !v.is_empty())
    .map(str::to_string);

  let pool = get_pool().await?;

  let db_model = fetch_tenant_gemini_model(pool, GLOBAL_TENANT_ID).await?;
  let model = match db_model {
    Some(v) if !v.trim().is_empty() => v,
    _ => {
      return json_response(
        StatusCode::NOT_IMPLEMENTED,
        serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing gemini_model for tenant_id=global"}),
      )
    }
  };

  let projects: Vec<(String, i64)> = if let Some(tid) = tenant_filter.as_deref() {
    sqlx::query_as(
      r#"
        SELECT tenant_id, id
        FROM geo_monitor_projects
        WHERE tenant_id = ? AND enabled = 1 AND schedule = ?;
      "#,
    )
    .bind(tid)
    .bind(schedule)
    .fetch_all(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?
  } else {
    sqlx::query_as(
      r#"
        SELECT tenant_id, id
        FROM geo_monitor_projects
        WHERE enabled = 1 AND schedule = ?;
      "#,
    )
    .bind(schedule)
    .fetch_all(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?
  };

  let mut runs_ensured: i64 = 0;
  let mut tasks_enqueued: u64 = 0;

  for (tenant_id, project_id) in projects.iter() {
    let prompts = list_geo_monitor_prompts(pool, tenant_id, *project_id).await?;
    let prompt_ids: Vec<i64> = prompts.iter().filter(|p| p.enabled).map(|p| p.id).collect();
    let prompt_total = prompt_ids.len() as i32;
    if prompt_total <= 0 {
      continue;
    }

    let _run = ensure_geo_monitor_run(
      pool,
      tenant_id,
      *project_id,
      run_for_dt,
      "gemini",
      model.trim(),
      prompt_total,
    )
    .await?;
    runs_ensured += 1;

    let enqueued = enqueue_geo_monitor_prompt_tasks(pool, tenant_id, *project_id, run_for_dt, &prompt_ids).await?;
    tasks_enqueued = tasks_enqueued.saturating_add(enqueued);
  }

  json_response(
    StatusCode::OK,
    serde_json::json!({
      "ok": true,
      "tenant_id": tenant_filter,
      "schedule": schedule,
      "run_for_dt": run_for_dt.to_string(),
      "projects": projects.len(),
      "runs_ensured": runs_ensured,
      "tasks_enqueued_rows": tasks_enqueued
    }),
  )
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let schedule = schedule_from_request(req.uri());
  let method = req.method().clone();
  let headers = req.headers().clone();
  let bytes = req.into_body().collect().await?.to_bytes();
  handle_dispatch(schedule, &method, &headers, bytes).await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  run(service_fn(handler)).await
}


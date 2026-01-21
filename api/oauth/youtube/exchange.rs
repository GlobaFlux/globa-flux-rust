use http_body_util::BodyExt;
use hyper::StatusCode;
use serde::Deserialize;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use chrono::{Duration, Utc};

use globa_flux_rust::db::{get_pool, upsert_video_daily_metric, upsert_youtube_connection};
use globa_flux_rust::decision_engine::{compute_decision, DecisionEngineConfig};
use globa_flux_rust::providers::youtube::{exchange_code_for_tokens, youtube_oauth_client_from_env};
use globa_flux_rust::providers::youtube_api::fetch_my_channel_id;
use globa_flux_rust::providers::youtube_analytics::{fetch_video_daily_metrics, youtube_analytics_error_to_vercel_error};

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
struct ExchangeRequest {
  tenant_id: String,
  code: String,
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
    req
      .headers()
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

  let bytes = req.into_body().collect().await?.to_bytes();
  let parsed: ExchangeRequest = serde_json::from_slice(&bytes).map_err(|e| -> Error {
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

#[tokio::main]
async fn main() -> Result<(), Error> {
  run(service_fn(handler)).await
}

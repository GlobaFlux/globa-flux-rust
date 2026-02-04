use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode, Uri};
use serde::Deserialize;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use chrono::{DateTime, Duration, NaiveDate, Utc};

use globa_flux_rust::db::{
  fetch_youtube_channel_id,
  fetch_youtube_connection_tokens,
  fetch_youtube_content_owner_id,
  fetch_or_seed_youtube_oauth_app_config,
  fetch_youtube_oauth_app_config,
  get_pool,
  set_youtube_channel_id,
  set_youtube_content_owner_id,
  update_youtube_connection_tokens,
  upsert_video_daily_metric,
  upsert_youtube_connection,
  upsert_youtube_oauth_app_config,
};
use globa_flux_rust::decision_engine::{compute_decision, DecisionEngineConfig};
use globa_flux_rust::providers::youtube::{
  build_authorize_url, exchange_code_for_tokens, refresh_tokens, youtube_oauth_client_from_config,
};
use globa_flux_rust::providers::youtube_analytics::{
  fetch_video_daily_metrics_for_channel, youtube_analytics_error_to_vercel_error,
};
use globa_flux_rust::providers::youtube_api::fetch_my_channel_id;
use globa_flux_rust::providers::youtube_videos::{
  fetch_video_snapshot, set_video_thumbnail_from_url, update_video_publish_at, update_video_title,
};
use globa_flux_rust::providers::youtube_partner::fetch_my_content_owner_id;

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

fn truncate_string(value: &str, max_chars: usize) -> String {
  if max_chars == 0 {
    return String::new();
  }
  let mut out = String::new();
  for (idx, ch) in value.chars().enumerate() {
    if idx >= max_chars {
      break;
    }
    out.push(ch);
  }
  out
}

fn now_ms() -> i64 {
  use std::time::{SystemTime, UNIX_EPOCH};
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|d| d.as_millis() as i64)
    .unwrap_or(0)
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

fn parse_dt(v: &str) -> Option<NaiveDate> {
  let s = v.trim();
  NaiveDate::parse_from_str(s, "%Y-%m-%d")
    .ok()
    .or_else(|| NaiveDate::parse_from_str(s, "%Y/%m/%d").ok())
    .or_else(|| NaiveDate::parse_from_str(s, "%m/%d/%Y").ok())
}

fn round2(v: f64) -> f64 {
  (v * 100.0).round() / 100.0
}

fn median_i64(values: &mut [i64]) -> Option<i64> {
  if values.is_empty() {
    return None;
  }
  values.sort_unstable();
  let mid = values.len() / 2;
  if values.len() % 2 == 1 {
    Some(values[mid])
  } else {
    Some((values[mid - 1] + values[mid]) / 2)
  }
}

#[derive(Deserialize)]
struct StartRequest {
  tenant_id: String,
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

  if !has_tidb_url() {
    return json_response(
      StatusCode::NOT_IMPLEMENTED,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
    );
  }

  let parsed: StartRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

  if parsed.tenant_id.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
    );
  }

  if parsed.state.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "state is required"}),
    );
  }

  let pool = get_pool().await?;
  let app = fetch_or_seed_youtube_oauth_app_config(pool, &parsed.tenant_id).await?;
  let Some(app) = app else {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({
        "ok": false,
        "error": "not_configured",
        "message": "Missing YouTube OAuth app config for tenant. Configure via /api/oauth/youtube/app_config or set YOUTUBE_CLIENT_ID/YOUTUBE_CLIENT_SECRET/YOUTUBE_REDIRECT_URI on the Rust backend."
      }),
    );
  };

  let Some(client_secret) = app.client_secret.as_deref().map(str::trim).filter(|v| !v.is_empty()) else {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing YouTube OAuth client_secret for tenant"}),
    );
  };

  let (client, _redirect) =
    youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
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

  let pool = get_pool().await?;
  let app = fetch_or_seed_youtube_oauth_app_config(pool, &parsed.tenant_id).await?;
  let Some(app) = app else {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({
        "ok": false,
        "error": "not_configured",
        "message": "Missing YouTube OAuth app config for tenant. Configure via /api/oauth/youtube/app_config or set YOUTUBE_CLIENT_ID/YOUTUBE_CLIENT_SECRET/YOUTUBE_REDIRECT_URI on the Rust backend."
      }),
    );
  };
  let Some(client_secret) = app.client_secret.as_deref().map(str::trim).filter(|v| !v.is_empty()) else {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing YouTube OAuth client_secret for tenant"}),
    );
  };
  let (client, _redirect) =
    youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
  let tokens = exchange_code_for_tokens(&client, &parsed.code).await?;
  let channel_id = fetch_my_channel_id(&tokens.access_token).await?;

  upsert_youtube_connection(pool, &parsed.tenant_id, &channel_id, &tokens)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

  // Hybrid onboarding: generate the first decision quickly after OAuth connect.
  // Uses the last 7 completed days (ending yesterday) as the decision window.
  let as_of_dt = Utc::now().date_naive();
  let start_dt = as_of_dt - Duration::days(7);
  let end_dt = as_of_dt - Duration::days(1);

  let metrics =
    fetch_video_daily_metrics_for_channel(&tokens.access_token, &channel_id, start_dt, end_dt)
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

#[derive(Deserialize)]
struct SetActiveChannelRequest {
  tenant_id: String,
  channel_id: String,
}

async fn handle_set_active_channel(method: &Method, headers: &HeaderMap, body: Bytes) -> Result<Response<ResponseBody>, Error> {
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

  if !has_tidb_url() {
    return json_response(
      StatusCode::NOT_IMPLEMENTED,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
    );
  }

  let parsed: SetActiveChannelRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

  let tenant_id = parsed.tenant_id.trim();
  let channel_id = parsed.channel_id.trim();
  if tenant_id.is_empty() || channel_id.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id and channel_id are required"}),
    );
  }

  let pool = get_pool().await?;

  let existing_channel_id = fetch_youtube_channel_id(pool, tenant_id).await?;
  let Some(existing_channel_id) = existing_channel_id else {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_connected", "message": "No YouTube channel connection found for this tenant"}),
    );
  };

  let mut tokens = fetch_youtube_connection_tokens(pool, tenant_id, &existing_channel_id)
    .await?
    .ok_or_else(|| Box::new(std::io::Error::other("missing youtube channel connection")) as Error)?;

  // Proactive refresh if expired (best-effort).
  let needs_refresh = tokens
    .expires_at
    .map(|dt| dt <= chrono::Utc::now())
    .unwrap_or(false);
  if needs_refresh {
      if let Some(refresh) = tokens.refresh_token.clone() {
      let app = fetch_or_seed_youtube_oauth_app_config(pool, tenant_id).await?;
      let Some(app) = app else {
        return json_response(
          StatusCode::NOT_FOUND,
          serde_json::json!({
            "ok": false,
            "error": "not_configured",
            "message": "Missing YouTube OAuth app config for tenant. Configure via /api/oauth/youtube/app_config or set YOUTUBE_CLIENT_ID/YOUTUBE_CLIENT_SECRET/YOUTUBE_REDIRECT_URI on the Rust backend."
          }),
        );
      };
      let Some(client_secret) = app.client_secret.as_deref().map(str::trim).filter(|v| !v.is_empty()) else {
        return json_response(
          StatusCode::NOT_FOUND,
          serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing YouTube OAuth client_secret for tenant"}),
        );
      };

      let (client, _redirect) =
        youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
      let refreshed = refresh_tokens(&client, &refresh).await?;
      update_youtube_connection_tokens(pool, tenant_id, &existing_channel_id, &refreshed).await?;
      tokens.access_token = refreshed.access_token;
      tokens.refresh_token = refreshed.refresh_token.or(Some(refresh));
    }
  }

  let as_of_dt = Utc::now().date_naive();
  let start_dt = as_of_dt - Duration::days(7);
  let end_dt = as_of_dt - Duration::days(1);

  let metrics = match fetch_video_daily_metrics_for_channel(&tokens.access_token, channel_id, start_dt, end_dt).await {
    Ok(rows) => rows,
    Err(err) if err.status == Some(401) => {
      if let Some(refresh) = tokens.refresh_token.clone() {
        let app = fetch_or_seed_youtube_oauth_app_config(pool, tenant_id).await?;
        let Some(app) = app else {
          return json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({
              "ok": false,
              "error": "not_configured",
              "message": "Missing YouTube OAuth app config for tenant. Configure via /api/oauth/youtube/app_config or set YOUTUBE_CLIENT_ID/YOUTUBE_CLIENT_SECRET/YOUTUBE_REDIRECT_URI on the Rust backend."
            }),
          );
        };
        let Some(client_secret) = app.client_secret.as_deref().map(str::trim).filter(|v| !v.is_empty()) else {
          return json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing YouTube OAuth client_secret for tenant"}),
          );
        };
        let (client, _redirect) =
          youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
        let refreshed = refresh_tokens(&client, &refresh).await?;
        update_youtube_connection_tokens(pool, tenant_id, &existing_channel_id, &refreshed).await?;
        tokens.access_token = refreshed.access_token;

        fetch_video_daily_metrics_for_channel(&tokens.access_token, channel_id, start_dt, end_dt)
          .await
          .map_err(youtube_analytics_error_to_vercel_error)?
      } else {
        return json_response(
          StatusCode::UNAUTHORIZED,
          serde_json::json!({"ok": false, "error": "unauthorized", "message": "YouTube access token expired and no refresh token available"}),
        );
      }
    }
    Err(err) if err.status == Some(403) => {
      return json_response(
        StatusCode::FORBIDDEN,
        serde_json::json!({"ok": false, "error": "forbidden", "message": "No permission to access this channel's analytics", "details": err.to_string()}),
      );
    }
    Err(err) => {
      return json_response(
        StatusCode::BAD_GATEWAY,
        serde_json::json!({"ok": false, "error": "youtube_analytics_error", "message": err.to_string(), "status": err.status}),
      );
    }
  };

  for row in metrics.iter() {
    upsert_video_daily_metric(
      pool,
      tenant_id,
      channel_id,
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
  .bind(tenant_id)
  .bind(channel_id)
  .bind(as_of_dt)
  .bind(&decision.direction)
  .bind(decision.confidence)
  .bind(evidence_json)
  .bind(forbidden_json)
  .bind(reevaluate_json)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  set_youtube_channel_id(pool, tenant_id, channel_id).await?;

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

  if !has_tidb_url() {
    return json_response(
      StatusCode::NOT_IMPLEMENTED,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
    );
  }

  let pool = get_pool().await?;
  let channel_id = fetch_youtube_channel_id(pool, &tenant_id).await?;
  let content_owner_id = fetch_youtube_content_owner_id(pool, &tenant_id).await?;
  let connected = channel_id.is_some();

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "connected": connected, "channel_id": channel_id, "content_owner_id": content_owner_id}),
  )
}

#[derive(Deserialize)]
struct AppConfigUpsertRequest {
  tenant_id: String,
  client_id: String,
  #[serde(default)]
  client_secret: Option<String>,
  redirect_uri: String,
}

async fn handle_app_config(method: &Method, headers: &HeaderMap, uri: &Uri, body: Option<Bytes>) -> Result<Response<ResponseBody>, Error> {
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

  if !has_tidb_url() {
    return json_response(
      StatusCode::NOT_IMPLEMENTED,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
    );
  }

  match *method {
    Method::GET => {
      let tenant_id = get_query_param(uri, "tenant_id").unwrap_or_default();
      if tenant_id.is_empty() {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
        );
      }

      let pool = get_pool().await?;
      let cfg = fetch_youtube_oauth_app_config(pool, &tenant_id).await?;

      let (client_id, redirect_uri, has_client_secret) = match cfg {
        Some(cfg) => (
          Some(cfg.client_id),
          Some(cfg.redirect_uri),
          cfg.client_secret
            .as_deref()
            .map(str::trim)
            .is_some_and(|v| !v.is_empty()),
        ),
        None => (None, None, false),
      };

      json_response(
        StatusCode::OK,
        serde_json::json!({
          "ok": true,
          "tenant_id": tenant_id,
          "provider": "youtube",
          "configured": has_client_secret
            && client_id.as_deref().is_some_and(|v| !v.is_empty())
            && redirect_uri.as_deref().is_some_and(|v| !v.is_empty()),
          "client_id": client_id,
          "redirect_uri": redirect_uri,
          "has_client_secret": has_client_secret
        }),
      )
    }
    Method::POST => {
      let body = body.ok_or_else(|| Box::new(std::io::Error::other("missing body")) as Error)?;
      let parsed: AppConfigUpsertRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid json body: {e}")))
      })?;

      if parsed.tenant_id.trim().is_empty() {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
        );
      }
      if parsed.client_id.trim().is_empty() {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "client_id is required"}),
        );
      }
      if parsed.redirect_uri.trim().is_empty() {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "redirect_uri is required"}),
        );
      }

      let secret = parsed
        .client_secret
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());

      let pool = get_pool().await?;
      let existing = fetch_youtube_oauth_app_config(pool, &parsed.tenant_id).await?;
      let has_existing_secret = existing
        .as_ref()
        .and_then(|cfg| cfg.client_secret.as_deref())
        .map(str::trim)
        .is_some_and(|v| !v.is_empty());

      if secret.is_none() && !has_existing_secret {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "client_secret is required for initial setup"}),
        );
      }

      upsert_youtube_oauth_app_config(
        pool,
        &parsed.tenant_id,
        parsed.client_id.trim(),
        secret,
        parsed.redirect_uri.trim(),
      )
      .await?;

      json_response(StatusCode::OK, serde_json::json!({"ok": true}))
    }
    _ => json_response(
      StatusCode::METHOD_NOT_ALLOWED,
      serde_json::json!({"ok": false, "error": "method_not_allowed"}),
    ),
  }
}

#[derive(Deserialize)]
struct ContentOwnerDiscoverRequest {
  tenant_id: String,
}

async fn handle_content_owner_discover(method: &Method, headers: &HeaderMap, body: Bytes) -> Result<Response<ResponseBody>, Error> {
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

  if !has_tidb_url() {
    return json_response(
      StatusCode::NOT_IMPLEMENTED,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
    );
  }

  let parsed: ContentOwnerDiscoverRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

  if parsed.tenant_id.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
    );
  }

  let pool = get_pool().await?;
  let channel_id = fetch_youtube_channel_id(pool, &parsed.tenant_id).await?;
  let Some(channel_id) = channel_id else {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_connected", "message": "No YouTube channel connection found for this tenant"}),
    );
  };

  let tokens = fetch_youtube_connection_tokens(pool, &parsed.tenant_id, &channel_id).await?;
  let Some(mut tokens) = tokens else {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_connected", "message": "No YouTube tokens found for this tenant"}),
    );
  };

  // Best-effort proactive refresh if expired.
  let needs_refresh = tokens
    .expires_at
    .map(|dt| dt <= chrono::Utc::now())
    .unwrap_or(false);
  if needs_refresh {
    if let Some(refresh) = tokens.refresh_token.clone() {
      let app = fetch_or_seed_youtube_oauth_app_config(pool, &parsed.tenant_id).await?;
      let Some(app) = app else {
        return json_response(
          StatusCode::NOT_FOUND,
          serde_json::json!({
            "ok": false,
            "error": "not_configured",
            "message": "Missing YouTube OAuth app config for tenant. Configure via /api/oauth/youtube/app_config or set YOUTUBE_CLIENT_ID/YOUTUBE_CLIENT_SECRET/YOUTUBE_REDIRECT_URI on the Rust backend."
          }),
        );
      };
      let Some(client_secret) = app.client_secret.as_deref().map(str::trim).filter(|v| !v.is_empty()) else {
        return json_response(
          StatusCode::NOT_FOUND,
          serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing YouTube OAuth client_secret for tenant"}),
        );
      };

      let (client, _redirect) =
        youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
      let refreshed = refresh_tokens(&client, &refresh).await?;
      update_youtube_connection_tokens(pool, &parsed.tenant_id, &channel_id, &refreshed).await?;
      tokens.access_token = refreshed.access_token;
      tokens.refresh_token = refreshed.refresh_token.or(Some(refresh));
      tokens.expires_at = refreshed
        .expires_in_seconds
        .map(|secs| chrono::Utc::now() + chrono::Duration::seconds(secs as i64));
    }
  }

  let content_owner_id = fetch_my_content_owner_id(&tokens.access_token).await?;
  set_youtube_content_owner_id(pool, &parsed.tenant_id, content_owner_id.as_deref()).await?;

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "content_owner_id": content_owner_id, "discovered": content_owner_id.is_some()}),
  )
}

#[derive(serde::Serialize)]
struct MetricDailyItem {
  date: String,
  video_id: String,
  impressions: i64,
  views: i64,
  revenue_usd: f64,
  ctr: f64,
  rpm: f64,
  source: String,
}

async fn handle_youtube_metrics_daily(
  method: &Method,
  headers: &HeaderMap,
  uri: &Uri,
) -> Result<Response<ResponseBody>, Error> {
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

  let tenant_id = get_query_param(uri, "tenant_id").unwrap_or_default();
  if tenant_id.trim().is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
    );
  }

  let pool = get_pool().await?;
  let channel_id = match get_query_param(uri, "channel_id")
    .map(|v| v.trim().to_string())
    .filter(|v| !v.is_empty())
  {
    Some(v) => v,
    None => fetch_youtube_channel_id(pool, tenant_id.trim())
      .await?
      .unwrap_or_default(),
  };

  if channel_id.trim().is_empty() {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_connected", "message": "No active YouTube channel for this tenant"}),
    );
  }

  let today = Utc::now().date_naive();
  let start_dt = get_query_param(uri, "start_dt")
    .and_then(|v| parse_dt(&v))
    .unwrap_or(today - Duration::days(14));
  let end_dt = get_query_param(uri, "end_dt")
    .and_then(|v| parse_dt(&v))
    .unwrap_or(today);

  if start_dt > end_dt {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "start_dt must be <= end_dt"}),
    );
  }

  let video_id_filter = get_query_param(uri, "video_id")
    .map(|v| v.trim().to_string())
    .filter(|v| !v.is_empty());

  let rows: Vec<(NaiveDate, f64, i64, i64)> = if let Some(video_id) = video_id_filter.as_deref() {
    sqlx::query_as::<_, (NaiveDate, f64, i64, i64)>(
      r#"
        SELECT dt,
               CAST(SUM(estimated_revenue_usd) AS DOUBLE) AS revenue_usd,
               CAST(SUM(impressions) AS SIGNED) AS impressions,
               CAST(SUM(views) AS SIGNED) AS views
        FROM video_daily_metrics
        WHERE tenant_id = ?
          AND channel_id = ?
          AND dt BETWEEN ? AND ?
          AND video_id = ?
        GROUP BY dt
        ORDER BY dt ASC;
      "#,
    )
    .bind(tenant_id.trim())
    .bind(channel_id.trim())
    .bind(start_dt)
    .bind(end_dt)
    .bind(video_id)
    .fetch_all(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?
  } else {
    sqlx::query_as::<_, (NaiveDate, f64, i64, i64)>(
      r#"
        SELECT dt,
               CAST(SUM(estimated_revenue_usd) AS DOUBLE) AS revenue_usd,
               CAST(SUM(impressions) AS SIGNED) AS impressions,
               CAST(SUM(views) AS SIGNED) AS views
        FROM video_daily_metrics
        WHERE tenant_id = ?
          AND channel_id = ?
          AND dt BETWEEN ? AND ?
        GROUP BY dt
        ORDER BY dt ASC;
      "#,
    )
    .bind(tenant_id.trim())
    .bind(channel_id.trim())
    .bind(start_dt)
    .bind(end_dt)
    .fetch_all(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?
  };

  let video_id_out = video_id_filter.unwrap_or_else(|| "channel_total".to_string());
  let items: Vec<MetricDailyItem> = rows
    .into_iter()
    .map(|(dt, revenue_usd, impressions, views)| {
      let ctr = if impressions > 0 {
        (views as f64) / (impressions as f64)
      } else {
        0.0
      };
      let rpm = if views > 0 {
        (revenue_usd / (views as f64)) * 1000.0
      } else {
        0.0
      };
      MetricDailyItem {
        date: dt.to_string(),
        video_id: video_id_out.clone(),
        impressions,
        views,
        revenue_usd: round2(revenue_usd),
        ctr: (ctr * 10000.0).round() / 10000.0,
        rpm: round2(rpm),
        source: "tidb".to_string(),
      }
    })
    .collect();

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "items": items, "channel_id": channel_id, "start_dt": start_dt.to_string(), "end_dt": end_dt.to_string()}),
  )
}

#[derive(serde::Serialize)]
struct SponsorQuoteDefaultsBasis {
  long_source: String,
  long_n: i64,
  shorts_source: String,
  shorts_n: i64,
}

#[derive(serde::Serialize)]
struct SponsorQuoteDefaultsResponse {
  avg_views_long: i64,
  avg_views_shorts: i64,
  basis: SponsorQuoteDefaultsBasis,
}

async fn handle_youtube_sponsor_quote_defaults(
  method: &Method,
  headers: &HeaderMap,
  uri: &Uri,
) -> Result<Response<ResponseBody>, Error> {
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

  let tenant_id = get_query_param(uri, "tenant_id").unwrap_or_default();
  if tenant_id.trim().is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
    );
  }

  let pool = get_pool().await?;
  let channel_id = match get_query_param(uri, "channel_id")
    .map(|v| v.trim().to_string())
    .filter(|v| !v.is_empty())
  {
    Some(v) => v,
    None => fetch_youtube_channel_id(pool, tenant_id.trim())
      .await?
      .unwrap_or_default(),
  };

  if channel_id.trim().is_empty() {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_connected", "message": "No active YouTube channel for this tenant"}),
    );
  }

  let today = Utc::now().date_naive();
  let start_dt = today - Duration::days(28);
  let end_dt = today;

  let rows = sqlx::query_as::<_, (String, i64)>(
    r#"
      SELECT video_id,
             CAST(SUM(views) AS SIGNED) AS views_28d
      FROM video_daily_metrics
      WHERE tenant_id = ?
        AND channel_id = ?
        AND dt BETWEEN ? AND ?
      GROUP BY video_id
      ORDER BY views_28d DESC
      LIMIT 10;
    "#,
  )
  .bind(tenant_id.trim())
  .bind(channel_id.trim())
  .bind(start_dt)
  .bind(end_dt)
  .fetch_all(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let mut views: Vec<i64> = rows.iter().map(|(_, v)| *v).filter(|v| *v > 0).collect();
  let long = median_i64(&mut views).unwrap_or(50_000);
  let shorts = ((long as f64) * 0.6).round() as i64;

  let defaults = SponsorQuoteDefaultsResponse {
    avg_views_long: if long > 0 { long } else { 50_000 },
    avg_views_shorts: if shorts > 0 { shorts } else { 30_000 },
    basis: SponsorQuoteDefaultsBasis {
      long_source: "top_10_video_views_28d_median".to_string(),
      long_n: rows.len() as i64,
      shorts_source: "long_x0.6".to_string(),
      shorts_n: rows.len() as i64,
    },
  };

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "defaults": defaults, "channel_id": channel_id}),
  )
}

#[derive(Deserialize)]
struct SponsorQuoteRequest {
  tenant_id: String,
  channel_id: Option<String>,
  niches: Option<Vec<String>>,
  avg_views_long: Option<i64>,
  avg_views_shorts: Option<i64>,
  rpm_hint: Option<f64>,
}

#[derive(serde::Serialize)]
struct SponsorQuoteLine {
  deliverable: String,
  cpm_range: (f64, f64),
  flat_fee_range: (i64, i64),
  avg_views_used: i64,
}

async fn handle_youtube_sponsor_quote(
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

  let parsed: SponsorQuoteRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

  if parsed.tenant_id.trim().is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
    );
  }

  let pool = get_pool().await?;
  let channel_id = match parsed
    .channel_id
    .as_deref()
    .map(str::trim)
    .filter(|v| !v.is_empty())
  {
    Some(v) => v.to_string(),
    None => fetch_youtube_channel_id(pool, parsed.tenant_id.trim())
      .await?
      .unwrap_or_default(),
  };

  if channel_id.trim().is_empty() {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_connected", "message": "No active YouTube channel for this tenant"}),
    );
  }

  let today = Utc::now().date_naive();
  let start_dt = today - Duration::days(28);
  let end_dt = today;

  let defaults_rows = sqlx::query_as::<_, (String, i64)>(
    r#"
      SELECT video_id,
             CAST(SUM(views) AS SIGNED) AS views_28d
      FROM video_daily_metrics
      WHERE tenant_id = ?
        AND channel_id = ?
        AND dt BETWEEN ? AND ?
      GROUP BY video_id
      ORDER BY views_28d DESC
      LIMIT 10;
    "#,
  )
  .bind(parsed.tenant_id.trim())
  .bind(channel_id.trim())
  .bind(start_dt)
  .bind(end_dt)
  .fetch_all(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let mut default_views: Vec<i64> = defaults_rows
    .iter()
    .map(|(_, v)| *v)
    .filter(|v| *v > 0)
    .collect();
  let default_long = median_i64(&mut default_views).unwrap_or(50_000);
  let default_shorts = ((default_long as f64) * 0.6).round() as i64;

  let avg_views_long = parsed.avg_views_long.unwrap_or(default_long).max(1);
  let avg_views_shorts = parsed.avg_views_shorts.unwrap_or(default_shorts).max(1);

  let rpm_base = if let Some(hint) = parsed.rpm_hint.filter(|v| *v > 0.0) {
    hint
  } else {
    let rpm_row = sqlx::query_as::<_, (f64, i64)>(
      r#"
        SELECT CAST(COALESCE(SUM(estimated_revenue_usd), 0) AS DOUBLE) AS revenue_usd,
               CAST(COALESCE(SUM(views), 0) AS SIGNED) AS views
        FROM video_daily_metrics
        WHERE tenant_id = ?
          AND channel_id = ?
          AND dt BETWEEN ? AND ?;
      "#,
    )
    .bind(parsed.tenant_id.trim())
    .bind(channel_id.trim())
    .bind(start_dt)
    .bind(end_dt)
    .fetch_optional(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

    if let Some((revenue, views)) = rpm_row {
      if views > 0 && revenue > 0.0 {
        (revenue / (views as f64)) * 1000.0
      } else {
        12.0
      }
    } else {
      12.0
    }
  };

  let cpm_low = round2(rpm_base * 0.8);
  let cpm_high = round2(rpm_base * 1.4);

  let deliverables = vec![
    ("integration", avg_views_long, 1.0_f64),
    ("dedicated", avg_views_long, 2.0_f64),
    ("shorts", avg_views_shorts, 0.5_f64),
  ];

  let quotes: Vec<SponsorQuoteLine> = deliverables
    .into_iter()
    .map(|(deliverable, views, multiplier)| {
      let low = ((views as f64) / 1000.0) * cpm_low * multiplier;
      let high = ((views as f64) / 1000.0) * cpm_high * multiplier;
      SponsorQuoteLine {
        deliverable: deliverable.to_string(),
        cpm_range: (cpm_low, cpm_high),
        flat_fee_range: (low.round() as i64, high.round() as i64),
        avg_views_used: views,
      }
    })
    .collect();

  let quote_id = format!("quote_{}", now_ms());

  json_response(
    StatusCode::OK,
    serde_json::json!({
      "ok": true,
      "quote_id": quote_id,
      "quotes": quotes,
      "channel_id": channel_id,
      "niches": parsed.niches.unwrap_or_default(),
    }),
  )
}

#[derive(serde::Serialize)]
struct UploadItem {
  id: String,
  filename: String,
  channel_id: String,
  created_at: String,
  status: String,
}

async fn handle_youtube_uploads_list(
  method: &Method,
  headers: &HeaderMap,
  uri: &Uri,
) -> Result<Response<ResponseBody>, Error> {
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

  let tenant_id = get_query_param(uri, "tenant_id").unwrap_or_default();
  if tenant_id.trim().is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
    );
  }

  let pool = get_pool().await?;
  let channel_id = match get_query_param(uri, "channel_id")
    .map(|v| v.trim().to_string())
    .filter(|v| !v.is_empty())
  {
    Some(v) => v,
    None => fetch_youtube_channel_id(pool, tenant_id.trim())
      .await?
      .unwrap_or_default(),
  };

  if channel_id.trim().is_empty() {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_connected", "message": "No active YouTube channel for this tenant"}),
    );
  }

  let rows = sqlx::query_as::<_, (i64, String, String, DateTime<Utc>)>(
    r#"
      SELECT id, filename, status, created_at
      FROM yt_csv_uploads
      WHERE tenant_id = ?
        AND channel_id = ?
      ORDER BY created_at DESC
      LIMIT 20;
    "#,
  )
  .bind(tenant_id.trim())
  .bind(channel_id.trim())
  .fetch_all(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let items: Vec<UploadItem> = rows
    .into_iter()
    .map(|(id, filename, status, created_at)| UploadItem {
      id: format!("upload_{id}"),
      filename,
      channel_id: channel_id.clone(),
      created_at: created_at.to_rfc3339(),
      status,
    })
    .collect();

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "items": items, "channel_id": channel_id}),
  )
}

fn normalize_csv_header_name(input: &str) -> String {
  let mut out = String::with_capacity(input.len());
  let mut last_was_sep = false;
  for ch in input.trim().chars() {
    if ch.is_ascii_alphanumeric() {
      out.push(ch.to_ascii_lowercase());
      last_was_sep = false;
    } else if !last_was_sep {
      out.push('_');
      last_was_sep = true;
    }
  }
  out.trim_matches('_').to_string()
}

fn parse_i64_field(raw: &str) -> Option<i64> {
  let cleaned = raw.trim().replace(',', "");
  cleaned.parse::<i64>().ok()
}

fn parse_f64_field(raw: &str) -> Option<f64> {
  let cleaned = raw.trim().replace(',', "").replace('$', "");
  cleaned.parse::<f64>().ok()
}

fn parse_ctr_field(raw: &str) -> Option<f64> {
  let s = raw.trim();
  let is_percent = s.ends_with('%');
  let cleaned = s.trim_end_matches('%').replace(',', "");
  let v = cleaned.parse::<f64>().ok()?;
  if is_percent {
    Some(v / 100.0)
  } else {
    Some(v)
  }
}

#[derive(Debug, Clone)]
struct CsvMetricRow {
  dt: NaiveDate,
  video_id: String,
  estimated_revenue_usd: f64,
  impressions: i64,
  views: i64,
}

fn parse_csv_metrics(csv_text: &str) -> Result<Vec<CsvMetricRow>, String> {
  use std::collections::HashMap;

  if csv_text.trim().is_empty() {
    return Err("csv_text is empty".to_string());
  }

  let mut rdr = csv::ReaderBuilder::new()
    .has_headers(true)
    .flexible(true)
    .from_reader(csv_text.as_bytes());

  let headers = rdr
    .headers()
    .map_err(|e| format!("invalid csv headers: {e}"))?
    .clone();

  let mut idx: HashMap<String, usize> = HashMap::new();
  for (i, h) in headers.iter().enumerate() {
    idx.insert(normalize_csv_header_name(h), i);
  }

  let find_idx = |candidates: &[&str]| -> Option<usize> {
    for c in candidates {
      if let Some(i) = idx.get(*c) {
        return Some(*i);
      }
    }
    None
  };

  let dt_idx = find_idx(&["date", "day", "dt"]).ok_or_else(|| "missing date/day/dt column".to_string())?;
  let video_idx = find_idx(&["video_id", "videoid", "video"]);
  let views_idx = find_idx(&["views", "view"]);
  let impressions_idx = find_idx(&["impressions", "impr", "impression"]);
  let revenue_idx = find_idx(&[
    "revenue_usd",
    "estimated_revenue_usd",
    "estimatedrevenue",
    "estimated_revenue",
    "revenue",
  ]);
  let rpm_idx = find_idx(&["rpm"]);
  let ctr_idx = find_idx(&["ctr", "impressions_click_through_rate"]);

  let mut out: Vec<CsvMetricRow> = Vec::new();

  for (row_i, rec) in rdr.records().enumerate() {
    let rec = rec.map_err(|e| format!("invalid csv row {}: {}", row_i + 1, e))?;

    let dt_raw = rec.get(dt_idx).unwrap_or("").trim();
    let dt = parse_dt(dt_raw).ok_or_else(|| format!("invalid date at row {}: {}", row_i + 1, dt_raw))?;

    let video_id = video_idx
      .and_then(|i| rec.get(i))
      .map(|v| v.trim().to_string())
      .filter(|v| !v.is_empty())
      .unwrap_or_else(|| "csv_channel_total".to_string());

    let impressions = impressions_idx
      .and_then(|i| rec.get(i))
      .and_then(parse_i64_field)
      .unwrap_or(0)
      .max(0);

    let views_from_field = views_idx
      .and_then(|i| rec.get(i))
      .and_then(parse_i64_field);

    let views_from_ctr = match (ctr_idx, impressions) {
      (Some(i), impr) if impr > 0 => rec
        .get(i)
        .and_then(parse_ctr_field)
        .map(|ctr| ((impr as f64) * ctr).round() as i64),
      _ => None,
    };

    let views = views_from_field
      .or(views_from_ctr)
      .unwrap_or(0)
      .max(0);

    let revenue_from_field = revenue_idx
      .and_then(|i| rec.get(i))
      .and_then(parse_f64_field);

    let revenue_from_rpm = match (rpm_idx, views) {
      (Some(i), v) if v > 0 => rec
        .get(i)
        .and_then(parse_f64_field)
        .map(|rpm| (rpm * (v as f64)) / 1000.0),
      _ => None,
    };

    let revenue = revenue_from_field
      .or(revenue_from_rpm)
      .unwrap_or(0.0)
      .max(0.0);

    // Drop fully-empty rows (common in exports).
    if impressions == 0 && views == 0 && revenue == 0.0 {
      continue;
    }

    out.push(CsvMetricRow {
      dt,
      video_id,
      estimated_revenue_usd: revenue,
      impressions,
      views,
    });
  }

  Ok(out)
}

#[derive(Deserialize)]
struct UploadCsvRequest {
  tenant_id: String,
  channel_id: Option<String>,
  filename: String,
  csv_text: String,
}

async fn handle_youtube_upload_csv(
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

  let parsed: UploadCsvRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

  if parsed.tenant_id.trim().is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
    );
  }
  if parsed.filename.trim().is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "filename is required"}),
    );
  }

  // Guardrail: keep this endpoint safe for MVP use.
  if parsed.csv_text.len() > 5_000_000 {
    return json_response(
      StatusCode::PAYLOAD_TOO_LARGE,
      serde_json::json!({"ok": false, "error": "payload_too_large", "message": "csv_text too large"}),
    );
  }

  let pool = get_pool().await?;
  let tenant_id = parsed.tenant_id.trim();
  let channel_id = match parsed
    .channel_id
    .as_deref()
    .map(str::trim)
    .filter(|v| !v.is_empty())
  {
    Some(v) => v.to_string(),
    None => fetch_youtube_channel_id(pool, tenant_id).await?.unwrap_or_default(),
  };

  if channel_id.trim().is_empty() {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_connected", "message": "No active YouTube channel for this tenant"}),
    );
  }

  let insert = sqlx::query(
    r#"
      INSERT INTO yt_csv_uploads (tenant_id, channel_id, filename, status)
      VALUES (?, ?, ?, 'received');
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id.trim())
  .bind(parsed.filename.trim())
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let upload_id = insert.last_insert_id() as i64;

  let parsed_rows = match parse_csv_metrics(&parsed.csv_text) {
    Ok(rows) => rows,
    Err(err) => {
      sqlx::query(
        r#"
          UPDATE yt_csv_uploads
          SET status = 'error',
              error = ?,
              updated_at = CURRENT_TIMESTAMP(3)
          WHERE id = ? AND tenant_id = ? AND channel_id = ?;
        "#,
      )
      .bind(&err)
      .bind(upload_id)
      .bind(tenant_id)
      .bind(channel_id.trim())
      .execute(pool)
      .await
      .map_err(|e| -> Error { Box::new(e) })?;

      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_csv", "message": err}),
      );
    }
  };

  for row in parsed_rows.iter() {
    upsert_video_daily_metric(
      pool,
      tenant_id,
      channel_id.trim(),
      row.dt,
      &row.video_id,
      row.estimated_revenue_usd,
      row.impressions,
      row.views,
    )
    .await?;
  }

  sqlx::query(
    r#"
      UPDATE yt_csv_uploads
      SET status = 'parsed',
          rows_parsed = ?,
          error = NULL,
          updated_at = CURRENT_TIMESTAMP(3)
      WHERE id = ? AND tenant_id = ? AND channel_id = ?;
    "#,
  )
  .bind(parsed_rows.len() as i64)
  .bind(upload_id)
  .bind(tenant_id)
  .bind(channel_id.trim())
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "upload_id": format!("upload_{upload_id}"), "rows_parsed": parsed_rows.len(), "channel_id": channel_id}),
  )
}

fn severity_for_drop(drop_pct: f64) -> &'static str {
  if drop_pct >= 0.30 {
    "critical"
  } else if drop_pct >= 0.20 {
    "error"
  } else {
    "warning"
  }
}

async fn upsert_alert(
  pool: &sqlx::MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  alert_key: &str,
  kind: &str,
  severity: &str,
  message: &str,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO yt_alerts (
        tenant_id, channel_id, alert_key,
        kind, severity, message,
        detected_at, resolved_at
      )
      VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3), NULL)
      ON DUPLICATE KEY UPDATE
        kind = VALUES(kind),
        severity = VALUES(severity),
        message = VALUES(message),
        detected_at = CURRENT_TIMESTAMP(3),
        resolved_at = NULL,
        updated_at = CURRENT_TIMESTAMP(3);
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(alert_key)
  .bind(kind)
  .bind(severity)
  .bind(message)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

async fn auto_resolve_alert(
  pool: &sqlx::MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  alert_key: &str,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      UPDATE yt_alerts
      SET resolved_at = CURRENT_TIMESTAMP(3),
          updated_at = CURRENT_TIMESTAMP(3)
      WHERE tenant_id = ?
        AND channel_id = ?
        AND alert_key = ?
        AND resolved_at IS NULL;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(alert_key)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

async fn evaluate_alerts(
  pool: &sqlx::MySqlPool,
  tenant_id: &str,
  channel_id: &str,
) -> Result<(), Error> {
  let today = Utc::now().date_naive();
  let current_start = today - Duration::days(7);
  let current_end = today - Duration::days(1);
  let baseline_start = today - Duration::days(14);
  let baseline_end = today - Duration::days(8);

  let (cur_rev, cur_views) = sqlx::query_as::<_, (f64, i64)>(
    r#"
      SELECT CAST(COALESCE(SUM(estimated_revenue_usd), 0) AS DOUBLE) AS revenue_usd,
             CAST(COALESCE(SUM(views), 0) AS SIGNED) AS views
      FROM video_daily_metrics
      WHERE tenant_id = ?
        AND channel_id = ?
        AND dt BETWEEN ? AND ?;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(current_start)
  .bind(current_end)
  .fetch_one(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let (base_rev, base_views) = sqlx::query_as::<_, (f64, i64)>(
    r#"
      SELECT CAST(COALESCE(SUM(estimated_revenue_usd), 0) AS DOUBLE) AS revenue_usd,
             CAST(COALESCE(SUM(views), 0) AS SIGNED) AS views
      FROM video_daily_metrics
      WHERE tenant_id = ?
        AND channel_id = ?
        AND dt BETWEEN ? AND ?;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(baseline_start)
  .bind(baseline_end)
  .fetch_one(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let cur_rpm = if cur_views > 0 {
    (cur_rev / (cur_views as f64)) * 1000.0
  } else {
    0.0
  };
  let base_rpm = if base_views > 0 {
    (base_rev / (base_views as f64)) * 1000.0
  } else {
    0.0
  };

  let can_compare = cur_views >= 1000 && base_views >= 1000 && base_rpm > 0.0;
  if can_compare {
    let drop_pct = ((base_rpm - cur_rpm) / base_rpm).max(-1.0);
    if drop_pct >= 0.10 {
      let severity = severity_for_drop(drop_pct);
      let msg = format!(
        "Revenue per mille dropped {:.0}% vs previous 7d (current ${:.2}, prev ${:.2}).",
        drop_pct * 100.0,
        cur_rpm,
        base_rpm
      );
      upsert_alert(
        pool,
        tenant_id,
        channel_id,
        "rpm_drop_7d",
        "RPM drop",
        severity,
        &msg,
      )
      .await?;
    } else {
      auto_resolve_alert(pool, tenant_id, channel_id, "rpm_drop_7d").await?;
    }
  }

  let max_dt = sqlx::query_scalar::<_, Option<NaiveDate>>(
    r#"
      SELECT MAX(dt) AS max_dt
      FROM video_daily_metrics
      WHERE tenant_id = ? AND channel_id = ?;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .fetch_one(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  match max_dt {
    None => {
      upsert_alert(
        pool,
        tenant_id,
        channel_id,
        "metrics_stale",
        "Data missing",
        "info",
        "No metrics found yet. Upload CSV or wait for the first sync.",
      )
      .await?;
    }
    Some(dt) => {
      let age_days = (today - dt).num_days();
      if age_days >= 3 {
        let msg = format!(
          "Metrics look stale (latest day {}). Upload CSV or run a sync.",
          dt.to_string()
        );
        upsert_alert(
          pool,
          tenant_id,
          channel_id,
          "metrics_stale",
          "Data stale",
          "warning",
          &msg,
        )
        .await?;
      } else {
        auto_resolve_alert(pool, tenant_id, channel_id, "metrics_stale").await?;
      }
    }
  }

  Ok(())
}

#[derive(serde::Serialize)]
struct AlertItem {
  id: String,
  kind: String,
  severity: String,
  message: String,
  detected_at: String,
  resolved_at: Option<String>,
}

#[derive(Deserialize)]
struct ResolveAlertRequest {
  tenant_id: String,
  id: String,
}

fn parse_prefixed_id(raw: &str, prefix: &str) -> Option<i64> {
  let s = raw.trim();
  let s = s.strip_prefix(prefix).unwrap_or(s);
  s.parse::<i64>().ok()
}

async fn handle_youtube_alerts(method: &Method, headers: &HeaderMap, uri: &Uri, body: Option<Bytes>) -> Result<Response<ResponseBody>, Error> {
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

  if method == Method::GET {
    let tenant_id = get_query_param(uri, "tenant_id").unwrap_or_default();
    if tenant_id.trim().is_empty() {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
      );
    }

    let pool = get_pool().await?;
    let channel_id = match get_query_param(uri, "channel_id")
      .map(|v| v.trim().to_string())
      .filter(|v| !v.is_empty())
    {
      Some(v) => v,
      None => fetch_youtube_channel_id(pool, tenant_id.trim())
        .await?
        .unwrap_or_default(),
    };

    if channel_id.trim().is_empty() {
      return json_response(
        StatusCode::NOT_FOUND,
        serde_json::json!({"ok": false, "error": "not_connected", "message": "No active YouTube channel for this tenant"}),
      );
    }

    // Best-effort: keep alerts fresh without requiring a separate job.
    let eval_error = match evaluate_alerts(pool, tenant_id.trim(), channel_id.trim()).await {
      Ok(()) => None,
      Err(err) => Some(truncate_string(&err.to_string(), 2000)),
    };

    let rows = match sqlx::query_as::<_, (i64, String, String, String, DateTime<Utc>, Option<DateTime<Utc>>)>(
        r#"
          SELECT id, kind, severity, message, detected_at, resolved_at
          FROM yt_alerts
          WHERE tenant_id = ? AND channel_id = ?
          ORDER BY (resolved_at IS NULL) DESC, detected_at DESC
          LIMIT 50;
        "#,
      )
      .bind(tenant_id.trim())
      .bind(channel_id.trim())
      .fetch_all(pool)
      .await
    {
      Ok(v) => v,
      Err(e) => {
        return json_response(
          StatusCode::OK,
          serde_json::json!({
            "ok": false,
            "error": "alerts_query_failed",
            "message": truncate_string(&e.to_string(), 2000),
            "channel_id": channel_id,
            "eval_error": eval_error,
          }),
        );
      }
    };

    let items: Vec<AlertItem> = rows
      .into_iter()
      .map(|(id, kind, severity, message, detected_at, resolved_at)| AlertItem {
        id: format!("alert_{id}"),
        kind,
        severity,
        message,
        detected_at: detected_at.to_rfc3339(),
        resolved_at: resolved_at.map(|dt| dt.to_rfc3339()),
      })
      .collect();

    return json_response(
      StatusCode::OK,
      serde_json::json!({"ok": true, "items": items, "channel_id": channel_id, "eval_error": eval_error}),
    );
  }

  if method == Method::POST {
    let Some(body) = body else {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "missing body"}),
      );
    };

    let parsed: ResolveAlertRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
      Box::new(std::io::Error::other(format!("invalid json body: {e}")))
    })?;

    if parsed.tenant_id.trim().is_empty() || parsed.id.trim().is_empty() {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id and id are required"}),
      );
    }

    let Some(alert_id) = parse_prefixed_id(&parsed.id, "alert_") else {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "invalid alert id"}),
      );
    };

    let pool = get_pool().await?;
    let updated = sqlx::query(
      r#"
        UPDATE yt_alerts
        SET resolved_at = CURRENT_TIMESTAMP(3),
            updated_at = CURRENT_TIMESTAMP(3)
        WHERE id = ? AND tenant_id = ?;
      "#,
    )
    .bind(alert_id)
    .bind(parsed.tenant_id.trim())
    .execute(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

    return json_response(
      StatusCode::OK,
      serde_json::json!({"ok": true, "updated": updated.rows_affected() > 0}),
    );
  }

  json_response(
    StatusCode::METHOD_NOT_ALLOWED,
    serde_json::json!({"ok": false, "error": "method_not_allowed"}),
  )
}

#[derive(serde::Serialize)]
struct ExperimentVariantResponse {
  variant_id: String,
  status: String,
  payload: serde_json::Value,
  impressions: Option<i64>,
  views: Option<i64>,
  revenue_usd: Option<f64>,
  ctr: Option<f64>,
  rpm: Option<f64>,
}

#[derive(serde::Serialize)]
struct ExperimentResponse {
  id: String,
  channel_id: String,
  video_ids: Vec<String>,
  r#type: String,
  state: String,
  stop_loss_pct: Option<f64>,
  planned_duration_days: Option<i64>,
  started_at: Option<String>,
  ended_at: Option<String>,
  variants: Option<Vec<ExperimentVariantResponse>>,
}

fn parse_video_ids_json(raw: &str) -> Vec<String> {
  serde_json::from_str::<Vec<String>>(raw)
    .unwrap_or_default()
    .into_iter()
    .map(|v| v.trim().to_string())
    .filter(|v| !v.is_empty())
    .collect()
}

fn json_string_field(payload: &serde_json::Value, key: &str) -> Option<String> {
  payload
    .get(key)
    .and_then(|v| v.as_str())
    .map(|v| v.trim().to_string())
    .filter(|v| !v.is_empty())
}

async fn fetch_experiment_variants(
  pool: &sqlx::MySqlPool,
  experiment_id: i64,
) -> Result<Vec<ExperimentVariantResponse>, Error> {
  let rows = sqlx::query_as::<_, (String, String, String)>(
    r#"
      SELECT variant_id, payload_json, status
      FROM yt_experiment_variants
      WHERE experiment_id = ?
      ORDER BY variant_id ASC;
    "#,
  )
  .bind(experiment_id)
  .fetch_all(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(
    rows
      .into_iter()
      .map(|(variant_id, payload_json, status)| {
        let payload = serde_json::from_str::<serde_json::Value>(&payload_json)
          .ok()
          .and_then(|v| if v.is_object() { Some(v) } else { None })
          .unwrap_or_else(|| serde_json::json!({}));
        ExperimentVariantResponse {
          variant_id,
          status,
          payload,
          impressions: None,
          views: None,
          revenue_usd: None,
          ctr: None,
          rpm: None,
        }
      })
      .collect(),
  )
}

#[derive(Debug, Clone, Copy, Default)]
struct AggMetrics {
  revenue_usd: f64,
  impressions: i64,
  views: i64,
}

fn agg_ctr(m: AggMetrics) -> Option<f64> {
  if m.impressions > 0 {
    Some((m.views as f64) / (m.impressions as f64))
  } else {
    None
  }
}

fn agg_rpm(m: AggMetrics) -> Option<f64> {
  if m.views > 0 {
    Some((m.revenue_usd / (m.views as f64)) * 1000.0)
  } else {
    None
  }
}

async fn aggregate_metrics_for_videos(
  pool: &sqlx::MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  video_ids: &[String],
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> Result<AggMetrics, Error> {
  if start_dt > end_dt || video_ids.is_empty() {
    return Ok(AggMetrics::default());
  }

  let mut qb = sqlx::QueryBuilder::<sqlx::MySql>::new(
    r#"
      SELECT CAST(COALESCE(SUM(estimated_revenue_usd), 0) AS DOUBLE) AS revenue_usd,
             CAST(COALESCE(SUM(impressions), 0) AS SIGNED) AS impressions,
             CAST(COALESCE(SUM(views), 0) AS SIGNED) AS views
      FROM video_daily_metrics
      WHERE tenant_id =
    "#,
  );
  qb.push_bind(tenant_id);
  qb.push(" AND channel_id = ");
  qb.push_bind(channel_id);
  qb.push(" AND dt BETWEEN ");
  qb.push_bind(start_dt);
  qb.push(" AND ");
  qb.push_bind(end_dt);
  qb.push(" AND video_id IN (");
  {
    let mut separated = qb.separated(", ");
    for vid in video_ids {
      separated.push_bind(vid);
    }
  }
  qb.push(");");

  let (revenue_usd, impressions, views) = qb
    .build_query_as::<(f64, i64, i64)>()
    .fetch_one(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

  Ok(AggMetrics {
    revenue_usd,
    impressions,
    views,
  })
}

fn enrich_experiment_variants_with_stats(
  mut variants: Vec<ExperimentVariantResponse>,
  baseline: AggMetrics,
  current: AggMetrics,
) -> Vec<ExperimentVariantResponse> {
  if variants.is_empty() {
    return variants;
  }

  let baseline_idx = variants
    .iter()
    .position(|v| v.variant_id == "A")
    .or(Some(0));

  let current_idx = variants
    .iter()
    .position(|v| v.variant_id == "B")
    .or_else(|| {
      if variants.len() >= 2 {
        Some(1)
      } else {
        None
      }
    });

  if let Some(i) = baseline_idx {
    if let Some(v) = variants.get_mut(i) {
      v.impressions = Some(baseline.impressions);
      v.views = Some(baseline.views);
      v.revenue_usd = Some(round2(baseline.revenue_usd));
      v.ctr = agg_ctr(baseline).map(|v| (v * 10000.0).round() / 10000.0);
      v.rpm = agg_rpm(baseline).map(round2);
    }
  }

  if let Some(i) = current_idx {
    if let Some(v) = variants.get_mut(i) {
      v.impressions = Some(current.impressions);
      v.views = Some(current.views);
      v.revenue_usd = Some(round2(current.revenue_usd));
      v.ctr = agg_ctr(current).map(|v| (v * 10000.0).round() / 10000.0);
      v.rpm = agg_rpm(current).map(round2);
    }
  }

  variants
}

async fn handle_youtube_experiment_get(
  method: &Method,
  headers: &HeaderMap,
  uri: &Uri,
) -> Result<Response<ResponseBody>, Error> {
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

  let tenant_id = get_query_param(uri, "tenant_id").unwrap_or_default();
  if tenant_id.trim().is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
    );
  }

  let id_raw = get_query_param(uri, "id").unwrap_or_default();
  let Some(exp_id) = parse_prefixed_id(&id_raw, "exp_") else {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "invalid experiment id"}),
    );
  };

  let pool = get_pool().await?;
  let row = sqlx::query_as::<_, (i64, String, String, String, String, Option<f64>, Option<i64>, Option<DateTime<Utc>>, Option<DateTime<Utc>>)>(
    r#"
      SELECT id, channel_id, type, state, video_ids_json,
             stop_loss_pct, planned_duration_days,
             started_at, ended_at
      FROM yt_experiments
      WHERE id = ? AND tenant_id = ?
      LIMIT 1;
    "#,
  )
  .bind(exp_id)
  .bind(tenant_id.trim())
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let Some((id, channel_id, exp_type, state, video_ids_json, stop_loss_pct, planned_duration_days, started_at, ended_at)) = row else {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_found"}),
    );
  };

  let video_ids = parse_video_ids_json(&video_ids_json);
  let mut variants = fetch_experiment_variants(pool, id).await?;

  if let Some(started_at) = started_at {
    let start_dt = started_at.date_naive();
    let baseline_start_dt = start_dt - Duration::days(7);
    let baseline_end_dt = start_dt - Duration::days(1);

    let last_complete_dt = Utc::now().date_naive() - Duration::days(1);
    let ended_dt = ended_at.map(|dt| dt.date_naive());
    let current_end_dt = ended_dt.unwrap_or(last_complete_dt).min(last_complete_dt);

    let baseline =
      aggregate_metrics_for_videos(pool, tenant_id.trim(), channel_id.trim(), &video_ids, baseline_start_dt, baseline_end_dt)
        .await?;
    let current =
      aggregate_metrics_for_videos(pool, tenant_id.trim(), channel_id.trim(), &video_ids, start_dt, current_end_dt).await?;

    variants = enrich_experiment_variants_with_stats(variants, baseline, current);
  }

  let experiment = ExperimentResponse {
    id: format!("exp_{id}"),
    channel_id,
    video_ids,
    r#type: exp_type,
    state,
    stop_loss_pct,
    planned_duration_days,
    started_at: started_at.map(|dt| dt.to_rfc3339()),
    ended_at: ended_at.map(|dt| dt.to_rfc3339()),
    variants: if variants.is_empty() { None } else { Some(variants) },
  };

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "experiment": experiment}),
  )
}

#[derive(Deserialize)]
struct CreateExperimentVariantRequest {
  id: String,
  payload: serde_json::Value,
}

#[derive(Deserialize)]
struct CreateExperimentRequest {
  tenant_id: String,
  channel_id: Option<String>,
  r#type: String,
  video_ids: Vec<String>,
  stop_loss_pct: Option<f64>,
  planned_duration_days: Option<i64>,
  variants: Vec<CreateExperimentVariantRequest>,
}

#[derive(Deserialize)]
struct MutateExperimentRequest {
  tenant_id: String,
  id: String,
  op: String, // stop | rollback
}

fn normalize_experiment_type(raw: &str) -> Option<&'static str> {
  match raw.trim() {
    "title" => Some("title"),
    "thumbnail" => Some("thumbnail"),
    "publish_time" => Some("publish_time"),
    _ => None,
  }
}

async fn handle_youtube_experiments(
  method: &Method,
  headers: &HeaderMap,
  uri: &Uri,
  body: Option<Bytes>,
) -> Result<Response<ResponseBody>, Error> {
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

  if method == Method::GET {
    let tenant_id = get_query_param(uri, "tenant_id").unwrap_or_default();
    if tenant_id.trim().is_empty() {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
      );
    }

    let pool = get_pool().await?;
    let channel_id = match get_query_param(uri, "channel_id")
      .map(|v| v.trim().to_string())
      .filter(|v| !v.is_empty())
    {
      Some(v) => v,
      None => fetch_youtube_channel_id(pool, tenant_id.trim())
        .await?
        .unwrap_or_default(),
    };

    if channel_id.trim().is_empty() {
      return json_response(
        StatusCode::NOT_FOUND,
        serde_json::json!({"ok": false, "error": "not_connected", "message": "No active YouTube channel for this tenant"}),
      );
    }

    let rows = sqlx::query_as::<_, (i64, String, String, String, String, Option<f64>, Option<i64>, Option<DateTime<Utc>>, Option<DateTime<Utc>>)>(
      r#"
        SELECT id, channel_id, type, state, video_ids_json,
               stop_loss_pct, planned_duration_days,
               started_at, ended_at
        FROM yt_experiments
        WHERE tenant_id = ?
          AND channel_id = ?
        ORDER BY created_at DESC
        LIMIT 50;
      "#,
    )
    .bind(tenant_id.trim())
    .bind(channel_id.trim())
    .fetch_all(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

    let last_complete_dt = Utc::now().date_naive() - Duration::days(1);

    let mut out: Vec<ExperimentResponse> = Vec::with_capacity(rows.len());
    for (id, channel_id, exp_type, state, video_ids_json, stop_loss_pct, planned_duration_days, started_at, ended_at) in rows {
      let video_ids = parse_video_ids_json(&video_ids_json);
      let mut variants = fetch_experiment_variants(pool, id).await?;

      if let Some(started_at) = started_at {
        let start_dt = started_at.date_naive();
        let baseline_start_dt = start_dt - Duration::days(7);
        let baseline_end_dt = start_dt - Duration::days(1);

        let ended_dt = ended_at.map(|dt| dt.date_naive());
        let current_end_dt = ended_dt.unwrap_or(last_complete_dt).min(last_complete_dt);

        let baseline = aggregate_metrics_for_videos(
          pool,
          tenant_id.trim(),
          channel_id.trim(),
          &video_ids,
          baseline_start_dt,
          baseline_end_dt,
        )
        .await?;
        let current = aggregate_metrics_for_videos(
          pool,
          tenant_id.trim(),
          channel_id.trim(),
          &video_ids,
          start_dt,
          current_end_dt,
        )
        .await?;

        variants = enrich_experiment_variants_with_stats(variants, baseline, current);
      }
      out.push(ExperimentResponse {
        id: format!("exp_{id}"),
        channel_id,
        video_ids,
        r#type: exp_type,
        state,
        stop_loss_pct,
        planned_duration_days,
        started_at: started_at.map(|dt| dt.to_rfc3339()),
        ended_at: ended_at.map(|dt| dt.to_rfc3339()),
        variants: if variants.is_empty() { None } else { Some(variants) },
      });
    }

    return json_response(
      StatusCode::OK,
      serde_json::json!({"ok": true, "items": out, "channel_id": channel_id}),
    );
  }

  if method == Method::POST {
    let Some(body) = body else {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "missing body"}),
      );
    };

    let v: serde_json::Value = serde_json::from_slice(&body).map_err(|e| -> Error {
      Box::new(std::io::Error::other(format!("invalid json body: {e}")))
    })?;

    if v.get("op").is_some() {
      let parsed: MutateExperimentRequest = serde_json::from_value(v).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid mutate body: {e}")))
      })?;

      if parsed.tenant_id.trim().is_empty() || parsed.id.trim().is_empty() || parsed.op.trim().is_empty() {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id, id, op are required"}),
        );
      }

      let Some(exp_id) = parse_prefixed_id(&parsed.id, "exp_") else {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "invalid experiment id"}),
        );
      };

      let state = match parsed.op.as_str() {
        "stop" => "stopped",
        "rollback" => "rolled_back",
        _ => {
          return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "op must be stop or rollback"}),
          )
        }
      };

      let pool = get_pool().await?;

      let row = sqlx::query_as::<_, (i64, String, String, String)>(
        r#"
          SELECT id, channel_id, type, video_ids_json
          FROM yt_experiments
          WHERE id = ? AND tenant_id = ?
          LIMIT 1;
        "#,
      )
      .bind(exp_id)
      .bind(parsed.tenant_id.trim())
      .fetch_optional(pool)
      .await
      .map_err(|e| -> Error { Box::new(e) })?;

      let Some((id, channel_id, exp_type, video_ids_json)) = row else {
        return json_response(
          StatusCode::NOT_FOUND,
          serde_json::json!({"ok": false, "error": "not_found"}),
        );
      };

      let video_ids = parse_video_ids_json(&video_ids_json);
      if video_ids.len() != 1 {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "MVP only supports a single video_id per experiment"}),
        );
      }
      let primary_video_id = video_ids[0].trim().to_string();

      let baseline_payload_json = sqlx::query_scalar::<_, String>(
        r#"
          SELECT payload_json
          FROM yt_experiment_variants
          WHERE experiment_id = ?
            AND variant_id = 'A'
          LIMIT 1;
        "#,
      )
      .bind(id)
      .fetch_optional(pool)
      .await
      .map_err(|e| -> Error { Box::new(e) })?;

      let Some(baseline_payload_json) = baseline_payload_json else {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "Missing baseline variant A payload"}),
        );
      };

      let baseline_payload = serde_json::from_str::<serde_json::Value>(&baseline_payload_json)
        .ok()
        .and_then(|v| if v.is_object() { Some(v) } else { None })
        .unwrap_or_else(|| serde_json::json!({}));

      let baseline_title = if exp_type == "title" {
        json_string_field(&baseline_payload, "title")
      } else {
        None
      };
      let baseline_thumbnail_url = if exp_type == "thumbnail" {
        json_string_field(&baseline_payload, "thumbnail_url")
          .or_else(|| json_string_field(&baseline_payload, "thumbnailUrl"))
      } else {
        None
      };
      let baseline_publish_at = if exp_type == "publish_time" {
        json_string_field(&baseline_payload, "publish_at")
          .or_else(|| json_string_field(&baseline_payload, "publishAt"))
      } else {
        None
      };

      let mut tokens = fetch_youtube_connection_tokens(pool, parsed.tenant_id.trim(), channel_id.trim())
        .await?
        .ok_or_else(|| Box::new(std::io::Error::other("missing youtube channel connection")) as Error)?;

      // Proactive refresh if expired (best-effort).
      let needs_refresh = tokens
        .expires_at
        .map(|dt| dt <= chrono::Utc::now())
        .unwrap_or(false);
      if needs_refresh {
        if let Some(refresh) = tokens.refresh_token.clone() {
          let app = fetch_or_seed_youtube_oauth_app_config(pool, parsed.tenant_id.trim()).await?;
          let Some(app) = app else {
            return json_response(
              StatusCode::NOT_FOUND,
              serde_json::json!({
                "ok": false,
                "error": "not_configured",
                "message": "Missing YouTube OAuth app config for tenant. Configure via /api/oauth/youtube/app_config or set YOUTUBE_CLIENT_ID/YOUTUBE_CLIENT_SECRET/YOUTUBE_REDIRECT_URI on the Rust backend."
              }),
            );
          };
          let Some(client_secret) = app
            .client_secret
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty()) else {
            return json_response(
              StatusCode::NOT_FOUND,
              serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing YouTube OAuth client_secret for tenant"}),
            );
          };

          let (client, _redirect) =
            youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
          let refreshed = refresh_tokens(&client, &refresh).await?;
          update_youtube_connection_tokens(pool, parsed.tenant_id.trim(), channel_id.trim(), &refreshed).await?;
          tokens.access_token = refreshed.access_token;
          tokens.refresh_token = refreshed.refresh_token.or(Some(refresh));
        }
      }

      let rollback_result: Result<(), String> = match exp_type.as_str() {
        "title" => {
          let title = baseline_title.unwrap_or_default();
          if title.trim().is_empty() {
            Err("baseline variant A missing title".to_string())
          } else {
            update_video_title(&tokens.access_token, &primary_video_id, &title)
              .await
              .map_err(|e| e.to_string())
          }
        }
        "thumbnail" => {
          let url = baseline_thumbnail_url.unwrap_or_default();
          if url.trim().is_empty() {
            Err("baseline variant A missing thumbnail_url".to_string())
          } else {
            set_video_thumbnail_from_url(&tokens.access_token, &primary_video_id, &url)
              .await
              .map_err(|e| e.to_string())
          }
        }
        "publish_time" => {
          let publish_at = baseline_publish_at.unwrap_or_default();
          if publish_at.trim().is_empty() {
            Err("baseline variant A missing publish_at".to_string())
          } else {
            update_video_publish_at(&tokens.access_token, &primary_video_id, &publish_at)
              .await
              .map_err(|e| e.to_string())
          }
        }
        _ => Ok(()),
      };

      if let Err(err) = rollback_result {
        return json_response(
          StatusCode::BAD_GATEWAY,
          serde_json::json!({"ok": false, "error": "rollback_failed", "message": err}),
        );
      }

      let updated = sqlx::query(
        r#"
          UPDATE yt_experiments
          SET state = ?,
              ended_at = CURRENT_TIMESTAMP(3),
              updated_at = CURRENT_TIMESTAMP(3)
          WHERE id = ? AND tenant_id = ?;
        "#,
      )
      .bind(state)
      .bind(exp_id)
      .bind(parsed.tenant_id.trim())
      .execute(pool)
      .await
      .map_err(|e| -> Error { Box::new(e) })?;

      let _ = sqlx::query(
        r#"
          UPDATE yt_experiment_variants
          SET status = CASE
            WHEN variant_id = 'A' THEN 'active'
            WHEN variant_id = 'B' THEN ?
            ELSE status
          END,
          updated_at = CURRENT_TIMESTAMP(3)
          WHERE experiment_id = ?;
        "#,
      )
      .bind(state)
      .bind(exp_id)
      .execute(pool)
      .await;

      return json_response(
        StatusCode::OK,
        serde_json::json!({"ok": true, "updated": updated.rows_affected() > 0}),
      );
    }

    let parsed: CreateExperimentRequest = serde_json::from_value(v).map_err(|e| -> Error {
      Box::new(std::io::Error::other(format!("invalid create body: {e}")))
    })?;

    let tenant_id = parsed.tenant_id.trim();
    if tenant_id.is_empty() {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
      );
    }

    let Some(exp_type) = normalize_experiment_type(&parsed.r#type) else {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "type must be title|thumbnail|publish_time"}),
      );
    };

    let video_ids: Vec<String> = parsed
      .video_ids
      .into_iter()
      .map(|v| v.trim().to_string())
      .filter(|v| !v.is_empty())
      .collect();

    if video_ids.is_empty() {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "video_ids is required"}),
      );
    }

    let variants: Vec<CreateExperimentVariantRequest> = parsed
      .variants
      .into_iter()
      .filter(|v| !v.id.trim().is_empty())
      .collect();

    if variants.is_empty() {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "variants is required"}),
      );
    }

    let pool = get_pool().await?;
    let channel_id = match parsed
      .channel_id
      .as_deref()
      .map(str::trim)
      .filter(|v| !v.is_empty())
    {
      Some(v) => v.to_string(),
      None => fetch_youtube_channel_id(pool, tenant_id).await?.unwrap_or_default(),
    };

    if channel_id.trim().is_empty() {
      return json_response(
        StatusCode::NOT_FOUND,
        serde_json::json!({"ok": false, "error": "not_connected", "message": "No active YouTube channel for this tenant"}),
      );
    }

    if video_ids.len() != 1 {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "MVP only supports a single video_id per experiment"}),
      );
    }

    let primary_video_id = video_ids[0].trim().to_string();

    let payload_b = variants
      .iter()
      .find(|v| v.id.trim() == "B")
      .map(|v| v.payload.clone())
      .unwrap_or_else(|| serde_json::json!({}));

    let desired_title = if exp_type == "title" {
      json_string_field(&payload_b, "title")
    } else {
      None
    };
    let desired_thumbnail_url = if exp_type == "thumbnail" {
      json_string_field(&payload_b, "thumbnail_url")
        .or_else(|| json_string_field(&payload_b, "thumbnailUrl"))
    } else {
      None
    };
    let desired_publish_at = if exp_type == "publish_time" {
      json_string_field(&payload_b, "publish_at")
        .or_else(|| json_string_field(&payload_b, "publishAt"))
    } else {
      None
    };

    if exp_type == "title" && desired_title.is_none() {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "Variant B payload must include title"}),
      );
    }
    if exp_type == "thumbnail" && desired_thumbnail_url.is_none() {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "Variant B payload must include thumbnail_url"}),
      );
    }
    if exp_type == "publish_time" && desired_publish_at.is_none() {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": "Variant B payload must include publish_at (RFC3339)"}),
      );
    }

    let mut tokens = fetch_youtube_connection_tokens(pool, tenant_id, channel_id.trim())
      .await?
      .ok_or_else(|| Box::new(std::io::Error::other("missing youtube channel connection")) as Error)?;

    // Proactive refresh if expired (best-effort).
    let needs_refresh = tokens
      .expires_at
      .map(|dt| dt <= chrono::Utc::now())
      .unwrap_or(false);
    if needs_refresh {
      if let Some(refresh) = tokens.refresh_token.clone() {
        let app = fetch_or_seed_youtube_oauth_app_config(pool, tenant_id).await?;
        let Some(app) = app else {
          return json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({
              "ok": false,
              "error": "not_configured",
              "message": "Missing YouTube OAuth app config for tenant. Configure via /api/oauth/youtube/app_config or set YOUTUBE_CLIENT_ID/YOUTUBE_CLIENT_SECRET/YOUTUBE_REDIRECT_URI on the Rust backend."
            }),
          );
        };
        let Some(client_secret) = app
          .client_secret
          .as_deref()
          .map(str::trim)
          .filter(|v| !v.is_empty()) else {
          return json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing YouTube OAuth client_secret for tenant"}),
          );
        };

        let (client, _redirect) =
          youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
        let refreshed = refresh_tokens(&client, &refresh).await?;
        update_youtube_connection_tokens(pool, tenant_id, channel_id.trim(), &refreshed).await?;
        tokens.access_token = refreshed.access_token;
        tokens.refresh_token = refreshed.refresh_token.or(Some(refresh));
      }
    }

    let baseline_snapshot = match fetch_video_snapshot(&tokens.access_token, &primary_video_id).await {
      Ok(v) => v,
      Err(err) => {
        return json_response(
          StatusCode::BAD_GATEWAY,
          serde_json::json!({"ok": false, "error": "youtube_api_error", "message": err.to_string(), "status": err.status}),
        );
      }
    };

    let baseline_payload = match exp_type {
      "title" => serde_json::json!({"title": baseline_snapshot.title}),
      "thumbnail" => {
        let Some(url) = baseline_snapshot.thumbnail_url.clone() else {
          return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "Could not determine current thumbnail URL for baseline"}),
          );
        };
        serde_json::json!({"thumbnail_url": url})
      }
      "publish_time" => {
        let Some(publish_at) = baseline_snapshot.publish_at.clone() else {
          return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "publish_time experiments only support scheduled videos (missing publishAt)"}),
          );
        };
        if baseline_snapshot.privacy_status.as_deref() != Some("private") {
          return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"ok": false, "error": "bad_request", "message": "publish_time experiments only support scheduled videos (privacyStatus must be private)"}),
          );
        }
        serde_json::json!({"publish_at": publish_at})
      }
      _ => serde_json::json!({}),
    };

    let video_ids_json = serde_json::to_string(&video_ids).unwrap_or_else(|_| "[]".to_string());

    let mut tx = pool.begin().await.map_err(|e| -> Error { Box::new(e) })?;

    let insert = sqlx::query(
      r#"
        INSERT INTO yt_experiments (
          tenant_id, channel_id,
          type, state,
          video_ids_json,
          stop_loss_pct,
          planned_duration_days,
          started_at,
          ended_at
        )
        VALUES (?, ?, ?, 'draft', ?, ?, ?, NULL, NULL);
      "#,
    )
    .bind(tenant_id)
    .bind(channel_id.trim())
    .bind(exp_type)
    .bind(video_ids_json)
    .bind(parsed.stop_loss_pct)
    .bind(parsed.planned_duration_days)
    .execute(&mut *tx)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

    let exp_id = insert.last_insert_id() as i64;

    for variant in variants.iter() {
      let (payload, status) = if variant.id.trim() == "A" {
        (baseline_payload.clone(), "control")
      } else {
        let payload = if variant.payload.is_object() {
          variant.payload.clone()
        } else {
          serde_json::json!({})
        };
        let status = if variant.id.trim() == "B" { "pending" } else { "pending" };
        (payload, status)
      };

      let payload_json = serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string());
      sqlx::query(
        r#"
          INSERT INTO yt_experiment_variants (experiment_id, variant_id, payload_json, status)
          VALUES (?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            payload_json = VALUES(payload_json),
            status = VALUES(status),
            updated_at = CURRENT_TIMESTAMP(3);
        "#,
      )
      .bind(exp_id)
      .bind(variant.id.trim())
      .bind(payload_json)
      .bind(status)
      .execute(&mut *tx)
      .await
      .map_err(|e| -> Error { Box::new(e) })?;
    }

    tx.commit().await.map_err(|e| -> Error { Box::new(e) })?;

    let apply_result: Result<(), String> = match exp_type {
      "title" => {
        let title = desired_title.clone().unwrap_or_default();
        update_video_title(&tokens.access_token, &primary_video_id, &title)
          .await
          .map_err(|e| e.to_string())
      }
      "thumbnail" => {
        let url = desired_thumbnail_url.clone().unwrap_or_default();
        set_video_thumbnail_from_url(&tokens.access_token, &primary_video_id, &url)
          .await
          .map_err(|e| e.to_string())
      }
      "publish_time" => {
        let publish_at = desired_publish_at.clone().unwrap_or_default();
        update_video_publish_at(&tokens.access_token, &primary_video_id, &publish_at)
          .await
          .map_err(|e| e.to_string())
      }
      _ => Ok(()),
    };

    match apply_result {
      Ok(()) => {
        sqlx::query(
          r#"
            UPDATE yt_experiments
            SET state = 'running',
                started_at = CURRENT_TIMESTAMP(3),
                updated_at = CURRENT_TIMESTAMP(3)
            WHERE id = ? AND tenant_id = ?;
          "#,
        )
        .bind(exp_id)
        .bind(tenant_id)
        .execute(pool)
        .await
        .map_err(|e| -> Error { Box::new(e) })?;

        let _ = sqlx::query(
          r#"
            UPDATE yt_experiment_variants
            SET status = CASE
              WHEN variant_id = 'A' THEN 'control'
              WHEN variant_id = 'B' THEN 'active'
              ELSE status
            END,
            updated_at = CURRENT_TIMESTAMP(3)
            WHERE experiment_id = ?;
          "#,
        )
        .bind(exp_id)
        .execute(pool)
        .await;

        return json_response(
          StatusCode::CREATED,
          serde_json::json!({"ok": true, "experiment_id": format!("exp_{exp_id}"), "channel_id": channel_id, "applied": true}),
        );
      }
      Err(err) => {
        let _ = sqlx::query(
          r#"
            UPDATE yt_experiments
            SET state = 'failed',
                ended_at = CURRENT_TIMESTAMP(3),
                updated_at = CURRENT_TIMESTAMP(3)
            WHERE id = ? AND tenant_id = ?;
          "#,
        )
        .bind(exp_id)
        .bind(tenant_id)
        .execute(pool)
        .await;

        let _ = sqlx::query(
          r#"
            UPDATE yt_experiment_variants
            SET status = CASE
              WHEN variant_id = 'B' THEN 'failed'
              ELSE status
            END,
            updated_at = CURRENT_TIMESTAMP(3)
            WHERE experiment_id = ?;
          "#,
        )
        .bind(exp_id)
        .execute(pool)
        .await;

        return json_response(
          StatusCode::BAD_GATEWAY,
          serde_json::json!({"ok": false, "error": "apply_failed", "message": err, "experiment_id": format!("exp_{exp_id}"), "channel_id": channel_id}),
        );
      }
    }
  }

  json_response(
    StatusCode::METHOD_NOT_ALLOWED,
    serde_json::json!({"ok": false, "error": "method_not_allowed"}),
  )
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let action = get_query_param(req.uri(), "action").unwrap_or_default();

  let result = match action.as_str() {
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
    "app_config" => {
      let method = req.method().clone();
      let headers = req.headers().clone();
      let uri = req.uri().clone();
      let body = if method == Method::POST {
        Some(req.into_body().collect().await?.to_bytes())
      } else {
        None
      };
      handle_app_config(&method, &headers, &uri, body).await
    }
    "content_owner_discover" => {
      let method = req.method().clone();
      let headers = req.headers().clone();
      let bytes = req.into_body().collect().await?.to_bytes();
      handle_content_owner_discover(&method, &headers, bytes).await
    }
    "set_active_channel" => {
      let method = req.method().clone();
      let headers = req.headers().clone();
      let bytes = req.into_body().collect().await?.to_bytes();
      handle_set_active_channel(&method, &headers, bytes).await
    }
    "youtube_metrics_daily" => handle_youtube_metrics_daily(req.method(), req.headers(), req.uri()).await,
    "youtube_sponsor_quote_defaults" => {
      let method = req.method().clone();
      let headers = req.headers().clone();
      let uri = req.uri().clone();
      handle_youtube_sponsor_quote_defaults(&method, &headers, &uri).await
    }
    "youtube_sponsor_quote" => {
      let method = req.method().clone();
      let headers = req.headers().clone();
      let bytes = req.into_body().collect().await?.to_bytes();
      handle_youtube_sponsor_quote(&method, &headers, bytes).await
    }
    "youtube_uploads_list" => handle_youtube_uploads_list(req.method(), req.headers(), req.uri()).await,
    "youtube_upload_csv" => {
      let method = req.method().clone();
      let headers = req.headers().clone();
      let bytes = req.into_body().collect().await?.to_bytes();
      handle_youtube_upload_csv(&method, &headers, bytes).await
    }
    "youtube_alerts" => {
      let method = req.method().clone();
      let headers = req.headers().clone();
      let uri = req.uri().clone();
      let body = if method == Method::POST {
        Some(req.into_body().collect().await?.to_bytes())
      } else {
        None
      };
      handle_youtube_alerts(&method, &headers, &uri, body).await
    }
    "youtube_experiments" => {
      let method = req.method().clone();
      let headers = req.headers().clone();
      let uri = req.uri().clone();
      let body = if method == Method::POST {
        Some(req.into_body().collect().await?.to_bytes())
      } else {
        None
      };
      handle_youtube_experiments(&method, &headers, &uri, body).await
    }
    "youtube_experiment_get" => handle_youtube_experiment_get(req.method(), req.headers(), req.uri()).await,
    "" => json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "action is required"}),
    ),
    _ => json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_found"}),
    ),
  };

  match result {
    Ok(resp) => Ok(resp),
    Err(err) => {
      let message = truncate_string(&err.to_string(), 2000);
      json_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::json!({"ok": false, "error": "internal_error", "action": action, "message": message}),
      )
    }
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
  async fn start_returns_not_configured_when_tidb_env_missing() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
    std::env::remove_var("TIDB_DATABASE_URL");
    std::env::remove_var("DATABASE_URL");

    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer secret".parse().unwrap());
    headers.insert("content-type", "application/json".parse().unwrap());

    let body = Bytes::from(r#"{"tenant_id":"t1","state":"state123"}"#);
    let response = handle_start(&Method::POST, &headers, body).await.unwrap();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
  }

  #[tokio::test]
  async fn status_returns_unauthorized_when_missing_internal_token() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
    let headers = HeaderMap::new();
    let uri: Uri = "/api/oauth/youtube/status?tenant_id=t1".parse().unwrap();
    let response = handle_status(&Method::GET, &headers, &uri).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  }

  #[test]
  fn parse_csv_metrics_supports_minimal_schema() {
    let csv = "date,video_id,views,impressions,revenue_usd\n2026-02-01,vid1,100,1000,12.34\n";
    let rows = parse_csv_metrics(csv).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].dt.to_string(), "2026-02-01");
    assert_eq!(rows[0].video_id, "vid1");
    assert_eq!(rows[0].views, 100);
    assert_eq!(rows[0].impressions, 1000);
    assert!((rows[0].estimated_revenue_usd - 12.34).abs() < 1e-6);
  }
}

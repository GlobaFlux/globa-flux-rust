use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode, Uri};
use serde::Deserialize;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use chrono::{Duration, Utc};

use globa_flux_rust::db::{
  fetch_youtube_channel_id,
  fetch_youtube_connection_tokens,
  fetch_youtube_content_owner_id,
  fetch_youtube_oauth_app_config,
  get_pool,
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
  fetch_video_daily_metrics, youtube_analytics_error_to_vercel_error,
};
use globa_flux_rust::providers::youtube_api::fetch_my_channel_id;
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
  let app = fetch_youtube_oauth_app_config(pool, &parsed.tenant_id).await?;
  let Some(app) = app else {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing YouTube OAuth app config for tenant"}),
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
  let app = fetch_youtube_oauth_app_config(pool, &parsed.tenant_id).await?;
  let Some(app) = app else {
    return json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing YouTube OAuth app config for tenant"}),
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
      let app = fetch_youtube_oauth_app_config(pool, &parsed.tenant_id).await?;
      let Some(app) = app else {
        return json_response(
          StatusCode::NOT_FOUND,
          serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing YouTube OAuth app config for tenant"}),
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
}

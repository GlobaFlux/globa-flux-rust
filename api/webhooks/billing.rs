use bytes::Bytes;
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode};
use serde::Deserialize;
use serde_json::Value;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::db::{fetch_subscription, get_pool, upsert_subscription};

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

fn normalize_subscription_status(topic: &str, raw: Option<&str>) -> Option<String> {
  let s = raw.unwrap_or("").trim().to_lowercase();
  if s.is_empty() {
    return None;
  }

  let normalized = match s.as_str() {
    "active" | "activated" => "active",
    "trialing" | "trial" => "trialing",
    "past_due" | "past-due" | "past due" | "paused" => "past_due",
    "cancelled" | "canceled" | "cancelled_by_user" | "expired" => "canceled",
    other => {
      // Some Shopify topics provide status-like values that are domain-specific; keep them for audit.
      if topic.contains("subscription") {
        other
      } else {
        return None;
      }
    }
  };

  Some(normalized.to_string())
}

fn parse_shopify_customer_gid(id: &Value) -> Option<String> {
  if let Some(s) = id.as_str() {
    if s.starts_with("gid://shopify/Customer/") {
      return Some(s.to_string());
    }
    if let Ok(n) = s.parse::<i64>() {
      return Some(format!("gid://shopify/Customer/{n}"));
    }
  }
  if let Some(n) = id.as_i64() {
    return Some(format!("gid://shopify/Customer/{n}"));
  }
  None
}

fn extract_tenant_id(payload: &Value) -> Option<String> {
  if let Some(customer) = payload.get("customer") {
    if let Some(gid) = customer.get("admin_graphql_api_id").and_then(|v| v.as_str()) {
      if gid.starts_with("gid://shopify/Customer/") {
        return Some(gid.to_string());
      }
    }
    if let Some(id) = customer.get("id") {
      if let Some(gid) = parse_shopify_customer_gid(id) {
        return Some(gid);
      }
    }
  }

  if let Some(id) = payload.get("customer_id") {
    return parse_shopify_customer_gid(id);
  }

  None
}

fn extract_provider_customer_id(payload: &Value) -> Option<String> {
  if let Some(customer) = payload.get("customer") {
    if let Some(id) = customer.get("id").and_then(|v| v.as_i64()) {
      return Some(id.to_string());
    }
    if let Some(id) = customer.get("id").and_then(|v| v.as_str()) {
      return Some(id.to_string());
    }
  }
  if let Some(id) = payload.get("customer_id").and_then(|v| v.as_i64()) {
    return Some(id.to_string());
  }
  payload
    .get("customer_id")
    .and_then(|v| v.as_str())
    .map(|s| s.to_string())
}

fn extract_provider_subscription_id(payload: &Value) -> Option<String> {
  if let Some(gid) = payload
    .get("admin_graphql_api_id")
    .and_then(|v| v.as_str())
  {
    return Some(gid.to_string());
  }
  payload
    .get("id")
    .and_then(|v| v.as_i64())
    .map(|n| n.to_string())
    .or_else(|| payload.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()))
}

fn parse_shopify_time(value: &Value) -> Option<DateTime<Utc>> {
  let s = value.as_str()?.trim();
  if s.is_empty() {
    return None;
  }

  if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
    return Some(dt.with_timezone(&Utc));
  }

  if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
    return Utc
      .with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
      .single();
  }

  None
}

#[derive(Deserialize)]
struct BillingWebhookIngestRequest {
  provider_event_id: String,
  topic: String,
  #[serde(default)]
  shop_domain: Option<String>,
  raw_body: String,
}

async fn handle_billing(method: &Method, headers: &HeaderMap, body: Bytes) -> Result<Response<ResponseBody>, Error> {
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

  let parsed: BillingWebhookIngestRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

  if parsed.provider_event_id.is_empty() || parsed.topic.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "provider_event_id and topic are required"}),
    );
  }

  let pool = get_pool().await?;

  let inserted = sqlx::query(
    r#"
      INSERT INTO billing_events (provider, provider_event_id, topic, shop_domain, raw_payload)
      VALUES ('shopify', ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE topic=VALUES(topic), shop_domain=VALUES(shop_domain);
    "#,
  )
  .bind(&parsed.provider_event_id)
  .bind(&parsed.topic)
  .bind(&parsed.shop_domain)
  .bind(&parsed.raw_body)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?
  .rows_affected()
    == 1;

  let mut subscription_updated = false;
  let mut subscription_error: Option<String> = None;

  if let Ok(payload) = serde_json::from_str::<Value>(&parsed.raw_body) {
    let tenant_id = extract_tenant_id(&payload);
    let provider_customer_id = extract_provider_customer_id(&payload);
    let provider_subscription_id = extract_provider_subscription_id(&payload);
    let status = normalize_subscription_status(&parsed.topic, payload.get("status").and_then(|v| v.as_str()));

    // Common Shopify field names for subscription contract/recurring purchases:
    let current_period_end = payload
      .get("current_period_end")
      .and_then(parse_shopify_time)
      .or_else(|| payload.get("next_billing_date").and_then(parse_shopify_time));

    if let (Some(tenant_id), Some(status)) = (tenant_id, status) {
      let res = upsert_subscription(
        pool,
        &tenant_id,
        &status,
        provider_customer_id.as_deref(),
        provider_subscription_id.as_deref(),
        current_period_end,
      )
      .await;

      match res {
        Ok(()) => subscription_updated = true,
        Err(e) => subscription_error = Some(e.to_string()),
      }
    }
  }

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "inserted": inserted, "subscription_updated": subscription_updated, "subscription_error": subscription_error}),
  )
}

async fn handle_subscription_status(
  method: &Method,
  headers: &HeaderMap,
  uri: &hyper::Uri,
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

async fn handle_router(
  method: &Method,
  headers: &HeaderMap,
  uri: &hyper::Uri,
  body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
  let action = query_param(uri.query(), "action").unwrap_or_default();
  match action.as_str() {
    "subscription_status" => handle_subscription_status(method, headers, uri).await,
    "" | "webhook" => handle_billing(method, headers, body).await,
    _ => json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_found"}),
    ),
  }
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let method = req.method().clone();
  let headers = req.headers().clone();
  let uri = req.uri().clone();
  let bytes = req.into_body().collect().await?.to_bytes();
  handle_router(&method, &headers, &uri, bytes).await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  run(service_fn(handler)).await
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn subscription_status_returns_unauthorized_when_missing_internal_token() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");

    let headers = HeaderMap::new();
    let uri: hyper::Uri = "/api/webhooks/billing?action=subscription_status&tenant_id=t1"
      .parse()
      .unwrap();
    let response = handle_subscription_status(&Method::GET, &headers, &uri)
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  }

  #[tokio::test]
  async fn returns_unauthorized_when_missing_internal_token() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");

    let headers = HeaderMap::new();
    let response = handle_billing(&Method::POST, &headers, Bytes::new())
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

    let body = Bytes::from(
      r#"{"provider_event_id":"w1","topic":"app_subscriptions/update","shop_domain":"example.myshopify.com","raw_body":"{}"}"#,
    );
    let response = handle_billing(&Method::POST, &headers, body).await.unwrap();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
  }
}

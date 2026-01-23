use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode};
use serde::Deserialize;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::db::{fetch_tenant_gemini_model, get_pool, upsert_tenant_gemini_model};

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

fn normalize_model(model: &str) -> String {
  model.trim().trim_start_matches("models/").to_string()
}

#[derive(Deserialize)]
struct UpdateLlmSettingsRequest {
  tenant_id: String,
  gemini_model: String,
  #[serde(default)]
  updated_by: Option<String>,
}

async fn handle_llm_settings(
  method: &Method,
  headers: &HeaderMap,
  uri: &hyper::Uri,
  body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
  let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
  let provided = bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

  if expected.is_empty() || provided != expected {
    return json_response(
      StatusCode::UNAUTHORIZED,
      serde_json::json!({"ok": false, "error": "unauthorized"}),
    );
  }

  match *method {
    Method::GET => {
      let tenant_id = query_param(uri.query(), "tenant_id").unwrap_or_default();
      if tenant_id.is_empty() {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
        );
      }

      if tenant_id != GLOBAL_TENANT_ID {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id must be global"}),
        );
      }

      if !has_tidb_url() {
        return json_response(
          StatusCode::NOT_IMPLEMENTED,
          serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
        );
      }

      let pool = get_pool().await?;
      let db_model = fetch_tenant_gemini_model(pool, &tenant_id).await?;

      json_response(
        StatusCode::OK,
        serde_json::json!({
          "ok": true,
          "tenant_id": tenant_id,
          "gemini_model": db_model
        }),
      )
    }
    Method::PUT | Method::POST => {
      let parsed: UpdateLlmSettingsRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
        Box::new(std::io::Error::other(format!("invalid json body: {e}")))
      })?;

      if parsed.tenant_id.trim().is_empty() {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id is required"}),
        );
      }

      if parsed.tenant_id.trim() != GLOBAL_TENANT_ID {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "tenant_id must be global"}),
        );
      }

      if !has_tidb_url() {
        return json_response(
          StatusCode::NOT_IMPLEMENTED,
          serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
        );
      }

      let model = normalize_model(&parsed.gemini_model);
      if model.is_empty() {
        return json_response(
          StatusCode::BAD_REQUEST,
          serde_json::json!({"ok": false, "error": "bad_request", "message": "gemini_model is required"}),
        );
      }

      let updated_by = parsed.updated_by.unwrap_or_else(|| "system".to_string());

      let pool = get_pool().await?;
      upsert_tenant_gemini_model(pool, parsed.tenant_id.trim(), &model, &updated_by).await?;

      json_response(
        StatusCode::OK,
        serde_json::json!({"ok": true, "tenant_id": parsed.tenant_id.trim(), "gemini_model": model}),
      )
    }
    _ => json_response(
      StatusCode::METHOD_NOT_ALLOWED,
      serde_json::json!({"ok": false, "error": "method_not_allowed"}),
    ),
  }
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let method = req.method().clone();
  let headers = req.headers().clone();
  let uri = req.uri().clone();
  let bytes = req.into_body().collect().await?.to_bytes();
  handle_llm_settings(&method, &headers, &uri, bytes).await
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
    let uri: hyper::Uri = "/api/tenants/llm_settings?tenant_id=t1".parse().unwrap();
    let response = handle_llm_settings(&Method::GET, &headers, &uri, Bytes::new())
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  }

  #[tokio::test]
  async fn rejects_non_global_tenant_id_even_when_tidb_missing() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
    std::env::remove_var("TIDB_DATABASE_URL");
    std::env::remove_var("DATABASE_URL");

    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer secret".parse().unwrap());
    let uri: hyper::Uri = "/api/tenants/llm_settings?tenant_id=foo".parse().unwrap();
    let response = handle_llm_settings(&Method::GET, &headers, &uri, Bytes::new())
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
  }
}

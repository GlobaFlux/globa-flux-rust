use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode};
use serde::Deserialize;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::providers::youtube::{build_authorize_url, youtube_oauth_client_from_env};

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
struct StartRequest {
  state: String,
}

async fn handle_start(method: &Method, headers: &HeaderMap, _body: Bytes) -> Result<Response<ResponseBody>, Error> {
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

  let parsed: StartRequest = serde_json::from_slice(&_body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

  if parsed.state.is_empty() {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "state is required"}),
    );
  }

  let (client, _redirect) = youtube_oauth_client_from_env()?;
  let (authorize_url, state) = build_authorize_url(&client, Some(parsed.state));

  json_response(
    StatusCode::OK,
    serde_json::json!({"ok": true, "authorize_url": authorize_url, "state": state}),
  )
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let method = req.method().clone();
  let headers = req.headers().clone();
  let bytes = req.into_body().collect().await?.to_bytes();
  handle_start(&method, &headers, bytes).await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  run(service_fn(handler)).await
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn returns_authorize_url_with_provided_state() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
    std::env::set_var("YOUTUBE_CLIENT_ID", "id");
    std::env::set_var("YOUTUBE_CLIENT_SECRET", "secret2");
    std::env::set_var("YOUTUBE_REDIRECT_URI", "https://example.com/cb");

    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer secret".parse().unwrap());
    headers.insert("content-type", "application/json".parse().unwrap());

    let body = Bytes::from(r#"{"state":"state123"}"#);
    let response = handle_start(&Method::POST, &headers, body).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed.get("ok").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
      parsed.get("state").and_then(|v| v.as_str()),
      Some("state123")
    );
    let url = parsed.get("authorize_url").and_then(|v| v.as_str()).unwrap();
    assert!(url.contains("accounts.google.com/o/oauth2/v2/auth"));
    assert!(url.contains("state=state123"));
  }

  #[tokio::test]
  async fn returns_unauthorized_when_missing_internal_token() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");

    let headers = HeaderMap::new();
    let response = handle_start(&Method::POST, &headers, Bytes::new())
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  }
}

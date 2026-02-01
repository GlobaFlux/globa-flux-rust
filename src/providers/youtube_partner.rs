use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::header::{ACCEPT, AUTHORIZATION};
use hyper::{Method, Request, StatusCode};
use serde_json::Value;
use vercel_runtime::Error;

pub fn build_content_owners_list_url(base_url: &str) -> String {
  let base = base_url.trim_end_matches('/');
  format!("{base}/contentOwners?fetchMine=true")
}

fn parse_content_owner_id(json: &Value) -> Option<String> {
  json
    .get("items")
    .and_then(|v| v.as_array())
    .and_then(|items| {
      items.iter().find_map(|item| {
        item
          .get("id")
          .and_then(|v| v.as_str())
          .map(|s| s.to_string())
      })
    })
}

pub async fn fetch_my_content_owner_id_with_base_url(access_token: &str, base_url: &str) -> Result<Option<String>, Error> {
  let connector = hyper_rustls::HttpsConnectorBuilder::new()
    .with_native_roots()
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?
    .https_or_http()
    .enable_http1()
    .build();

  let client =
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build(connector);

  let url = build_content_owners_list_url(base_url);
  let req = Request::builder()
    .method(Method::GET)
    .uri(url)
    .header(AUTHORIZATION, format!("Bearer {}", access_token))
    .header(ACCEPT, "application/json")
    .body(Empty::<Bytes>::new())
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  let resp = client
    .request(req)
    .await
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  let status = resp.status();
  let body_bytes = resp
    .into_body()
    .collect()
    .await
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?
    .to_bytes();

  if status == StatusCode::FORBIDDEN {
    return Ok(None);
  }

  if !status.is_success() {
    let snippet = String::from_utf8_lossy(&body_bytes);
    return Err(Box::new(std::io::Error::other(format!(
      "YouTube Partner API error (status {}): {}",
      status.as_u16(),
      snippet.chars().take(200).collect::<String>()
    ))));
  }

  let json: Value = serde_json::from_slice(&body_bytes)
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  Ok(parse_content_owner_id(&json))
}

pub async fn fetch_my_content_owner_id(access_token: &str) -> Result<Option<String>, Error> {
  fetch_my_content_owner_id_with_base_url(access_token, "https://www.googleapis.com/youtube/partner/v1/").await
}

#[cfg(test)]
mod tests {
  use super::*;
  use bytes::Bytes;
  use http_body_util::Full;
  use hyper::body::Incoming;
  use hyper::header::AUTHORIZATION;
  use hyper::server::conn::http1;
  use hyper::service::service_fn;
  use hyper::{Request, Response, StatusCode};
  use hyper_util::rt::TokioIo;
  use tokio::net::TcpListener;

  async fn serve_one(listener: TcpListener) {
    let (stream, _) = listener.accept().await.unwrap();
    let io = TokioIo::new(stream);
    http1::Builder::new()
      .serve_connection(
        io,
        service_fn(|req: Request<Incoming>| async move {
          let auth = req
            .headers()
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
          if auth != "Bearer token123" {
            return Ok::<_, hyper::Error>(
              Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body(Full::new(Bytes::from_static(b"unauthorized")))
                .unwrap(),
            );
          }

          let body = r#"{"kind":"youtubePartner#contentOwnerListResponse","items":[{"id":"CMS123"}]}"#;
          Ok::<_, hyper::Error>(
            Response::builder()
              .status(StatusCode::OK)
              .header("content-type", "application/json")
              .body(Full::new(Bytes::from(body)))
              .unwrap(),
          )
        }),
      )
      .await
      .unwrap();
  }

  #[tokio::test]
  async fn fetches_content_owner_id_against_mock_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}/", addr);

    let task = tokio::spawn(serve_one(listener));

    let owner_id = fetch_my_content_owner_id_with_base_url("token123", &base_url)
      .await
      .unwrap();
    assert_eq!(owner_id, Some("CMS123".to_string()));

    task.await.unwrap();
  }
}

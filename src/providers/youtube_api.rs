use vercel_runtime::Error;

pub async fn fetch_my_channel_id_with_base_url(access_token: &str, base_url: &str) -> Result<String, Error> {
  let connector = hyper_rustls::HttpsConnectorBuilder::new()
    .with_native_roots()
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?
    .https_or_http()
    .enable_http1()
    .build();

  let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build(connector);

  let mut hub = google_youtube3::YouTube::new(client, access_token.to_string());
  hub.base_url(base_url.to_string());
  hub.root_url(base_url.to_string());

  let (_, response) = hub
    .channels()
    .list(&vec!["id".into()])
    .mine(true)
    .doit()
    .await
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  let channel_id = response
    .items
    .unwrap_or_default()
    .into_iter()
    .find_map(|c| c.id)
    .ok_or_else(|| Box::new(std::io::Error::other("No channel_id found for this token")) as Error)?;

  Ok(channel_id)
}

pub async fn fetch_my_channel_id(access_token: &str) -> Result<String, Error> {
  fetch_my_channel_id_with_base_url(access_token, "https://youtube.googleapis.com/").await
}

#[cfg(test)]
mod tests {
  use super::*;
  use bytes::Bytes;
  use http_body_util::Full;
  use hyper::body::Incoming;
  use hyper::header::AUTHORIZATION;
  use hyper::service::service_fn;
  use hyper::{Request, Response, StatusCode};
  use hyper_util::rt::TokioIo;
  use hyper::server::conn::http1;
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

          let body = r#"{"kind":"youtube#channelListResponse","items":[{"id":"UC123"}]}"#;
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
  async fn fetches_channel_id_via_sdk_against_mock_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}/", addr);

    let task = tokio::spawn(serve_one(listener));

    let channel_id = fetch_my_channel_id_with_base_url("token123", &base_url).await.unwrap();
    assert_eq!(channel_id, "UC123");

    task.await.unwrap();
  }
}

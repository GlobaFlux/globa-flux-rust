use vercel_runtime::Error;

#[derive(Debug, Clone, serde::Serialize)]
pub struct MyChannelSummary {
  pub channel_id: String,
  pub title: String,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub thumbnail_url: Option<String>,
}

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

pub async fn list_my_channels_with_base_url(access_token: &str, base_url: &str) -> Result<Vec<MyChannelSummary>, Error> {
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
    .list(&vec!["id".into(), "snippet".into()])
    .mine(true)
    .doit()
    .await
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  let items = response.items.unwrap_or_default();
  let mut out: Vec<MyChannelSummary> = Vec::with_capacity(items.len());
  for c in items.into_iter() {
    let Some(channel_id) = c.id else { continue };
    let title = c
      .snippet
      .as_ref()
      .and_then(|s| s.title.as_ref())
      .map(|v| v.trim().to_string())
      .filter(|v| !v.is_empty())
      .unwrap_or_else(|| "Untitled channel".to_string());

    let thumbnail_url = c
      .snippet
      .as_ref()
      .and_then(|s| s.thumbnails.as_ref())
      .and_then(|t| t.default.as_ref().and_then(|x| x.url.as_ref()))
      .or_else(|| c.snippet.as_ref().and_then(|s| s.thumbnails.as_ref()).and_then(|t| t.high.as_ref().and_then(|x| x.url.as_ref())))
      .map(|v| v.trim().to_string())
      .filter(|v| !v.is_empty());

    out.push(MyChannelSummary {
      channel_id,
      title,
      thumbnail_url,
    });
  }

  Ok(out)
}

pub async fn list_my_channels(access_token: &str) -> Result<Vec<MyChannelSummary>, Error> {
  list_my_channels_with_base_url(access_token, "https://youtube.googleapis.com/").await
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

  #[tokio::test]
  async fn lists_channels_via_sdk_against_mock_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}/", addr);

    let task = tokio::spawn(async move {
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

            let body = r#"{
              "kind":"youtube#channelListResponse",
              "items":[
                {"id":"UC1","snippet":{"title":"Ch 1","thumbnails":{"default":{"url":"https://example.com/a.jpg"}}}},
                {"id":"UC2","snippet":{"title":"Ch 2","thumbnails":{"high":{"url":"https://example.com/b.jpg"}}}}
              ]
            }"#;
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
    });

    let channels = list_my_channels_with_base_url("token123", &base_url).await.unwrap();
    assert_eq!(channels.len(), 2);
    assert_eq!(channels[0].channel_id, "UC1");
    assert_eq!(channels[0].title, "Ch 1");
    assert_eq!(channels[0].thumbnail_url.as_deref(), Some("https://example.com/a.jpg"));
    assert_eq!(channels[1].channel_id, "UC2");
    assert_eq!(channels[1].title, "Ch 2");
    assert_eq!(channels[1].thumbnail_url.as_deref(), Some("https://example.com/b.jpg"));

    task.await.unwrap();
  }
}

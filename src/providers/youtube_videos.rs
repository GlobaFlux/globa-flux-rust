use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use hyper::{Method, Request, StatusCode};
use serde_json::Value;
use std::net::IpAddr;

#[derive(Debug, Clone)]
pub struct YoutubeVideoError {
  pub status: Option<u16>,
  pub message: String,
}

impl std::fmt::Display for YoutubeVideoError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self.status {
      Some(code) => write!(f, "youtube video error (status={}): {}", code, self.message),
      None => write!(f, "youtube video error: {}", self.message),
    }
  }
}

impl std::error::Error for YoutubeVideoError {}

#[derive(Debug, Clone)]
pub struct VideoSnapshot {
  pub title: String,
  pub description: String,
  pub category_id: Option<String>,
  pub tags: Option<Vec<String>>,
  pub privacy_status: Option<String>,
  pub publish_at: Option<String>,
  pub thumbnail_url: Option<String>,
}

fn best_thumbnail_url(snippet: &Value) -> Option<String> {
  let thumbs = snippet.get("thumbnails")?;
  for key in ["maxres", "standard", "high", "medium", "default"] {
    if let Some(url) = thumbs.get(key).and_then(|v| v.get("url")).and_then(|v| v.as_str()) {
      let url = url.trim();
      if !url.is_empty() {
        return Some(url.to_string());
      }
    }
  }
  None
}

async fn fetch_json(access_token: &str, url: &str) -> Result<Value, YoutubeVideoError> {
  let connector = hyper_rustls::HttpsConnectorBuilder::new()
    .with_native_roots()
    .map_err(|e| YoutubeVideoError {
      status: None,
      message: e.to_string(),
    })?
    .https_or_http()
    .enable_http1()
    .build();

  let client =
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build(connector);

  let req = Request::builder()
    .method(Method::GET)
    .uri(url)
    .header(AUTHORIZATION, format!("Bearer {}", access_token))
    .header(ACCEPT, "application/json")
    .body(Empty::<Bytes>::new())
    .map_err(|e| YoutubeVideoError {
      status: None,
      message: e.to_string(),
    })?;

  let resp = client.request(req).await.map_err(|e| YoutubeVideoError {
    status: None,
    message: e.to_string(),
  })?;

  let status = resp.status();
  let body_bytes = resp
    .into_body()
    .collect()
    .await
    .map_err(|e| YoutubeVideoError {
      status: Some(status.as_u16()),
      message: e.to_string(),
    })?
    .to_bytes();

  if status != StatusCode::OK {
    let msg = String::from_utf8_lossy(&body_bytes).to_string();
    return Err(YoutubeVideoError {
      status: Some(status.as_u16()),
      message: msg,
    });
  }

  serde_json::from_slice::<Value>(&body_bytes).map_err(|e| YoutubeVideoError {
    status: Some(status.as_u16()),
    message: format!("invalid json response: {e}"),
  })
}

async fn put_json(access_token: &str, url: &str, body: &Value) -> Result<Value, YoutubeVideoError> {
  let connector = hyper_rustls::HttpsConnectorBuilder::new()
    .with_native_roots()
    .map_err(|e| YoutubeVideoError {
      status: None,
      message: e.to_string(),
    })?
    .https_or_http()
    .enable_http1()
    .build();

  let client =
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build(connector);

  let body_str = serde_json::to_string(body).map_err(|e| YoutubeVideoError {
    status: None,
    message: e.to_string(),
  })?;

  let req = Request::builder()
    .method(Method::PUT)
    .uri(url)
    .header(AUTHORIZATION, format!("Bearer {}", access_token))
    .header(ACCEPT, "application/json")
    .header(CONTENT_TYPE, "application/json")
    .body(Full::new(Bytes::from(body_str)))
    .map_err(|e| YoutubeVideoError {
      status: None,
      message: e.to_string(),
    })?;

  let resp = client.request(req).await.map_err(|e| YoutubeVideoError {
    status: None,
    message: e.to_string(),
  })?;

  let status = resp.status();
  let body_bytes = resp
    .into_body()
    .collect()
    .await
    .map_err(|e| YoutubeVideoError {
      status: Some(status.as_u16()),
      message: e.to_string(),
    })?
    .to_bytes();

  if status != StatusCode::OK {
    let msg = String::from_utf8_lossy(&body_bytes).to_string();
    return Err(YoutubeVideoError {
      status: Some(status.as_u16()),
      message: msg,
    });
  }

  serde_json::from_slice::<Value>(&body_bytes).map_err(|e| YoutubeVideoError {
    status: Some(status.as_u16()),
    message: format!("invalid json response: {e}"),
  })
}

pub async fn fetch_video_snapshot(access_token: &str, video_id: &str) -> Result<VideoSnapshot, YoutubeVideoError> {
  let video_id = video_id.trim();
  if video_id.is_empty() {
    return Err(YoutubeVideoError {
      status: None,
      message: "missing video_id".to_string(),
    });
  }

  let url = format!(
    "https://youtube.googleapis.com/youtube/v3/videos?part=snippet,status&id={}",
    video_id
  );
  let json = fetch_json(access_token, &url).await?;

  let item = json
    .get("items")
    .and_then(|v| v.as_array())
    .and_then(|items| items.first())
    .ok_or_else(|| YoutubeVideoError {
      status: Some(404),
      message: "video not found".to_string(),
    })?;

  let snippet = item.get("snippet").cloned().unwrap_or_else(|| serde_json::json!({}));
  let status = item.get("status").cloned().unwrap_or_else(|| serde_json::json!({}));

  let title = snippet
    .get("title")
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .to_string();
  let description = snippet
    .get("description")
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .to_string();
  let category_id = snippet
    .get("categoryId")
    .and_then(|v| v.as_str())
    .map(|v| v.to_string());
  let tags = snippet.get("tags").and_then(|v| {
    v.as_array().map(|arr| {
      arr
        .iter()
        .filter_map(|t| t.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>()
    })
  });

  let privacy_status = status
    .get("privacyStatus")
    .and_then(|v| v.as_str())
    .map(|v| v.to_string());
  let publish_at = status
    .get("publishAt")
    .and_then(|v| v.as_str())
    .map(|v| v.to_string());

  Ok(VideoSnapshot {
    title,
    description,
    category_id,
    tags,
    privacy_status,
    publish_at,
    thumbnail_url: best_thumbnail_url(&snippet),
  })
}

pub async fn update_video_title(access_token: &str, video_id: &str, new_title: &str) -> Result<(), YoutubeVideoError> {
  let new_title = new_title.trim();
  if new_title.is_empty() {
    return Err(YoutubeVideoError {
      status: None,
      message: "missing new_title".to_string(),
    });
  }

  let snap = fetch_video_snapshot(access_token, video_id).await?;
  let Some(category_id) = snap.category_id.clone() else {
    return Err(YoutubeVideoError {
      status: None,
      message: "missing categoryId for video snippet update".to_string(),
    });
  };

  let mut snippet = serde_json::json!({
    "title": new_title,
    "description": snap.description,
    "categoryId": category_id,
  });
  if let Some(tags) = snap.tags {
    snippet
      .as_object_mut()
      .unwrap()
      .insert("tags".to_string(), serde_json::json!(tags));
  }

  let body = serde_json::json!({
    "id": video_id,
    "snippet": snippet,
  });

  let url = "https://youtube.googleapis.com/youtube/v3/videos?part=snippet";
  let _ = put_json(access_token, url, &body).await?;
  Ok(())
}

pub async fn update_video_publish_at(
  access_token: &str,
  video_id: &str,
  new_publish_at_rfc3339: &str,
) -> Result<(), YoutubeVideoError> {
  let new_publish_at_rfc3339 = new_publish_at_rfc3339.trim();
  if new_publish_at_rfc3339.is_empty() {
    return Err(YoutubeVideoError {
      status: None,
      message: "missing publish_at".to_string(),
    });
  }

  let snap = fetch_video_snapshot(access_token, video_id).await?;
  let Some(privacy_status) = snap.privacy_status else {
    return Err(YoutubeVideoError {
      status: None,
      message: "missing privacyStatus for video status update".to_string(),
    });
  };

  if privacy_status != "private" {
    return Err(YoutubeVideoError {
      status: Some(400),
      message: format!(
        "publish_time experiments only support scheduled videos (privacyStatus=private), got {privacy_status}"
      ),
    });
  }

  let body = serde_json::json!({
    "id": video_id,
    "status": {
      "privacyStatus": privacy_status,
      "publishAt": new_publish_at_rfc3339,
    }
  });

  let url = "https://youtube.googleapis.com/youtube/v3/videos?part=status";
  let _ = put_json(access_token, url, &body).await?;
  Ok(())
}

fn host_is_blocked(host: &str) -> bool {
  let host = host.trim().trim_matches('.').to_lowercase();
  let host = host
    .strip_prefix('[')
    .and_then(|v| v.strip_suffix(']'))
    .unwrap_or(host.as_str())
    .to_string();
  if host.is_empty() {
    return true;
  }
  if host == "localhost" || host.ends_with(".localhost") || host.ends_with(".local") {
    return true;
  }

  if let Ok(ip) = host.parse::<IpAddr>() {
    return ip_is_private_or_reserved(ip);
  }

  false
}

fn ip_is_private_or_reserved(ip: IpAddr) -> bool {
  match ip {
    IpAddr::V4(v4) => {
      if v4.is_private() || v4.is_loopback() || v4.is_link_local() || v4.is_multicast() || v4.is_unspecified() {
        return true;
      }
      let octets = v4.octets();
      // 100.64.0.0/10 (carrier-grade NAT)
      if octets[0] == 100 && (octets[1] & 0b1100_0000) == 0b0100_0000 {
        return true;
      }
      // 169.254.0.0/16 (link local / metadata)
      if octets[0] == 169 && octets[1] == 254 {
        return true;
      }
      // 0.0.0.0/8
      if octets[0] == 0 {
        return true;
      }
      // 224.0.0.0/4 (multicast already covered)
      // 240.0.0.0/4 (reserved)
      if octets[0] >= 240 {
        return true;
      }
      false
    }
    IpAddr::V6(v6) => {
      if v6.is_loopback()
        || v6.is_unspecified()
        || v6.is_multicast()
        || v6.is_unicast_link_local()
        || v6.is_unique_local()
      {
        return true;
      }
      // IPv4-mapped
      if let Some(v4) = v6.to_ipv4() {
        return ip_is_private_or_reserved(IpAddr::V4(v4));
      }
      false
    }
  }
}

async fn download_image_bytes(url: &str, max_bytes: usize) -> Result<(Bytes, String), YoutubeVideoError> {
  let uri = url.parse::<hyper::Uri>().map_err(|e| YoutubeVideoError {
    status: None,
    message: format!("invalid thumbnail url: {e}"),
  })?;

  if uri.scheme_str() != Some("https") {
    return Err(YoutubeVideoError {
      status: Some(400),
      message: "thumbnail_url must be https".to_string(),
    });
  }

  let host = uri.host().unwrap_or("");
  if host_is_blocked(host) {
    return Err(YoutubeVideoError {
      status: Some(400),
      message: "thumbnail_url host is not allowed".to_string(),
    });
  }

  let connector = hyper_rustls::HttpsConnectorBuilder::new()
    .with_native_roots()
    .map_err(|e| YoutubeVideoError {
      status: None,
      message: e.to_string(),
    })?
    .https_or_http()
    .enable_http1()
    .build();

  let client =
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build(connector);

  let req = Request::builder()
    .method(Method::GET)
    .uri(url)
    .header(ACCEPT, "image/*")
    .body(Empty::<Bytes>::new())
    .map_err(|e| YoutubeVideoError {
      status: None,
      message: e.to_string(),
    })?;

  let resp = client.request(req).await.map_err(|e| YoutubeVideoError {
    status: None,
    message: e.to_string(),
  })?;

  let status = resp.status();
  let content_type = resp
    .headers()
    .get(CONTENT_TYPE)
    .and_then(|v| v.to_str().ok())
    .map(|v| v.to_string())
    .unwrap_or_else(|| "image/jpeg".to_string());

  let body_bytes = resp
    .into_body()
    .collect()
    .await
    .map_err(|e| YoutubeVideoError {
      status: Some(status.as_u16()),
      message: e.to_string(),
    })?
    .to_bytes();

  if status != StatusCode::OK {
    let msg = String::from_utf8_lossy(&body_bytes).to_string();
    return Err(YoutubeVideoError {
      status: Some(status.as_u16()),
      message: msg,
    });
  }

  if body_bytes.len() > max_bytes {
    return Err(YoutubeVideoError {
      status: Some(413),
      message: format!("thumbnail too large ({} bytes)", body_bytes.len()),
    });
  }

  Ok((body_bytes, content_type))
}

pub async fn set_video_thumbnail_from_url(
  access_token: &str,
  video_id: &str,
  thumbnail_url: &str,
) -> Result<(), YoutubeVideoError> {
  const MAX_THUMBNAIL_BYTES: usize = 5 * 1024 * 1024;
  let (bytes, content_type) = download_image_bytes(thumbnail_url, MAX_THUMBNAIL_BYTES).await?;

  let connector = hyper_rustls::HttpsConnectorBuilder::new()
    .with_native_roots()
    .map_err(|e| YoutubeVideoError {
      status: None,
      message: e.to_string(),
    })?
    .https_or_http()
    .enable_http1()
    .build();

  let client =
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build(connector);

  let url = format!(
    "https://youtube.googleapis.com/upload/youtube/v3/thumbnails/set?videoId={}&uploadType=media",
    video_id
  );

  let req = Request::builder()
    .method(Method::POST)
    .uri(url)
    .header(AUTHORIZATION, format!("Bearer {}", access_token))
    .header(CONTENT_TYPE, content_type)
    .body(Full::new(bytes))
    .map_err(|e| YoutubeVideoError {
      status: None,
      message: e.to_string(),
    })?;

  let resp = client.request(req).await.map_err(|e| YoutubeVideoError {
    status: None,
    message: e.to_string(),
  })?;

  let status = resp.status();
  let body_bytes = resp
    .into_body()
    .collect()
    .await
    .map_err(|e| YoutubeVideoError {
      status: Some(status.as_u16()),
      message: e.to_string(),
    })?
    .to_bytes();

  if status != StatusCode::OK {
    let msg = String::from_utf8_lossy(&body_bytes).to_string();
    return Err(YoutubeVideoError {
      status: Some(status.as_u16()),
      message: msg,
    });
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::net::Ipv6Addr;

  #[test]
  fn host_is_blocked_rejects_private_hosts() {
    assert!(host_is_blocked("localhost"));
    assert!(host_is_blocked("127.0.0.1"));
    assert!(host_is_blocked("10.0.0.1"));
    assert!(host_is_blocked("169.254.169.254"));
    assert!(host_is_blocked("192.168.1.1"));
    assert!(host_is_blocked("172.16.0.1"));
    assert!(host_is_blocked("[::1]"));
    assert!(!host_is_blocked("images.example.com"));
  }

  #[test]
  fn ip_private_detection_handles_ipv4_mapped() {
    let v6 = Ipv6Addr::from_str_radix("00000000000000000000ffff0a000001", 16).unwrap();
    assert!(ip_is_private_or_reserved(IpAddr::V6(v6)));
  }

  trait FromStrRadixExt {
    fn from_str_radix(s: &str, radix: u32) -> Option<Ipv6Addr>;
  }

  impl FromStrRadixExt for Ipv6Addr {
    fn from_str_radix(s: &str, radix: u32) -> Option<Ipv6Addr> {
      if radix != 16 {
        return None;
      }
      if s.len() != 32 {
        return None;
      }
      let mut bytes = [0u8; 16];
      for i in 0..16 {
        let start = i * 2;
        let byte = u8::from_str_radix(&s[start..start + 2], 16).ok()?;
        bytes[i] = byte;
      }
      Some(Ipv6Addr::from(bytes))
    }
  }
}

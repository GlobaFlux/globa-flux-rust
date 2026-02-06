use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use hyper::{Method, Request};
use serde_json::Value;

#[derive(Debug)]
pub struct YoutubeReportingError {
  pub status: Option<u16>,
  pub message: String,
}

impl std::fmt::Display for YoutubeReportingError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if let Some(status) = self.status {
      write!(f, "YouTube Reporting error (status {status}): {}", self.message)
    } else {
      write!(f, "YouTube Reporting error: {}", self.message)
    }
  }
}

impl std::error::Error for YoutubeReportingError {}

#[derive(Debug, Clone)]
pub struct YoutubeReportingReportType {
  pub report_type_id: String,
  pub report_type_name: Option<String>,
  pub system_managed: bool,
}

#[derive(Debug, Clone)]
pub struct YoutubeReportingJob {
  pub job_id: String,
  pub report_type_id: Option<String>,
  pub name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct YoutubeReportingReport {
  pub report_id: String,
  pub download_url: Option<String>,
  pub start_time: Option<String>,
  pub end_time: Option<String>,
  pub create_time: Option<String>,
}

const DEFAULT_BASE_URL: &str = "https://youtubereporting.googleapis.com/v1/";

pub fn build_report_types_list_url(base_url: &str, content_owner_id: &str) -> String {
  let base = base_url.trim_end_matches('/');
  format!("{base}/reportTypes?onBehalfOfContentOwner={content_owner_id}")
}

pub fn build_report_types_list_url_channel(base_url: &str, include_system_managed: bool) -> String {
  let base = base_url.trim_end_matches('/');
  let mut url = format!("{base}/reportTypes");
  if include_system_managed {
    url.push_str("?includeSystemManaged=true");
  }
  url
}

pub fn build_jobs_list_url(base_url: &str, content_owner_id: &str) -> String {
  let base = base_url.trim_end_matches('/');
  format!("{base}/jobs?onBehalfOfContentOwner={content_owner_id}")
}

pub fn build_jobs_list_url_channel(base_url: &str, include_system_managed: bool) -> String {
  let base = base_url.trim_end_matches('/');
  let mut url = format!("{base}/jobs");
  if include_system_managed {
    url.push_str("?includeSystemManaged=true");
  }
  url
}

pub fn build_reports_list_url(
  base_url: &str,
  job_id: &str,
  content_owner_id: &str,
  created_after: Option<&str>,
) -> String {
  let base = base_url.trim_end_matches('/');
  let mut url = format!(
    "{base}/jobs/{job_id}/reports?onBehalfOfContentOwner={content_owner_id}"
  );
  if let Some(created_after) = created_after {
    url.push_str("&createdAfter=");
    url.push_str(created_after);
  }
  url
}

pub fn build_reports_list_url_channel(
  base_url: &str,
  job_id: &str,
  created_after: Option<&str>,
) -> String {
  let base = base_url.trim_end_matches('/');
  let mut url = format!("{base}/jobs/{job_id}/reports");
  if let Some(created_after) = created_after {
    url.push_str("?createdAfter=");
    url.push_str(created_after);
  }
  url
}

fn parse_report_types(json: &Value) -> Vec<YoutubeReportingReportType> {
  let array = json
    .get("reportTypes")
    .and_then(|v| v.as_array())
    .cloned()
    .unwrap_or_default();

  let mut out = Vec::with_capacity(array.len());
  for item in array {
    let report_type_id = item
      .get("id")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    if report_type_id.is_empty() {
      continue;
    }
    let report_type_name = item.get("name").and_then(|v| v.as_str()).map(|s| s.to_string());
    let system_managed = item
      .get("systemManaged")
      .and_then(|v| v.as_bool())
      .unwrap_or(false);
    out.push(YoutubeReportingReportType {
      report_type_id,
      report_type_name,
      system_managed,
    });
  }
  out
}

fn parse_jobs(json: &Value) -> Vec<YoutubeReportingJob> {
  let array = json
    .get("jobs")
    .and_then(|v| v.as_array())
    .cloned()
    .unwrap_or_default();

  let mut out = Vec::with_capacity(array.len());
  for item in array {
    let job_id = item.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    if job_id.is_empty() {
      continue;
    }

    let report_type_id = item
      .get("reportTypeId")
      .and_then(|v| v.as_str())
      .map(|s| s.to_string());
    let name = item.get("name").and_then(|v| v.as_str()).map(|s| s.to_string());

    out.push(YoutubeReportingJob {
      job_id,
      report_type_id,
      name,
    });
  }
  out
}

fn parse_reports(json: &Value) -> Vec<YoutubeReportingReport> {
  let array = json
    .get("reports")
    .and_then(|v| v.as_array())
    .cloned()
    .unwrap_or_default();

  let mut out = Vec::with_capacity(array.len());
  for item in array {
    let report_id = item.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    if report_id.is_empty() {
      continue;
    }

    let download_url = item
      .get("downloadUrl")
      .and_then(|v| v.as_str())
      .map(|s| s.to_string());
    let start_time = item
      .get("startTime")
      .and_then(|v| v.as_str())
      .map(|s| s.to_string());
    let end_time = item
      .get("endTime")
      .and_then(|v| v.as_str())
      .map(|s| s.to_string());
    let create_time = item
      .get("createTime")
      .and_then(|v| v.as_str())
      .map(|s| s.to_string());

    out.push(YoutubeReportingReport {
      report_id,
      download_url,
      start_time,
      end_time,
      create_time,
    });
  }
  out
}

async fn fetch_json_by_url(access_token: &str, url: &str) -> Result<Value, YoutubeReportingError> {
  let connector = hyper_rustls::HttpsConnectorBuilder::new()
    .with_native_roots()
    .map_err(|e| YoutubeReportingError {
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
    .map_err(|e| YoutubeReportingError {
      status: None,
      message: e.to_string(),
    })?;

  let resp = client
    .request(req)
    .await
    .map_err(|e| YoutubeReportingError {
      status: None,
      message: e.to_string(),
    })?;

  let status = resp.status();
  let body_bytes = resp
    .into_body()
    .collect()
    .await
    .map_err(|e| YoutubeReportingError {
      status: Some(status.as_u16()),
      message: e.to_string(),
    })?
    .to_bytes();

  if !status.is_success() {
    let snippet = String::from_utf8_lossy(&body_bytes);
    return Err(YoutubeReportingError {
      status: Some(status.as_u16()),
      message: snippet.chars().take(200).collect::<String>(),
    });
  }

  serde_json::from_slice(&body_bytes).map_err(|e| YoutubeReportingError {
    status: Some(status.as_u16()),
    message: e.to_string(),
  })
}

async fn request_json(
  access_token: &str,
  method: Method,
  url: &str,
  body_json: Option<Value>,
) -> Result<Value, YoutubeReportingError> {
  let connector = hyper_rustls::HttpsConnectorBuilder::new()
    .with_native_roots()
    .map_err(|e| YoutubeReportingError {
      status: None,
      message: e.to_string(),
    })?
    .https_or_http()
    .enable_http1()
    .build();

  let client =
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build(connector);

  let (body_bytes, content_type) = match body_json {
    Some(v) => (
      serde_json::to_vec(&v).map_err(|e| YoutubeReportingError {
        status: None,
        message: e.to_string(),
      })?,
      Some("application/json"),
    ),
    None => (Vec::<u8>::new(), None),
  };

  let mut builder = Request::builder()
    .method(method)
    .uri(url)
    .header(AUTHORIZATION, format!("Bearer {}", access_token))
    .header(ACCEPT, "application/json");

  if let Some(content_type) = content_type {
    builder = builder.header(CONTENT_TYPE, content_type);
  }

  let req = builder
    .body(Full::new(Bytes::from(body_bytes)))
    .map_err(|e| YoutubeReportingError {
      status: None,
      message: e.to_string(),
    })?;

  let resp = client
    .request(req)
    .await
    .map_err(|e| YoutubeReportingError {
      status: None,
      message: e.to_string(),
    })?;

  let status = resp.status();
  let body_bytes = resp
    .into_body()
    .collect()
    .await
    .map_err(|e| YoutubeReportingError {
      status: Some(status.as_u16()),
      message: e.to_string(),
    })?
    .to_bytes();

  if !status.is_success() {
    let snippet = String::from_utf8_lossy(&body_bytes);
    return Err(YoutubeReportingError {
      status: Some(status.as_u16()),
      message: snippet.chars().take(200).collect::<String>(),
    });
  }

  serde_json::from_slice(&body_bytes).map_err(|e| YoutubeReportingError {
    status: Some(status.as_u16()),
    message: e.to_string(),
  })
}

pub async fn download_report_file(access_token: &str, download_url: &str) -> Result<Bytes, YoutubeReportingError> {
  let connector = hyper_rustls::HttpsConnectorBuilder::new()
    .with_native_roots()
    .map_err(|e| YoutubeReportingError {
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
    .uri(download_url)
    .header(AUTHORIZATION, format!("Bearer {}", access_token))
    .header(ACCEPT, "application/octet-stream")
    .body(Empty::<Bytes>::new())
    .map_err(|e| YoutubeReportingError {
      status: None,
      message: e.to_string(),
    })?;

  let resp = client
    .request(req)
    .await
    .map_err(|e| YoutubeReportingError {
      status: None,
      message: e.to_string(),
    })?;

  let status = resp.status();
  let body_bytes = resp
    .into_body()
    .collect()
    .await
    .map_err(|e| YoutubeReportingError {
      status: Some(status.as_u16()),
      message: e.to_string(),
    })?
    .to_bytes();

  if !status.is_success() {
    let snippet = String::from_utf8_lossy(&body_bytes);
    return Err(YoutubeReportingError {
      status: Some(status.as_u16()),
      message: snippet.chars().take(200).collect::<String>(),
    });
  }

  Ok(body_bytes)
}

pub async fn create_job_for_report_type_with_base_url(
  access_token: &str,
  content_owner_id: &str,
  report_type_id: &str,
  base_url: &str,
) -> Result<String, YoutubeReportingError> {
  let url = build_jobs_list_url(base_url, content_owner_id);
  let body = serde_json::json!({
    "reportTypeId": report_type_id,
    "name": report_type_id
  });

  let json = request_json(access_token, Method::POST, &url, Some(body)).await?;
  let job_id = json.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
  if job_id.is_empty() {
    return Err(YoutubeReportingError {
      status: None,
      message: "Missing job id in response".to_string(),
    });
  }

  Ok(job_id)
}

pub async fn create_job_for_report_type_channel_with_base_url(
  access_token: &str,
  report_type_id: &str,
  base_url: &str,
) -> Result<String, YoutubeReportingError> {
  let url = build_jobs_list_url_channel(base_url, false);
  let body = serde_json::json!({
    "reportTypeId": report_type_id,
    "name": report_type_id
  });

  let json = request_json(access_token, Method::POST, &url, Some(body)).await?;
  let job_id = json.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
  if job_id.is_empty() {
    return Err(YoutubeReportingError {
      status: None,
      message: "Missing job id in response".to_string(),
    });
  }

  Ok(job_id)
}

pub async fn ensure_job_for_report_type_with_base_url(
  access_token: &str,
  content_owner_id: &str,
  report_type_id: &str,
  base_url: &str,
) -> Result<String, YoutubeReportingError> {
  let jobs = list_jobs_with_base_url(access_token, content_owner_id, base_url).await?;
  if let Some(job) = jobs
    .into_iter()
    .find(|j| j.report_type_id.as_deref() == Some(report_type_id))
  {
    return Ok(job.job_id);
  }

  create_job_for_report_type_with_base_url(access_token, content_owner_id, report_type_id, base_url).await
}

pub async fn ensure_job_for_report_type_channel_with_base_url(
  access_token: &str,
  report_type_id: &str,
  base_url: &str,
) -> Result<String, YoutubeReportingError> {
  let jobs = list_jobs_channel_with_base_url(access_token, base_url, true).await?;
  if let Some(job) = jobs
    .into_iter()
    .find(|j| j.report_type_id.as_deref() == Some(report_type_id))
  {
    return Ok(job.job_id);
  }

  create_job_for_report_type_channel_with_base_url(access_token, report_type_id, base_url).await
}

pub async fn ensure_job_for_report_type(
  access_token: &str,
  content_owner_id: &str,
  report_type_id: &str,
) -> Result<String, YoutubeReportingError> {
  ensure_job_for_report_type_with_base_url(access_token, content_owner_id, report_type_id, DEFAULT_BASE_URL).await
}

pub async fn ensure_job_for_report_type_channel(
  access_token: &str,
  report_type_id: &str,
) -> Result<String, YoutubeReportingError> {
  ensure_job_for_report_type_channel_with_base_url(access_token, report_type_id, DEFAULT_BASE_URL).await
}

pub async fn list_report_types_with_base_url(
  access_token: &str,
  content_owner_id: &str,
  base_url: &str,
) -> Result<Vec<YoutubeReportingReportType>, YoutubeReportingError> {
  let url = build_report_types_list_url(base_url, content_owner_id);
  let json = fetch_json_by_url(access_token, &url).await?;
  Ok(parse_report_types(&json))
}

pub async fn list_report_types(
  access_token: &str,
  content_owner_id: &str,
) -> Result<Vec<YoutubeReportingReportType>, YoutubeReportingError> {
  list_report_types_with_base_url(access_token, content_owner_id, DEFAULT_BASE_URL).await
}

pub async fn list_report_types_channel_with_base_url(
  access_token: &str,
  base_url: &str,
  include_system_managed: bool,
) -> Result<Vec<YoutubeReportingReportType>, YoutubeReportingError> {
  let url = build_report_types_list_url_channel(base_url, include_system_managed);
  let json = fetch_json_by_url(access_token, &url).await?;
  Ok(parse_report_types(&json))
}

pub async fn list_report_types_channel(
  access_token: &str,
  include_system_managed: bool,
) -> Result<Vec<YoutubeReportingReportType>, YoutubeReportingError> {
  list_report_types_channel_with_base_url(access_token, DEFAULT_BASE_URL, include_system_managed).await
}

pub async fn list_jobs_with_base_url(
  access_token: &str,
  content_owner_id: &str,
  base_url: &str,
) -> Result<Vec<YoutubeReportingJob>, YoutubeReportingError> {
  let url = build_jobs_list_url(base_url, content_owner_id);
  let json = fetch_json_by_url(access_token, &url).await?;
  Ok(parse_jobs(&json))
}

pub async fn list_jobs(
  access_token: &str,
  content_owner_id: &str,
) -> Result<Vec<YoutubeReportingJob>, YoutubeReportingError> {
  list_jobs_with_base_url(access_token, content_owner_id, DEFAULT_BASE_URL).await
}

pub async fn list_jobs_channel_with_base_url(
  access_token: &str,
  base_url: &str,
  include_system_managed: bool,
) -> Result<Vec<YoutubeReportingJob>, YoutubeReportingError> {
  let url = build_jobs_list_url_channel(base_url, include_system_managed);
  let json = fetch_json_by_url(access_token, &url).await?;
  Ok(parse_jobs(&json))
}

pub async fn list_jobs_channel(
  access_token: &str,
  include_system_managed: bool,
) -> Result<Vec<YoutubeReportingJob>, YoutubeReportingError> {
  list_jobs_channel_with_base_url(access_token, DEFAULT_BASE_URL, include_system_managed).await
}

pub async fn list_reports_with_base_url(
  access_token: &str,
  job_id: &str,
  content_owner_id: &str,
  base_url: &str,
  created_after: Option<&str>,
) -> Result<Vec<YoutubeReportingReport>, YoutubeReportingError> {
  let url = build_reports_list_url(base_url, job_id, content_owner_id, created_after);
  let json = fetch_json_by_url(access_token, &url).await?;
  Ok(parse_reports(&json))
}

pub async fn list_reports(
  access_token: &str,
  job_id: &str,
  content_owner_id: &str,
  created_after: Option<&str>,
) -> Result<Vec<YoutubeReportingReport>, YoutubeReportingError> {
  list_reports_with_base_url(access_token, job_id, content_owner_id, DEFAULT_BASE_URL, created_after).await
}

pub async fn list_reports_channel_with_base_url(
  access_token: &str,
  job_id: &str,
  base_url: &str,
  created_after: Option<&str>,
) -> Result<Vec<YoutubeReportingReport>, YoutubeReportingError> {
  let url = build_reports_list_url_channel(base_url, job_id, created_after);
  let json = fetch_json_by_url(access_token, &url).await?;
  Ok(parse_reports(&json))
}

pub async fn list_reports_channel(
  access_token: &str,
  job_id: &str,
  created_after: Option<&str>,
) -> Result<Vec<YoutubeReportingReport>, YoutubeReportingError> {
  list_reports_channel_with_base_url(access_token, job_id, DEFAULT_BASE_URL, created_after).await
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

  #[test]
  fn builds_report_types_list_url_with_content_owner() {
    let url = build_report_types_list_url("https://youtubereporting.googleapis.com/v1/", "CMS123");
    assert_eq!(
      url,
      "https://youtubereporting.googleapis.com/v1/reportTypes?onBehalfOfContentOwner=CMS123"
    );
  }

  #[test]
  fn builds_jobs_list_url_with_content_owner() {
    let url = build_jobs_list_url("https://youtubereporting.googleapis.com/v1", "CMS123");
    assert_eq!(
      url,
      "https://youtubereporting.googleapis.com/v1/jobs?onBehalfOfContentOwner=CMS123"
    );
  }

  #[test]
  fn builds_report_types_list_url_for_channel() {
    let url = build_report_types_list_url_channel("https://youtubereporting.googleapis.com/v1/", true);
    assert_eq!(
      url,
      "https://youtubereporting.googleapis.com/v1/reportTypes?includeSystemManaged=true"
    );
  }

  #[test]
  fn builds_jobs_list_url_for_channel() {
    let url = build_jobs_list_url_channel("https://youtubereporting.googleapis.com/v1", true);
    assert_eq!(
      url,
      "https://youtubereporting.googleapis.com/v1/jobs?includeSystemManaged=true"
    );
  }

  #[test]
  fn builds_reports_list_url_for_channel() {
    let url = build_reports_list_url_channel(
      "https://youtubereporting.googleapis.com/v1/",
      "job_1",
      Some("2026-01-01T00:00:00Z"),
    );
    assert_eq!(
      url,
      "https://youtubereporting.googleapis.com/v1/jobs/job_1/reports?createdAfter=2026-01-01T00:00:00Z"
    );
  }

  #[test]
  fn builds_reports_list_url_with_created_after() {
    let url = build_reports_list_url(
      "https://youtubereporting.googleapis.com/v1/",
      "job_1",
      "CMS123",
      Some("2026-01-01T00:00:00Z"),
    );
    assert_eq!(
      url,
      "https://youtubereporting.googleapis.com/v1/jobs/job_1/reports?onBehalfOfContentOwner=CMS123&createdAfter=2026-01-01T00:00:00Z"
    );
  }

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

          let body = r#"{"reportTypes":[{"id":"rt1","name":"Report 1","systemManaged":true}]}"#;
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
  async fn lists_report_types_against_mock_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}/", addr);

    let task = tokio::spawn(serve_one(listener));

    let types = list_report_types_with_base_url("token123", "CMS123", &base_url)
      .await
      .unwrap();
    assert_eq!(types.len(), 1);
    assert_eq!(types[0].report_type_id, "rt1");
    assert_eq!(types[0].report_type_name.as_deref(), Some("Report 1"));
    assert_eq!(types[0].system_managed, true);

    task.await.unwrap();
  }

  #[test]
  fn parses_jobs_from_list_response() {
    let json: Value = serde_json::from_str(
      r#"{
        "jobs": [
          { "id": "job1", "reportTypeId": "rt1", "name": "My Job" }
        ]
      }"#,
    )
    .unwrap();

    let jobs = parse_jobs(&json);
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, "job1");
    assert_eq!(jobs[0].report_type_id.as_deref(), Some("rt1"));
    assert_eq!(jobs[0].name.as_deref(), Some("My Job"));
  }

  #[test]
  fn parses_reports_from_list_response() {
    let json: Value = serde_json::from_str(
      r#"{
        "reports": [
          {
            "id": "rep1",
            "downloadUrl": "https://example.com/file",
            "startTime": "2026-01-01T00:00:00Z",
            "endTime": "2026-01-02T00:00:00Z",
            "createTime": "2026-01-03T00:00:00Z"
          }
        ]
      }"#,
    )
    .unwrap();

    let reports = parse_reports(&json);
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].report_id, "rep1");
    assert_eq!(
      reports[0].download_url.as_deref(),
      Some("https://example.com/file")
    );
    assert_eq!(
      reports[0].start_time.as_deref(),
      Some("2026-01-01T00:00:00Z")
    );
    assert_eq!(
      reports[0].end_time.as_deref(),
      Some("2026-01-02T00:00:00Z")
    );
    assert_eq!(
      reports[0].create_time.as_deref(),
      Some("2026-01-03T00:00:00Z")
    );
  }

  async fn serve_one_download(listener: TcpListener) {
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

          Ok::<_, hyper::Error>(
            Response::builder()
              .status(StatusCode::OK)
              .body(Full::new(Bytes::from_static(b"hello")) )
              .unwrap(),
          )
        }),
      )
      .await
      .unwrap();
  }

  #[tokio::test]
  async fn downloads_report_file_bytes_against_mock_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let download_url = format!("http://{}/file", addr);

    let task = tokio::spawn(serve_one_download(listener));

    let bytes = download_report_file("token123", &download_url)
      .await
      .unwrap();
    assert_eq!(bytes, Bytes::from_static(b"hello"));

    task.await.unwrap();
  }

  async fn serve_one_job_create(listener: TcpListener) {
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

          let path = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("");
          if req.method() != Method::POST
            || path != "/jobs?onBehalfOfContentOwner=CMS123"
          {
            return Ok::<_, hyper::Error>(
              Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from_static(b"not found")))
                .unwrap(),
            );
          }

          let body = req.into_body().collect().await.unwrap().to_bytes();
          let body_str = String::from_utf8_lossy(&body);
          if !body_str.contains("\"reportTypeId\":\"rt1\"") {
            return Ok::<_, hyper::Error>(
              Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from_static(b"bad body")))
                .unwrap(),
            );
          }

          let resp = r#"{"id":"job_created"}"#;
          Ok::<_, hyper::Error>(
            Response::builder()
              .status(StatusCode::OK)
              .header("content-type", "application/json")
              .body(Full::new(Bytes::from(resp)))
              .unwrap(),
          )
        }),
      )
      .await
      .unwrap();
  }

  #[tokio::test]
  async fn creates_job_for_report_type_against_mock_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}/", addr);

    let task = tokio::spawn(serve_one_job_create(listener));

    let job_id = create_job_for_report_type_with_base_url("token123", "CMS123", "rt1", &base_url)
      .await
      .unwrap();
    assert_eq!(job_id, "job_created");

    task.await.unwrap();
  }

  async fn serve_one_jobs_list_has_match(listener: TcpListener) {
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

          if req.method() != Method::GET {
            return Ok::<_, hyper::Error>(
              Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from_static(b"expected get")))
                .unwrap(),
            );
          }

          let body = r#"{"jobs":[{"id":"job_existing","reportTypeId":"rt1","name":"Existing"}]}"#;
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
  async fn ensures_job_for_report_type_uses_existing_job() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}/", addr);

    let task = tokio::spawn(serve_one_jobs_list_has_match(listener));

    let job_id =
      ensure_job_for_report_type_with_base_url("token123", "CMS123", "rt1", &base_url)
        .await
        .unwrap();
    assert_eq!(job_id, "job_existing");

    task.await.unwrap();
  }
}

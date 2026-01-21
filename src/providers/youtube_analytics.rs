use bytes::Bytes;
use chrono::NaiveDate;
use http_body_util::{BodyExt, Empty};
use hyper::header::{ACCEPT, AUTHORIZATION};
use hyper::{Method, Request, StatusCode};
use serde_json::Value;
use vercel_runtime::Error;

#[derive(Debug, Clone)]
pub struct VideoDailyMetricRow {
  pub dt: NaiveDate,
  pub video_id: String,
  pub estimated_revenue_usd: f64,
  pub impressions: i64,
  pub views: i64,
}

#[derive(Debug)]
pub struct YoutubeAnalyticsError {
  pub status: Option<u16>,
  pub message: String,
}

impl std::fmt::Display for YoutubeAnalyticsError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if let Some(status) = self.status {
      write!(f, "YouTube Analytics error (status {status}): {}", self.message)
    } else {
      write!(f, "YouTube Analytics error: {}", self.message)
    }
  }
}

impl std::error::Error for YoutubeAnalyticsError {}

pub fn build_reports_url(base_url: &str, start_dt: NaiveDate, end_dt: NaiveDate) -> String {
  let base = base_url.trim_end_matches('/');
  format!(
    "{base}/v2/reports?ids=channel==MINE&startDate={}&endDate={}&metrics=estimatedRevenue,impressions,views&dimensions=day,video&sort=day&maxResults=200",
    start_dt,
    end_dt
  )
}

fn parse_rows(json: &Value) -> Vec<VideoDailyMetricRow> {
  let headers = json
    .get("columnHeaders")
    .and_then(|v| v.as_array())
    .cloned()
    .unwrap_or_default();

  let mut idx_day: Option<usize> = None;
  let mut idx_video: Option<usize> = None;
  let mut idx_rev: Option<usize> = None;
  let mut idx_impr: Option<usize> = None;
  let mut idx_views: Option<usize> = None;

  for (i, h) in headers.iter().enumerate() {
    let name = h.get("name").and_then(|v| v.as_str()).unwrap_or("");
    match name {
      "day" => idx_day = Some(i),
      "video" => idx_video = Some(i),
      "estimatedRevenue" => idx_rev = Some(i),
      "impressions" => idx_impr = Some(i),
      "views" => idx_views = Some(i),
      _ => {}
    }
  }

  let (idx_day, idx_video, idx_rev) = match (idx_day, idx_video, idx_rev) {
    (Some(a), Some(b), Some(c)) => (a, b, c),
    _ => return vec![],
  };

  let rows = json
    .get("rows")
    .and_then(|v| v.as_array())
    .cloned()
    .unwrap_or_default();

  let mut out = Vec::with_capacity(rows.len());

  for row in rows {
    let arr = match row.as_array() {
      Some(a) => a,
      None => continue,
    };

    let day_str = arr.get(idx_day).and_then(|v| v.as_str()).unwrap_or("");
    let dt = match NaiveDate::parse_from_str(day_str, "%Y-%m-%d") {
      Ok(d) => d,
      Err(_) => continue,
    };

    let video_id = arr
      .get(idx_video)
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string();
    if video_id.is_empty() {
      continue;
    }

    let estimated_revenue_usd = arr
      .get(idx_rev)
      .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
      .unwrap_or(0.0);

    let impressions = idx_impr
      .and_then(|i| arr.get(i))
      .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|n| n as i64)))
      .unwrap_or(0);

    let views = idx_views
      .and_then(|i| arr.get(i))
      .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|n| n as i64)))
      .unwrap_or(0);

    out.push(VideoDailyMetricRow {
      dt,
      video_id,
      estimated_revenue_usd,
      impressions,
      views,
    });
  }

  out
}

async fn fetch_report_json_with_base_url(
  access_token: &str,
  base_url: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> Result<Value, YoutubeAnalyticsError> {
  let url = build_reports_url(base_url, start_dt, end_dt);

  let connector = hyper_rustls::HttpsConnectorBuilder::new()
    .with_native_roots()
    .map_err(|e| YoutubeAnalyticsError {
      status: None,
      message: e.to_string(),
    })?
    .https_or_http()
    .enable_http1()
    .build();

  let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build(connector);

  let req = Request::builder()
    .method(Method::GET)
    .uri(url)
    .header(AUTHORIZATION, format!("Bearer {}", access_token))
    .header(ACCEPT, "application/json")
    .body(Empty::<Bytes>::new())
    .map_err(|e| YoutubeAnalyticsError {
      status: None,
      message: e.to_string(),
    })?;

  let resp = client
    .request(req)
    .await
    .map_err(|e| YoutubeAnalyticsError {
      status: None,
      message: e.to_string(),
    })?;

  let status = resp.status();
  let body_bytes = resp
    .into_body()
    .collect()
    .await
    .map_err(|e| YoutubeAnalyticsError {
      status: Some(status.as_u16()),
      message: e.to_string(),
    })?
    .to_bytes();

  if status != StatusCode::OK {
    let msg = String::from_utf8_lossy(&body_bytes).to_string();
    return Err(YoutubeAnalyticsError {
      status: Some(status.as_u16()),
      message: msg,
    });
  }

  serde_json::from_slice::<Value>(&body_bytes).map_err(|e| YoutubeAnalyticsError {
    status: Some(status.as_u16()),
    message: format!("invalid json response: {e}"),
  })
}

pub async fn fetch_video_daily_metrics_with_base_url(
  access_token: &str,
  base_url: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> Result<Vec<VideoDailyMetricRow>, YoutubeAnalyticsError> {
  let json = fetch_report_json_with_base_url(access_token, base_url, start_dt, end_dt).await?;
  Ok(parse_rows(&json))
}

pub async fn fetch_video_daily_metrics(
  access_token: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> Result<Vec<VideoDailyMetricRow>, YoutubeAnalyticsError> {
  fetch_video_daily_metrics_with_base_url(
    access_token,
    "https://youtubeanalytics.googleapis.com/",
    start_dt,
    end_dt,
  )
  .await
}

pub fn youtube_analytics_error_to_vercel_error(err: YoutubeAnalyticsError) -> Error {
  Box::new(err) as Error
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn build_reports_url_includes_expected_params() {
    let start_dt = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end_dt = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
    let url = build_reports_url("https://youtubeanalytics.googleapis.com/", start_dt, end_dt);

    assert!(url.contains("/v2/reports?"));
    assert!(url.contains("ids=channel==MINE"));
    assert!(url.contains("startDate=2026-01-01"));
    assert!(url.contains("endDate=2026-01-07"));
    assert!(url.contains("metrics=estimatedRevenue,impressions,views"));
    assert!(url.contains("dimensions=day,video"));
  }

  #[test]
  fn parse_rows_extracts_metrics() {
    let json: Value = serde_json::from_str(
      r#"
      {
        "columnHeaders": [
          {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
          {"name":"video","columnType":"DIMENSION","dataType":"STRING"},
          {"name":"estimatedRevenue","columnType":"METRIC","dataType":"FLOAT"},
          {"name":"impressions","columnType":"METRIC","dataType":"INTEGER"},
          {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
        ],
        "rows": [
          ["2026-01-02","vid1", 1.25, 1000, 200],
          ["2026-01-03","vid2", 0.0, 0, 0]
        ]
      }
    "#,
    )
    .unwrap();

    let rows = parse_rows(&json);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].video_id, "vid1");
    assert_eq!(rows[0].estimated_revenue_usd, 1.25);
    assert_eq!(rows[0].impressions, 1000);
    assert_eq!(rows[0].views, 200);
  }
}


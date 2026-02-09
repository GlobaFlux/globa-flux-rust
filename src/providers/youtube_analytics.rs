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
  pub impressions_ctr: Option<f64>,
  pub views: i64,
}

#[derive(Debug, Clone)]
pub struct VideoTotalsRow {
  pub video_id: String,
  pub estimated_revenue_usd: f64,
  pub views: i64,
}

const FALLBACK_CHANNEL_VIDEO_ID: &str = "__CHANNEL_TOTAL__";

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

fn is_query_not_supported(err: &YoutubeAnalyticsError) -> bool {
  let msg = err.message.as_str();
  err.status == Some(400)
    && (msg.contains("The query is not supported")
      || msg.contains("Unknown identifier")
      || msg.contains("Unknown metric")
      || msg.contains("Unknown dimension"))
}

fn is_forbidden(err: &YoutubeAnalyticsError) -> bool {
  err.status == Some(403)
}

fn should_fallback_to_views_only(err: &YoutubeAnalyticsError) -> bool {
  is_query_not_supported(err) || is_forbidden(err)
}

fn build_reports_url_with_ids_and_metrics(
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
  metrics: &str,
) -> String {
  let base = base_url.trim_end_matches('/');
  format!(
    "{base}/v2/reports?ids={ids_value}&startDate={}&endDate={}&metrics={metrics}&dimensions=day,video&sort=day&maxResults=200",
    start_dt, end_dt
  )
}

fn build_reports_url_with_ids(
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> String {
  // Keep this request to stable metrics; we fetch impressions via a separate query and merge.
  build_reports_url_with_ids_and_metrics(base_url, ids_value, start_dt, end_dt, "estimatedRevenue,views")
}

fn build_reports_url_with_ids_views_only(
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> String {
  build_reports_url_with_ids_and_metrics(base_url, ids_value, start_dt, end_dt, "views")
}

fn build_reports_url_with_ids_impressions(
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> String {
  build_reports_url_with_ids_and_metrics(
    base_url,
    ids_value,
    start_dt,
    end_dt,
    "videoThumbnailImpressions,videoThumbnailImpressionsClickRate",
  )
}

fn build_video_totals_url_with_ids_and_metrics(
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
  metrics: &str,
  sort: &str,
  max_results: i64,
) -> String {
  let base = base_url.trim_end_matches('/');
  format!(
    "{base}/v2/reports?ids={ids_value}&startDate={}&endDate={}&metrics={metrics}&dimensions=video&sort={sort}&maxResults={max_results}",
    start_dt, end_dt
  )
}

pub fn build_reports_url(base_url: &str, start_dt: NaiveDate, end_dt: NaiveDate) -> String {
  build_reports_url_with_ids(base_url, "channel==MINE", start_dt, end_dt)
}

fn build_reports_url_views_only(base_url: &str, start_dt: NaiveDate, end_dt: NaiveDate) -> String {
  build_reports_url_with_ids_views_only(base_url, "channel==MINE", start_dt, end_dt)
}

pub fn build_reports_url_for_channel(
  base_url: &str,
  channel_id: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> String {
  build_reports_url_with_ids(base_url, &format!("channel=={}", channel_id.trim()), start_dt, end_dt)
}

fn build_channel_reports_url_with_ids(
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> String {
  build_channel_reports_url_with_ids_and_metrics(base_url, ids_value, start_dt, end_dt, "estimatedRevenue,views")
}

fn build_channel_reports_url_with_ids_views_only(
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> String {
  build_channel_reports_url_with_ids_and_metrics(base_url, ids_value, start_dt, end_dt, "views")
}

fn build_channel_reports_url_with_ids_and_metrics(
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
  metrics: &str,
) -> String {
  let base = base_url.trim_end_matches('/');
  format!(
    "{base}/v2/reports?ids={ids_value}&startDate={}&endDate={}&metrics={metrics}&dimensions=day&sort=day&maxResults=200",
    start_dt, end_dt
  )
}

fn build_channel_reports_url_with_ids_impressions(
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> String {
  build_channel_reports_url_with_ids_and_metrics(
    base_url,
    ids_value,
    start_dt,
    end_dt,
    "videoThumbnailImpressions,videoThumbnailImpressionsClickRate",
  )
}

fn build_channel_reports_url(base_url: &str, start_dt: NaiveDate, end_dt: NaiveDate) -> String {
  build_channel_reports_url_with_ids(base_url, "channel==MINE", start_dt, end_dt)
}

fn build_channel_reports_url_views_only(base_url: &str, start_dt: NaiveDate, end_dt: NaiveDate) -> String {
  build_channel_reports_url_with_ids_views_only(base_url, "channel==MINE", start_dt, end_dt)
}

fn build_channel_reports_url_for_channel(
  base_url: &str,
  channel_id: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> String {
  build_channel_reports_url_with_ids(
    base_url,
    &format!("channel=={}", channel_id.trim()),
    start_dt,
    end_dt,
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
  let mut idx_ctr: Option<usize> = None;
  let mut idx_views: Option<usize> = None;

  for (i, h) in headers.iter().enumerate() {
    let name = h.get("name").and_then(|v| v.as_str()).unwrap_or("");
    match name {
      "day" => idx_day = Some(i),
      "video" => idx_video = Some(i),
      "estimatedRevenue" => idx_rev = Some(i),
      "impressions" | "videoThumbnailImpressions" => idx_impr = Some(i),
      "videoThumbnailImpressionsClickRate" => idx_ctr = Some(i),
      "views" => idx_views = Some(i),
      _ => {}
    }
  }

  let (idx_day, idx_video) = match (idx_day, idx_video) {
    (Some(a), Some(b)) => (a, b),
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

    let estimated_revenue_usd = idx_rev
      .and_then(|i| arr.get(i))
      .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
      .unwrap_or(0.0);

    let impressions = idx_impr
      .and_then(|i| arr.get(i))
      .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|n| n as i64)))
      .unwrap_or(0);

    let impressions_ctr = idx_ctr
      .and_then(|i| arr.get(i))
      .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));

    let views = idx_views
      .and_then(|i| arr.get(i))
      .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|n| n as i64)))
      .unwrap_or(0);

    out.push(VideoDailyMetricRow {
      dt,
      video_id,
      estimated_revenue_usd,
      impressions,
      impressions_ctr,
      views,
    });
  }

  out
}

fn parse_rows_channel(json: &Value) -> Vec<VideoDailyMetricRow> {
  let headers = json
    .get("columnHeaders")
    .and_then(|v| v.as_array())
    .cloned()
    .unwrap_or_default();

  let mut idx_day: Option<usize> = None;
  let mut idx_rev: Option<usize> = None;
  let mut idx_impr: Option<usize> = None;
  let mut idx_ctr: Option<usize> = None;
  let mut idx_views: Option<usize> = None;

  for (i, h) in headers.iter().enumerate() {
    let name = h.get("name").and_then(|v| v.as_str()).unwrap_or("");
    match name {
      "day" => idx_day = Some(i),
      "estimatedRevenue" => idx_rev = Some(i),
      "impressions" | "videoThumbnailImpressions" => idx_impr = Some(i),
      "videoThumbnailImpressionsClickRate" => idx_ctr = Some(i),
      "views" => idx_views = Some(i),
      _ => {}
    }
  }

  let idx_day = match idx_day {
    Some(v) => v,
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

    let estimated_revenue_usd = idx_rev
      .and_then(|i| arr.get(i))
      .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
      .unwrap_or(0.0);

    let impressions = idx_impr
      .and_then(|i| arr.get(i))
      .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|n| n as i64)))
      .unwrap_or(0);

    let impressions_ctr = idx_ctr
      .and_then(|i| arr.get(i))
      .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));

    let views = idx_views
      .and_then(|i| arr.get(i))
      .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|n| n as i64)))
      .unwrap_or(0);

    out.push(VideoDailyMetricRow {
      dt,
      video_id: FALLBACK_CHANNEL_VIDEO_ID.to_string(),
      estimated_revenue_usd,
      impressions,
      impressions_ctr,
      views,
    });
  }

  out
}

fn parse_video_totals_rows(json: &Value) -> Vec<VideoTotalsRow> {
  let headers = json
    .get("columnHeaders")
    .and_then(|v| v.as_array())
    .cloned()
    .unwrap_or_default();

  let mut idx_video: Option<usize> = None;
  let mut idx_rev: Option<usize> = None;
  let mut idx_views: Option<usize> = None;

  for (i, h) in headers.iter().enumerate() {
    let name = h.get("name").and_then(|v| v.as_str()).unwrap_or("");
    match name {
      "video" => idx_video = Some(i),
      "estimatedRevenue" => idx_rev = Some(i),
      "views" => idx_views = Some(i),
      _ => {}
    }
  }

  let idx_video = match idx_video {
    Some(v) => v,
    None => return vec![],
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

    let video_id = arr
      .get(idx_video)
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .trim()
      .to_string();
    if video_id.is_empty() {
      continue;
    }

    let estimated_revenue_usd = idx_rev
      .and_then(|i| arr.get(i))
      .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
      .unwrap_or(0.0);

    let views = idx_views
      .and_then(|i| arr.get(i))
      .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|n| n as i64)))
      .unwrap_or(0);

    out.push(VideoTotalsRow {
      video_id,
      estimated_revenue_usd,
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
  fetch_report_json_by_url(access_token, &url).await
}

async fn fetch_report_json_by_url(access_token: &str, url: &str) -> Result<Value, YoutubeAnalyticsError> {

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
      message: format!("{msg} (url: {url})"),
    });
  }

  serde_json::from_slice::<Value>(&body_bytes).map_err(|e| YoutubeAnalyticsError {
    status: Some(status.as_u16()),
    message: format!("invalid json response: {e}"),
  })
}

async fn fetch_video_totals_for_ids_with_base_url(
  access_token: &str,
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
  metrics: &str,
  sort: &str,
  limit: i64,
) -> Result<Vec<VideoTotalsRow>, YoutubeAnalyticsError> {
  let url = build_video_totals_url_with_ids_and_metrics(base_url, ids_value, start_dt, end_dt, metrics, sort, limit);
  let json = fetch_report_json_by_url(access_token, &url).await?;
  Ok(parse_video_totals_rows(&json))
}

async fn fetch_top_videos_by_revenue_for_ids_with_base_url(
  access_token: &str,
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
  limit: i64,
) -> Result<Vec<VideoTotalsRow>, YoutubeAnalyticsError> {
  match fetch_video_totals_for_ids_with_base_url(
    access_token,
    base_url,
    ids_value,
    start_dt,
    end_dt,
    "estimatedRevenue,views",
    "-estimatedRevenue",
    limit,
  )
  .await
  {
    Ok(rows) => Ok(rows),
    Err(err) if should_fallback_to_views_only(&err) => {
      fetch_video_totals_for_ids_with_base_url(access_token, base_url, ids_value, start_dt, end_dt, "views", "-views", limit).await
    }
    Err(err) => Err(err),
  }
}

async fn fetch_top_videos_by_views_for_ids_with_base_url(
  access_token: &str,
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
  limit: i64,
) -> Result<Vec<VideoTotalsRow>, YoutubeAnalyticsError> {
  fetch_video_totals_for_ids_with_base_url(access_token, base_url, ids_value, start_dt, end_dt, "views", "-views", limit).await
}

async fn fetch_video_daily_metrics_for_ids_with_base_url(
  access_token: &str,
  base_url: &str,
  ids_value: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> Result<Vec<VideoDailyMetricRow>, YoutubeAnalyticsError> {
  fn compute_channel_totals_from_video_rows(rows: &[VideoDailyMetricRow]) -> Vec<VideoDailyMetricRow> {
    use std::collections::BTreeMap;

    let mut by_day: BTreeMap<NaiveDate, (f64, i64, i64, f64, i64)> = BTreeMap::new();
    for row in rows.iter() {
      if row.video_id == FALLBACK_CHANNEL_VIDEO_ID {
        continue;
      }
      let entry = by_day.entry(row.dt).or_insert((0.0, 0, 0, 0.0, 0));
      entry.0 += row.estimated_revenue_usd;
      entry.1 += row.impressions;
      entry.2 += row.views;
      if let Some(ctr) = row.impressions_ctr {
        if row.impressions > 0 {
          entry.3 += ctr * (row.impressions as f64);
          entry.4 += row.impressions;
        }
      }
    }

    by_day
      .into_iter()
      .map(|(dt, (rev, impressions, views, ctr_weighted_sum, ctr_weight_impr))| {
        let impressions_ctr = if ctr_weight_impr > 0 {
          Some(ctr_weighted_sum / (ctr_weight_impr as f64))
        } else {
          None
        };
        VideoDailyMetricRow {
          dt,
          video_id: FALLBACK_CHANNEL_VIDEO_ID.to_string(),
          estimated_revenue_usd: rev,
          impressions,
          impressions_ctr,
          views,
        }
      })
      .collect()
  }

  // Prefer video-level report. Some channels/projects return 0 rows for `dimensions=day,video`,
  // so we fall back to day-level aggregation to at least populate the pipeline.
  let mut video_rows: Vec<VideoDailyMetricRow> = Vec::new();

  let video_url = build_reports_url_with_ids(base_url, ids_value, start_dt, end_dt);
  match fetch_report_json_by_url(access_token, &video_url).await {
    Ok(json) => {
      let parsed = parse_rows(&json);
      if !parsed.is_empty() {
        video_rows = parsed;
      } else {
        let video_url = build_reports_url_with_ids_views_only(base_url, ids_value, start_dt, end_dt);
        match fetch_report_json_by_url(access_token, &video_url).await {
          Ok(json) => {
            let parsed = parse_rows(&json);
            if !parsed.is_empty() {
              video_rows = parsed;
            }
          }
          Err(err) if should_fallback_to_views_only(&err) => {}
          Err(err) => return Err(err),
        }
      }
    }
    Err(err) if should_fallback_to_views_only(&err) => {
      let video_url = build_reports_url_with_ids_views_only(base_url, ids_value, start_dt, end_dt);
      match fetch_report_json_by_url(access_token, &video_url).await {
        Ok(json) => {
          let parsed = parse_rows(&json);
          if !parsed.is_empty() {
            video_rows = parsed;
          }
        }
        Err(err) if should_fallback_to_views_only(&err) => {}
        Err(err) => return Err(err),
      }
    }
    Err(err) => return Err(err),
  };

  if !video_rows.is_empty() {
    // Best-effort: fetch impressions (some accounts support it only via separate query).
    let impressions_url = build_reports_url_with_ids_impressions(base_url, ids_value, start_dt, end_dt);
    if let Ok(json) = fetch_report_json_by_url(access_token, &impressions_url).await {
      let parsed = parse_rows(&json);
      if !parsed.is_empty() {
        use std::collections::HashMap;
        let mut index: HashMap<(NaiveDate, String), usize> = HashMap::new();
        for (i, row) in video_rows.iter().enumerate() {
          index.insert((row.dt, row.video_id.clone()), i);
        }

        for row in parsed.into_iter() {
          if let Some(idx) = index.get(&(row.dt, row.video_id.clone())).copied() {
            video_rows[idx].impressions = row.impressions;
            if row.impressions_ctr.is_some() {
              video_rows[idx].impressions_ctr = row.impressions_ctr;
            }
            if video_rows[idx].views == 0 {
              video_rows[idx].views = row.views;
            }
          } else {
            video_rows.push(row);
          }
        }
      }
    }

    // Always try to add channel-level totals rows so downstream queries can avoid summing per-video rows.
    // If the channel report fails, fall back to aggregating the video-level rows (may be partial).
    let has_channel_totals = video_rows
      .iter()
      .any(|row| row.video_id == FALLBACK_CHANNEL_VIDEO_ID);

    if !has_channel_totals {
      let channel_url = build_channel_reports_url_with_ids(base_url, ids_value, start_dt, end_dt);
      let mut totals_rows = match fetch_report_json_by_url(access_token, &channel_url).await {
        Ok(json) => parse_rows_channel(&json),
        Err(err) if should_fallback_to_views_only(&err) => {
          let channel_url =
            build_channel_reports_url_with_ids_views_only(base_url, ids_value, start_dt, end_dt);
          match fetch_report_json_by_url(access_token, &channel_url).await {
            Ok(json) => parse_rows_channel(&json),
            Err(_) => Vec::new(),
          }
        }
        Err(_) => Vec::new(),
      };

      // Best-effort: fill impressions via separate channel report.
      let channel_impressions_url =
        build_channel_reports_url_with_ids_impressions(base_url, ids_value, start_dt, end_dt);
      match fetch_report_json_by_url(access_token, &channel_impressions_url).await {
        Ok(json) => {
          let impr_rows = parse_rows_channel(&json);
          if !impr_rows.is_empty() {
            use std::collections::HashMap;
            let mut index: HashMap<NaiveDate, usize> = HashMap::new();
            for (i, row) in totals_rows.iter().enumerate() {
              index.insert(row.dt, i);
            }

            for row in impr_rows.into_iter() {
              if let Some(idx) = index.get(&row.dt).copied() {
                totals_rows[idx].impressions = row.impressions;
                if row.impressions_ctr.is_some() {
                  totals_rows[idx].impressions_ctr = row.impressions_ctr;
                }
                if totals_rows[idx].views == 0 {
                  totals_rows[idx].views = row.views;
                }
              } else {
                totals_rows.push(row);
              }
            }
          }
        }
        Err(_) => {}
      }

      if totals_rows.is_empty() {
        totals_rows = compute_channel_totals_from_video_rows(&video_rows);
      }

      if !totals_rows.is_empty() {
        video_rows.extend(totals_rows);
      }
    }
    return Ok(video_rows);
  }

  let channel_url = build_channel_reports_url_with_ids(base_url, ids_value, start_dt, end_dt);
  match fetch_report_json_by_url(access_token, &channel_url).await {
    Ok(json) => {
      let mut rows = parse_rows_channel(&json);

      let channel_impressions_url =
        build_channel_reports_url_with_ids_impressions(base_url, ids_value, start_dt, end_dt);
      match fetch_report_json_by_url(access_token, &channel_impressions_url).await {
        Ok(impr_json) => {
          let impr_rows = parse_rows_channel(&impr_json);
          if !impr_rows.is_empty() {
            use std::collections::HashMap;
            let mut index: HashMap<NaiveDate, usize> = HashMap::new();
            for (i, row) in rows.iter().enumerate() {
              index.insert(row.dt, i);
            }

            for row in impr_rows.into_iter() {
              if let Some(idx) = index.get(&row.dt).copied() {
                rows[idx].impressions = row.impressions;
                if row.impressions_ctr.is_some() {
                  rows[idx].impressions_ctr = row.impressions_ctr;
                }
                if rows[idx].views == 0 {
                  rows[idx].views = row.views;
                }
              } else {
                rows.push(row);
              }
            }
          }
        }
        Err(_) => {}
      }

      Ok(rows)
    }
    Err(err) if should_fallback_to_views_only(&err) => {
      let channel_url =
        build_channel_reports_url_with_ids_views_only(base_url, ids_value, start_dt, end_dt);
      let json = fetch_report_json_by_url(access_token, &channel_url).await?;
      let mut rows = parse_rows_channel(&json);

      let channel_impressions_url =
        build_channel_reports_url_with_ids_impressions(base_url, ids_value, start_dt, end_dt);
      match fetch_report_json_by_url(access_token, &channel_impressions_url).await {
        Ok(impr_json) => {
          let impr_rows = parse_rows_channel(&impr_json);
          if !impr_rows.is_empty() {
            use std::collections::HashMap;
            let mut index: HashMap<NaiveDate, usize> = HashMap::new();
            for (i, row) in rows.iter().enumerate() {
              index.insert(row.dt, i);
            }

            for row in impr_rows.into_iter() {
              if let Some(idx) = index.get(&row.dt).copied() {
                rows[idx].impressions = row.impressions;
                if row.impressions_ctr.is_some() {
                  rows[idx].impressions_ctr = row.impressions_ctr;
                }
                if rows[idx].views == 0 {
                  rows[idx].views = row.views;
                }
              } else {
                rows.push(row);
              }
            }
          }
        }
        Err(_) => {}
      }

      Ok(rows)
    }
    Err(err) => Err(err),
  }
}

async fn fetch_video_daily_metrics_for_channel_with_base_url(
  access_token: &str,
  base_url: &str,
  channel_id: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> Result<Vec<VideoDailyMetricRow>, YoutubeAnalyticsError> {
  let channel_id = channel_id.trim();
  if channel_id.is_empty() {
    return Err(YoutubeAnalyticsError {
      status: None,
      message: "missing channel_id".to_string(),
    });
  }

  let ids_value = format!("channel=={}", channel_id);
  fetch_video_daily_metrics_for_ids_with_base_url(access_token, base_url, &ids_value, start_dt, end_dt).await
}

pub async fn fetch_video_daily_metrics_for_channel(
  access_token: &str,
  channel_id: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> Result<Vec<VideoDailyMetricRow>, YoutubeAnalyticsError> {
  fetch_video_daily_metrics_for_channel_with_base_url(
    access_token,
    "https://youtubeanalytics.googleapis.com/",
    channel_id,
    start_dt,
    end_dt,
  )
  .await
}

pub async fn fetch_video_daily_metrics_with_base_url(
  access_token: &str,
  base_url: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> Result<Vec<VideoDailyMetricRow>, YoutubeAnalyticsError> {
  fetch_video_daily_metrics_for_ids_with_base_url(access_token, base_url, "channel==MINE", start_dt, end_dt).await
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

pub async fn fetch_top_videos_by_revenue_for_channel(
  access_token: &str,
  channel_id: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
  limit: i64,
) -> Result<Vec<VideoTotalsRow>, YoutubeAnalyticsError> {
  let channel_id = channel_id.trim();
  if channel_id.is_empty() {
    return Err(YoutubeAnalyticsError {
      status: None,
      message: "missing channel_id".to_string(),
    });
  }

  let ids_value = format!("channel=={}", channel_id);
  fetch_top_videos_by_revenue_for_ids_with_base_url(
    access_token,
    "https://youtubeanalytics.googleapis.com/",
    &ids_value,
    start_dt,
    end_dt,
    limit,
  )
  .await
}

pub async fn fetch_top_videos_by_views_for_channel(
  access_token: &str,
  channel_id: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
  limit: i64,
) -> Result<Vec<VideoTotalsRow>, YoutubeAnalyticsError> {
  let channel_id = channel_id.trim();
  if channel_id.is_empty() {
    return Err(YoutubeAnalyticsError {
      status: None,
      message: "missing channel_id".to_string(),
    });
  }

  let ids_value = format!("channel=={}", channel_id);
  fetch_top_videos_by_views_for_ids_with_base_url(
    access_token,
    "https://youtubeanalytics.googleapis.com/",
    &ids_value,
    start_dt,
    end_dt,
    limit,
  )
  .await
}

pub fn youtube_analytics_error_to_vercel_error(err: YoutubeAnalyticsError) -> Error {
  Box::new(err) as Error
}

#[cfg(test)]
mod tests {
  use super::*;
  use bytes::Bytes;
  use http_body_util::Full;
  use hyper::body::Incoming;
  use hyper::service::service_fn;
  use hyper::{Request, Response, StatusCode};
  use hyper::server::conn::http1;
  use hyper_util::rt::TokioIo;
  use tokio::net::TcpListener;

  #[test]
  fn build_reports_url_includes_expected_params() {
    let start_dt = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end_dt = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
    let url = build_reports_url("https://youtubeanalytics.googleapis.com/", start_dt, end_dt);

    assert!(url.contains("/v2/reports?"));
    assert!(url.contains("ids=channel==MINE"));
    assert!(url.contains("startDate=2026-01-01"));
    assert!(url.contains("endDate=2026-01-07"));
    assert!(url.contains("metrics=estimatedRevenue,views"));
    assert!(url.contains("dimensions=day,video"));
  }

  #[test]
  fn build_reports_url_for_thumbnail_impressions_includes_expected_metrics() {
    let start_dt = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end_dt = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
    let url = build_reports_url_with_ids_impressions(
      "https://youtubeanalytics.googleapis.com/",
      "channel==MINE",
      start_dt,
      end_dt,
    );

    assert!(url.contains("metrics=videoThumbnailImpressions,videoThumbnailImpressionsClickRate"));
  }

  #[test]
  fn build_video_totals_url_includes_expected_params() {
    let start_dt = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end_dt = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
    let url = build_video_totals_url_with_ids_and_metrics(
      "https://youtubeanalytics.googleapis.com/",
      "channel==MINE",
      start_dt,
      end_dt,
      "views",
      "-views",
      10,
    );

    assert!(url.contains("dimensions=video"));
    assert!(url.contains("sort=-views"));
    assert!(url.contains("metrics=views"));
    assert!(url.contains("maxResults=10"));
  }

  #[test]
  fn build_channel_reports_url_includes_expected_params() {
    let start_dt = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end_dt = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
    let url = build_channel_reports_url("https://youtubeanalytics.googleapis.com/", start_dt, end_dt);

    assert!(url.contains("/v2/reports?"));
    assert!(url.contains("ids=channel==MINE"));
    assert!(url.contains("startDate=2026-01-01"));
    assert!(url.contains("endDate=2026-01-07"));
    assert!(url.contains("metrics=estimatedRevenue,views"));
    assert!(url.contains("dimensions=day&"));
  }

  #[test]
  fn build_reports_url_for_channel_includes_channel_id() {
    let start_dt = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end_dt = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
    let url = build_reports_url_for_channel(
      "https://youtubeanalytics.googleapis.com/",
      "UC123",
      start_dt,
      end_dt,
    );

    assert!(url.contains("ids=channel==UC123"));
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

  #[test]
  fn parse_rows_extracts_video_thumbnail_impressions() {
    let json: Value = serde_json::from_str(
      r#"
      {
        "columnHeaders": [
          {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
          {"name":"video","columnType":"DIMENSION","dataType":"STRING"},
          {"name":"videoThumbnailImpressions","columnType":"METRIC","dataType":"INTEGER"},
          {"name":"videoThumbnailImpressionsClickRate","columnType":"METRIC","dataType":"FLOAT"},
          {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
        ],
        "rows": [
          ["2026-01-02","vid1", 1000, 0.058, 200]
        ]
      }
    "#,
    )
    .unwrap();

    let rows = parse_rows(&json);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].video_id, "vid1");
    assert_eq!(rows[0].impressions, 1000);
    let ctr = rows[0].impressions_ctr.unwrap_or(-1.0);
    assert!((ctr - 0.058).abs() < 1e-9);
    assert_eq!(rows[0].views, 200);
  }

  #[test]
  fn parse_rows_channel_sets_synthetic_video_id() {
    let json: Value = serde_json::from_str(
      r#"
      {
        "columnHeaders": [
          {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
          {"name":"estimatedRevenue","columnType":"METRIC","dataType":"FLOAT"},
          {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
        ],
        "rows": [
          ["2026-01-02", 1.25, 200],
          ["2026-01-03", 0.0, 0]
        ]
      }
    "#,
    )
    .unwrap();

    let rows = parse_rows_channel(&json);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].video_id, FALLBACK_CHANNEL_VIDEO_ID);
    assert_eq!(rows[0].estimated_revenue_usd, 1.25);
    assert_eq!(rows[0].views, 200);
  }

  async fn serve_reports(listener: TcpListener, max_connections: usize) {
    for _ in 0..max_connections {
      let (stream, _) = listener.accept().await.unwrap();
      let io = TokioIo::new(stream);
      http1::Builder::new()
        .serve_connection(
          io,
          service_fn(|req: Request<Incoming>| async move {
            let query = req.uri().query().unwrap_or("");
            if query.contains("dimensions=day,video") && query.contains("metrics=estimatedRevenue,views") {
              let body = r#"{ "error": { "code": 400, "message": "The query is not supported.", "errors": [ { "message": "The query is not supported.", "domain": "global", "reason": "badRequest" } ] } }"#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::BAD_REQUEST)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            if query.contains("dimensions=day,video") && query.contains("metrics=views") {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"video","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
                  ],
                  "rows": [
                    ["2026-01-02","vid1", 200]
                  ]
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            if query.contains("dimensions=day") && query.contains("metrics=estimatedRevenue,views") {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"estimatedRevenue","columnType":"METRIC","dataType":"FLOAT"},
                    {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
                  ],
                  "rows": [
                    ["2026-01-02", 1.25, 200]
                  ]
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            Ok::<_, hyper::Error>(
              Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from_static(b"not found")))
                .unwrap(),
            )
          }),
        )
        .await
        .unwrap();
    }
  }

  #[tokio::test]
  async fn falls_back_to_views_only_video_report_when_query_not_supported() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}/", addr);

    let task = tokio::spawn(serve_reports(listener, 5));

    let start_dt = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end_dt = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
    let rows =
      fetch_video_daily_metrics_for_channel_with_base_url("token123", &base_url, "UC123", start_dt, end_dt)
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    let video = rows.iter().find(|r| r.video_id == "vid1").unwrap();
    assert_eq!(video.dt, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    assert_eq!(video.views, 200);
    assert_eq!(video.estimated_revenue_usd, 0.0);

    let total = rows
      .iter()
      .find(|r| r.video_id == FALLBACK_CHANNEL_VIDEO_ID)
      .unwrap();
    assert_eq!(total.dt, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    assert_eq!(total.views, 200);
    assert_eq!(total.estimated_revenue_usd, 1.25);

    task.await.unwrap();
  }

  async fn serve_reports_forbidden(listener: TcpListener, max_connections: usize) {
    for _ in 0..max_connections {
      let (stream, _) = listener.accept().await.unwrap();
      let io = TokioIo::new(stream);
      http1::Builder::new()
        .serve_connection(
          io,
          service_fn(|req: Request<Incoming>| async move {
            let query = req.uri().query().unwrap_or("");

            // Video-level request succeeds but returns no rows (forces channel-level fallback).
            if query.contains("dimensions=day,video") && query.contains("metrics=estimatedRevenue,views") {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"video","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"estimatedRevenue","columnType":"METRIC","dataType":"FLOAT"},
                    {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
                  ],
                  "rows": []
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            // Views-only video-level request also returns no rows (forces channel-level fallback).
            if query.contains("dimensions=day,video") && query.contains("metrics=views") {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"video","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
                  ],
                  "rows": []
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            // Channel-level request with revenue is forbidden.
            if query.contains("dimensions=day") && query.contains("metrics=estimatedRevenue,views") {
              let body = r#"{ "error": { "code": 403, "message": "Forbidden", "errors": [ { "message": "Forbidden", "domain": "global", "reason": "forbidden" } ] } }"#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::FORBIDDEN)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            // Views-only channel report works.
            if query.contains("dimensions=day") && query.contains("metrics=views") {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
                  ],
                  "rows": [
                    ["2026-01-02", 200]
                  ]
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            Ok::<_, hyper::Error>(
              Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from_static(b"not found")))
                .unwrap(),
            )
          }),
        )
        .await
        .unwrap();
    }
  }

  async fn serve_reports_empty_rows(listener: TcpListener, max_connections: usize) {
    for _ in 0..max_connections {
      let (stream, _) = listener.accept().await.unwrap();
      let io = TokioIo::new(stream);
      http1::Builder::new()
        .serve_connection(
          io,
          service_fn(|req: Request<Incoming>| async move {
            let query = req.uri().query().unwrap_or("");
            if query.contains("dimensions=day,video") && query.contains("metrics=estimatedRevenue,views") {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"video","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"estimatedRevenue","columnType":"METRIC","dataType":"FLOAT"},
                    {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
                  ],
                  "rows": []
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            if query.contains("dimensions=day,video") && query.contains("metrics=views") {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"video","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
                  ],
                  "rows": [
                    ["2026-01-02","vid1", 200]
                  ]
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            if query.contains("dimensions=day") && query.contains("metrics=estimatedRevenue,views") {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"estimatedRevenue","columnType":"METRIC","dataType":"FLOAT"},
                    {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
                  ],
                  "rows": [
                    ["2026-01-02", 1.25, 200]
                  ]
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            Ok::<_, hyper::Error>(
              Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from_static(b"not found")))
                .unwrap(),
            )
          }),
        )
        .await
        .unwrap();
    }
  }

  #[tokio::test]
  async fn falls_back_to_views_only_video_report_when_video_rows_empty() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}/", addr);

    let task = tokio::spawn(serve_reports_empty_rows(listener, 5));

    let start_dt = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end_dt = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
    let rows =
      fetch_video_daily_metrics_for_channel_with_base_url("token123", &base_url, "UC123", start_dt, end_dt)
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    let video = rows.iter().find(|r| r.video_id == "vid1").unwrap();
    assert_eq!(video.dt, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    assert_eq!(video.views, 200);
    assert_eq!(video.estimated_revenue_usd, 0.0);

    let total = rows
      .iter()
      .find(|r| r.video_id == FALLBACK_CHANNEL_VIDEO_ID)
      .unwrap();
    assert_eq!(total.dt, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    assert_eq!(total.views, 200);
    assert_eq!(total.estimated_revenue_usd, 1.25);

    task.await.unwrap();
  }

  #[tokio::test]
  async fn falls_back_to_views_only_channel_report_when_forbidden() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}/", addr);

    let task = tokio::spawn(serve_reports_forbidden(listener, 5));

    let start_dt = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end_dt = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
    let rows =
      fetch_video_daily_metrics_for_channel_with_base_url("token123", &base_url, "UC123", start_dt, end_dt)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].dt, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    assert_eq!(rows[0].video_id, FALLBACK_CHANNEL_VIDEO_ID);
    assert_eq!(rows[0].estimated_revenue_usd, 0.0);
    assert_eq!(rows[0].views, 200);

    task.await.unwrap();
  }

  async fn serve_reports_with_impressions(listener: TcpListener, max_connections: usize) {
    for _ in 0..max_connections {
      let (stream, _) = listener.accept().await.unwrap();
      let io = TokioIo::new(stream);
      http1::Builder::new()
        .serve_connection(
          io,
          service_fn(|req: Request<Incoming>| async move {
            let query = req.uri().query().unwrap_or("");

            if query.contains("dimensions=day,video") && query.contains("metrics=estimatedRevenue,views") {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"video","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"estimatedRevenue","columnType":"METRIC","dataType":"FLOAT"},
                    {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
                  ],
                  "rows": [
                    ["2026-01-02","vid1", 1.25, 200]
                  ]
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            if query.contains("dimensions=day,video")
              && query.contains("metrics=videoThumbnailImpressions,videoThumbnailImpressionsClickRate")
            {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"video","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"videoThumbnailImpressions","columnType":"METRIC","dataType":"INTEGER"},
                    {"name":"videoThumbnailImpressionsClickRate","columnType":"METRIC","dataType":"FLOAT"}
                  ],
                  "rows": [
                    ["2026-01-02","vid1", 1000, 0.06]
                  ]
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            if query.contains("dimensions=day") && query.contains("metrics=estimatedRevenue,views") {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"estimatedRevenue","columnType":"METRIC","dataType":"FLOAT"},
                    {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
                  ],
                  "rows": [
                    ["2026-01-02", 1.25, 200]
                  ]
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            if query.contains("dimensions=day")
              && query.contains("metrics=videoThumbnailImpressions,videoThumbnailImpressionsClickRate")
            {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"videoThumbnailImpressions","columnType":"METRIC","dataType":"INTEGER"},
                    {"name":"videoThumbnailImpressionsClickRate","columnType":"METRIC","dataType":"FLOAT"}
                  ],
                  "rows": [
                    ["2026-01-02", 1000, 0.06]
                  ]
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            Ok::<_, hyper::Error>(
              Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from_static(b"not found")))
                .unwrap(),
            )
          }),
        )
        .await
        .unwrap();
    }
  }

  async fn serve_reports_with_dimensionless_reach_fallback(listener: TcpListener, max_connections: usize) {
    for _ in 0..max_connections {
      let (stream, _) = listener.accept().await.unwrap();
      let io = TokioIo::new(stream);
      http1::Builder::new()
        .serve_connection(
          io,
          service_fn(|req: Request<Incoming>| async move {
            let query = req.uri().query().unwrap_or("");

            if query.contains("dimensions=day,video") && query.contains("metrics=estimatedRevenue,views") {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"video","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"estimatedRevenue","columnType":"METRIC","dataType":"FLOAT"},
                    {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
                  ],
                  "rows": [
                    ["2026-01-02","vid1", 1.25, 200]
                  ]
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            // Reach query not supported for daily breakdown in this mock.
            if query.contains("metrics=videoThumbnailImpressions,videoThumbnailImpressionsClickRate")
              && query.contains("dimensions=")
            {
              let body = r#"{ "error": { "code": 400, "message": "The query is not supported.", "errors": [ { "message": "The query is not supported.", "domain": "global", "reason": "badRequest" } ] } }"#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::BAD_REQUEST)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            if query.contains("dimensions=day") && query.contains("metrics=estimatedRevenue,views") {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"day","columnType":"DIMENSION","dataType":"STRING"},
                    {"name":"estimatedRevenue","columnType":"METRIC","dataType":"FLOAT"},
                    {"name":"views","columnType":"METRIC","dataType":"INTEGER"}
                  ],
                  "rows": [
                    ["2026-01-02", 1.25, 200]
                  ]
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            // Dimensionless reach report for a single day (startDate=endDate).
            if query.contains("metrics=videoThumbnailImpressions,videoThumbnailImpressionsClickRate")
              && query.contains("startDate=2026-01-02")
              && query.contains("endDate=2026-01-02")
              && !query.contains("dimensions=")
            {
              let body = r#"
                {
                  "columnHeaders": [
                    {"name":"videoThumbnailImpressions","columnType":"METRIC","dataType":"INTEGER"},
                    {"name":"videoThumbnailImpressionsClickRate","columnType":"METRIC","dataType":"FLOAT"}
                  ],
                  "rows": [
                    [1000, 0.06]
                  ]
                }
              "#;
              return Ok::<_, hyper::Error>(
                Response::builder()
                  .status(StatusCode::OK)
                  .header("content-type", "application/json")
                  .body(Full::new(Bytes::from(body)))
                  .unwrap(),
              );
            }

            Ok::<_, hyper::Error>(
              Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from_static(b"not found")))
                .unwrap(),
            )
          }),
        )
        .await
        .unwrap();
    }
  }

  #[tokio::test]
  async fn fills_impressions_via_separate_report() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}/", addr);

    let task = tokio::spawn(serve_reports_with_impressions(listener, 4));

    let start_dt = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end_dt = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
    let rows =
      fetch_video_daily_metrics_for_channel_with_base_url("token123", &base_url, "UC123", start_dt, end_dt)
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);

    let video = rows.iter().find(|r| r.video_id == "vid1").unwrap();
    assert_eq!(video.dt, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    assert_eq!(video.views, 200);
    assert_eq!(video.impressions, 1000);
    let ctr = video.impressions_ctr.unwrap_or(-1.0);
    assert!((ctr - 0.06).abs() < 1e-9);
    assert_eq!(video.estimated_revenue_usd, 1.25);

    let total = rows
      .iter()
      .find(|r| r.video_id == FALLBACK_CHANNEL_VIDEO_ID)
      .unwrap();
    assert_eq!(total.dt, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    assert_eq!(total.views, 200);
    assert_eq!(total.impressions, 1000);
    let ctr = total.impressions_ctr.unwrap_or(-1.0);
    assert!((ctr - 0.06).abs() < 1e-9);
    assert_eq!(total.estimated_revenue_usd, 1.25);

    task.await.unwrap();
  }

  #[tokio::test]
  async fn skips_reach_metrics_when_daily_reach_query_not_supported() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}/", addr);

    // Expect 4 report fetches: video report, video reach (fails), channel totals, channel reach (fails).
    let task = tokio::spawn(serve_reports_with_dimensionless_reach_fallback(listener, 4));

    let start_dt = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
    let end_dt = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
    let rows =
      fetch_video_daily_metrics_for_channel_with_base_url("token123", &base_url, "UC123", start_dt, end_dt)
        .await
        .unwrap();

    let total = rows
      .iter()
      .find(|r| r.video_id == FALLBACK_CHANNEL_VIDEO_ID)
      .unwrap();
    // Reach metrics are not reliably supported by the YouTube Analytics API for all accounts.
    // When the query is not supported, we keep sync fast and treat reach as unavailable.
    assert_eq!(total.impressions, 0);
    assert!(total.impressions_ctr.is_none());

    task.await.unwrap();
  }
}

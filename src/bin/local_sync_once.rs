use chrono::{Duration, NaiveDate, Utc};
use vercel_runtime::Error;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::header::{ACCEPT, AUTHORIZATION};
use hyper::{Method, Request as HyperRequest, StatusCode};

use globa_flux_rust::db::{
  fetch_or_seed_youtube_oauth_app_config, fetch_youtube_channel_id,
  fetch_youtube_connection_tokens, get_pool, update_youtube_connection_tokens,
  upsert_video_daily_metric,
};
use globa_flux_rust::providers::youtube_api::fetch_my_channel_id;
use globa_flux_rust::providers::youtube::{refresh_tokens, youtube_oauth_client_from_config};
use globa_flux_rust::providers::youtube_analytics::{
  fetch_video_daily_metrics, fetch_video_daily_metrics_for_channel,
};

async fn fetch_report_json_by_url(access_token: &str, url: &str) -> Result<serde_json::Value, Error> {
  let connector = hyper_rustls::HttpsConnectorBuilder::new()
    .with_native_roots()
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?
    .https_or_http()
    .enable_http1()
    .build();

  let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
    .build(connector);

  let req = HyperRequest::builder()
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

  if status != StatusCode::OK {
    let snippet = String::from_utf8_lossy(&body_bytes);
    let snippet = snippet.chars().take(800).collect::<String>();
    return Err(Box::new(std::io::Error::other(format!(
      "YouTube Analytics HTTP {}: {}",
      status.as_u16(),
      snippet
    ))) as Error);
  }

  serde_json::from_slice::<serde_json::Value>(&body_bytes)
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)
}

fn validate_database_url() -> Result<(), Error> {
  let url = std::env::var("TIDB_DATABASE_URL")
    .or_else(|_| std::env::var("DATABASE_URL"))
    .unwrap_or_default();
  let trimmed = url.trim();
  if trimmed.is_empty() {
    return Err(Box::new(std::io::Error::other(
      "Missing TIDB_DATABASE_URL (or DATABASE_URL)",
    )) as Error);
  }
  if !trimmed.contains("://") {
    return Err(Box::new(std::io::Error::other(
      "Invalid TIDB_DATABASE_URL/DATABASE_URL (expected URL scheme like mysql://...)",
    )) as Error);
  }
  Ok(())
}

fn parse_flag_value(args: &[String], flag: &str) -> Option<String> {
  args
    .iter()
    .position(|a| a == flag)
    .and_then(|idx| args.get(idx + 1))
    .cloned()
}

fn parse_dt(input: &str) -> Option<NaiveDate> {
  NaiveDate::parse_from_str(input.trim(), "%Y-%m-%d").ok()
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  validate_database_url()?;
  let args: Vec<String> = std::env::args().collect();

  let tenant_id = parse_flag_value(&args, "--tenant-id")
    .or_else(|| parse_flag_value(&args, "--tenant"))
    .unwrap_or_default();
  if tenant_id.trim().is_empty() {
    eprintln!("Missing required --tenant-id");
    eprintln!("Example: cargo run --bin local_sync_once -- --tenant-id gid://shopify/Customer/123 --days 14");
    return Ok(());
  }

  let channel_id_arg = parse_flag_value(&args, "--channel-id").or_else(|| parse_flag_value(&args, "--channel"));
  let days_arg = parse_flag_value(&args, "--days").and_then(|v| v.parse::<i64>().ok());
  let start_arg = parse_flag_value(&args, "--start-dt")
    .or_else(|| parse_flag_value(&args, "--start"))
    .and_then(|v| parse_dt(&v));
  let end_arg = parse_flag_value(&args, "--end-dt")
    .or_else(|| parse_flag_value(&args, "--end"))
    .and_then(|v| parse_dt(&v));

  let today = Utc::now().date_naive();
  let end_dt = end_arg.unwrap_or_else(|| today - Duration::days(1));
  let days = days_arg.unwrap_or(14).clamp(1, 365);
  let start_dt = start_arg.unwrap_or_else(|| end_dt - Duration::days(days));

  if start_dt > end_dt {
    eprintln!("Invalid range: start_dt ({start_dt}) > end_dt ({end_dt})");
    return Ok(());
  }

  let pool = get_pool().await?;
  let channel_id = match channel_id_arg
    .as_deref()
    .map(str::trim)
    .filter(|v| !v.is_empty())
  {
    Some(v) => v.to_string(),
    None => fetch_youtube_channel_id(pool, tenant_id.trim())
      .await?
      .unwrap_or_default(),
  };

  if channel_id.trim().is_empty() {
    eprintln!("No active YouTube channel for tenant. Pass --channel-id or reconnect OAuth.");
    return Ok(());
  }

  let before_rows: i64 = sqlx::query_scalar(
    r#"
      SELECT CAST(COUNT(*) AS SIGNED) AS n
      FROM video_daily_metrics
      WHERE tenant_id = ?
        AND channel_id = ?
        AND dt BETWEEN ? AND ?;
    "#,
  )
  .bind(tenant_id.trim())
  .bind(channel_id.trim())
  .bind(start_dt)
  .bind(end_dt)
  .fetch_one(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let mut tokens = match fetch_youtube_connection_tokens(pool, tenant_id.trim(), channel_id.trim()).await? {
    Some(v) => v,
    None => {
      eprintln!("Missing YouTube connection tokens for tenant/channel. Reconnect OAuth first.");
      return Ok(());
    }
  };

  match fetch_my_channel_id(&tokens.access_token).await {
    Ok(token_channel_id) => {
      if token_channel_id != channel_id.trim() {
        eprintln!(
          "warning: token channel_id mismatch (token_mine={} requested={})",
          token_channel_id,
          channel_id.trim()
        );
      } else {
        println!("token_mine_channel_id={}", token_channel_id);
      }
    }
    Err(_) => {
      eprintln!("warning: failed to fetch token mine channel_id via YouTube Data API");
    }
  }

  // Compare `ids=channel==MINE` vs `ids=channel==<channel_id>` for debugging.
  let mine_rows = fetch_video_daily_metrics(&tokens.access_token, start_dt, end_dt)
    .await
    .unwrap_or_default();
  let mut mine_min: Option<NaiveDate> = None;
  let mut mine_max: Option<NaiveDate> = None;
  for r in mine_rows.iter() {
    mine_min = Some(mine_min.map(|d| d.min(r.dt)).unwrap_or(r.dt));
    mine_max = Some(mine_max.map(|d| d.max(r.dt)).unwrap_or(r.dt));
  }
  println!(
    "debug_mine fetched_rows={} fetched_min_dt={} fetched_max_dt={}",
    mine_rows.len(),
    mine_min.map(|d| d.to_string()).unwrap_or_else(|| "null".to_string()),
    mine_max.map(|d| d.to_string()).unwrap_or_else(|| "null".to_string()),
  );

  // Raw report sanity check: does views-only return more rows than revenue+views?
  let base = "https://youtubeanalytics.googleapis.com";
  let ids_value = format!("channel=={}", channel_id.trim());
  let url_views_only = format!(
    "{base}/v2/reports?ids={ids_value}&startDate={start_dt}&endDate={end_dt}&metrics=views&dimensions=day&sort=day&maxResults=200"
  );
  let url_rev_views = format!(
    "{base}/v2/reports?ids={ids_value}&startDate={start_dt}&endDate={end_dt}&metrics=estimatedRevenue,views&dimensions=day&sort=day&maxResults=200"
  );
  let rows_len = |json: &serde_json::Value| {
    json
      .get("rows")
      .and_then(|v| v.as_array())
      .map(|a| a.len())
      .unwrap_or(0)
  };
  let first_day = |json: &serde_json::Value| {
    json
      .get("rows")
      .and_then(|v| v.as_array())
      .and_then(|a| a.first())
      .and_then(|r| r.as_array())
      .and_then(|arr| arr.first())
      .and_then(|v| v.as_str())
      .unwrap_or("null")
      .to_string()
  };
  let last_day = |json: &serde_json::Value| {
    json
      .get("rows")
      .and_then(|v| v.as_array())
      .and_then(|a| a.last())
      .and_then(|r| r.as_array())
      .and_then(|arr| arr.first())
      .and_then(|v| v.as_str())
      .unwrap_or("null")
      .to_string()
  };

  let views_res = fetch_report_json_by_url(&tokens.access_token, &url_views_only).await;
  let rev_res = fetch_report_json_by_url(&tokens.access_token, &url_rev_views).await;

  if let Ok(views_json) = &views_res {
    println!(
      "debug_raw views_only_rows={} first_day={} last_day={}",
      rows_len(views_json),
      first_day(views_json),
      last_day(views_json),
    );
  }
  if let Ok(rev_views_json) = &rev_res {
    println!(
      "debug_raw rev_views_rows={} first_day={} last_day={}",
      rows_len(rev_views_json),
      first_day(rev_views_json),
      last_day(rev_views_json),
    );
  }

  if views_res.is_err() || rev_res.is_err() {
    println!(
      "debug_raw_error views_only_err={} rev_views_err={}",
      views_res.err().map(|e| e.to_string()).unwrap_or_else(|| "null".to_string()),
      rev_res.err().map(|e| e.to_string()).unwrap_or_else(|| "null".to_string()),
    );
  }

  // Best-effort refresh before fetch if expired.
  let now_dt = Utc::now();
  let needs_refresh = tokens.expires_at.map(|t| t <= now_dt).unwrap_or(false);
  if needs_refresh {
    if let Some(refresh) = tokens.refresh_token.clone() {
      let app = fetch_or_seed_youtube_oauth_app_config(pool, tenant_id.trim())
        .await?
        .ok_or_else(|| Box::new(std::io::Error::other("missing youtube oauth app config")) as Error)?;
      let client_secret = app
        .client_secret
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Box::new(std::io::Error::other("missing youtube oauth client_secret")) as Error)?;
      let (client, _redirect) =
        youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
      let refreshed = refresh_tokens(&client, &refresh).await?;
      update_youtube_connection_tokens(pool, tenant_id.trim(), channel_id.trim(), &refreshed).await?;
      tokens.access_token = refreshed.access_token;
      tokens.refresh_token = refreshed.refresh_token.or(Some(refresh));
    }
  }

  let metrics = match fetch_video_daily_metrics_for_channel(&tokens.access_token, &channel_id, start_dt, end_dt).await {
    Ok(v) => v,
    Err(err) if err.status == Some(401) => {
      if let Some(refresh) = tokens.refresh_token.clone() {
        let app = fetch_or_seed_youtube_oauth_app_config(pool, tenant_id.trim())
          .await?
          .ok_or_else(|| Box::new(std::io::Error::other("missing youtube oauth app config")) as Error)?;
        let client_secret = app
          .client_secret
          .as_deref()
          .map(str::trim)
          .filter(|v| !v.is_empty())
          .ok_or_else(|| Box::new(std::io::Error::other("missing youtube oauth client_secret")) as Error)?;
        let (client, _redirect) =
          youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
        let refreshed = refresh_tokens(&client, &refresh).await?;
        update_youtube_connection_tokens(pool, tenant_id.trim(), channel_id.trim(), &refreshed).await?;
        tokens.access_token = refreshed.access_token;
        fetch_video_daily_metrics_for_channel(&tokens.access_token, &channel_id, start_dt, end_dt)
          .await
          .map_err(|e| -> Error { Box::new(e) })?
      } else {
        return Err(Box::new(err) as Error);
      }
    }
    Err(err) => return Err(Box::new(err) as Error),
  };

  let mut upserts = 0usize;
  let mut min_dt: Option<NaiveDate> = None;
  let mut max_dt: Option<NaiveDate> = None;
  for row in metrics.iter() {
    min_dt = Some(min_dt.map(|d| d.min(row.dt)).unwrap_or(row.dt));
    max_dt = Some(max_dt.map(|d| d.max(row.dt)).unwrap_or(row.dt));
    upsert_video_daily_metric(
      pool,
      tenant_id.trim(),
      channel_id.trim(),
      row.dt,
      &row.video_id,
      row.estimated_revenue_usd,
      row.impressions,
      row.views,
    )
    .await?;
    upserts += 1;
  }

  let after_rows: i64 = sqlx::query_scalar(
    r#"
      SELECT CAST(COUNT(*) AS SIGNED) AS n
      FROM video_daily_metrics
      WHERE tenant_id = ?
        AND channel_id = ?
        AND dt BETWEEN ? AND ?;
    "#,
  )
  .bind(tenant_id.trim())
  .bind(channel_id.trim())
  .bind(start_dt)
  .bind(end_dt)
  .fetch_one(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let (max_dt, sum_views, sum_revenue): (Option<NaiveDate>, i64, f64) = sqlx::query_as(
    r#"
      SELECT MAX(dt) AS max_dt,
             CAST(COALESCE(SUM(views), 0) AS SIGNED) AS sum_views,
             CAST(COALESCE(SUM(estimated_revenue_usd), 0) AS DOUBLE) AS sum_revenue
      FROM video_daily_metrics
      WHERE tenant_id = ?
        AND channel_id = ?
        AND dt BETWEEN ? AND ?;
    "#,
  )
  .bind(tenant_id.trim())
  .bind(channel_id.trim())
  .bind(start_dt)
  .bind(end_dt)
  .fetch_one(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  println!(
    "ok=true tenant_id={} channel_id={} start_dt={} end_dt={} fetched_rows={} upserts={} fetched_min_dt={} fetched_max_dt={} tidb_rows_before={} tidb_rows_after={} tidb_max_dt={}",
    tenant_id.trim(),
    channel_id.trim(),
    start_dt,
    end_dt,
    metrics.len(),
    upserts,
    min_dt.map(|d| d.to_string()).unwrap_or_else(|| "null".to_string()),
    max_dt.map(|d| d.to_string()).unwrap_or_else(|| "null".to_string()),
    before_rows,
    after_rows,
    max_dt.map(|d| d.to_string()).unwrap_or_else(|| "null".to_string()),
  );
  println!("tidb_sum_views={} tidb_sum_revenue_usd={}", sum_views, sum_revenue);

  if let Some(sample) = metrics.last() {
    println!(
      "sample dt={} video_id={} views={} revenue_usd={}",
      sample.dt, sample.video_id, sample.views, sample.estimated_revenue_usd
    );
  }

  Ok(())
}

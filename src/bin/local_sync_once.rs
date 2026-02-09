use chrono::{Duration, NaiveDate, Utc};
use vercel_runtime::Error;

use globa_flux_rust::http_client::http_client_for_url;

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
use globa_flux_rust::reach_reporting::ingest_channel_reach_basic_a1;

async fn fetch_report_json_by_url(access_token: &str, url: &str) -> Result<serde_json::Value, Error> {
  let client = http_client_for_url(url)
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  let resp = client
    .get(url)
    .bearer_auth(access_token)
    .header(reqwest::header::ACCEPT, "application/json")
    .send()
    .await
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  let status = resp.status();
  let body = resp
    .text()
    .await
    .unwrap_or_else(|e| format!("<failed to read body: {e}>"));

  if !status.is_success() {
    let snippet = body.chars().take(800).collect::<String>();
    return Err(Box::new(std::io::Error::other(format!(
      "YouTube Analytics HTTP {}: {}",
      status.as_u16(),
      snippet
    ))) as Error);
  }

  serde_json::from_str::<serde_json::Value>(&body)
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

  // Best-effort refresh before ANY API calls if expired.
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
  let url_thumb = format!(
    "{base}/v2/reports?ids={ids_value}&startDate={start_dt}&endDate={end_dt}&metrics=videoThumbnailImpressions,videoThumbnailImpressionsClickRate&dimensions=day&sort=day&maxResults=200"
  );
  let url_thumb_video = format!(
    "{base}/v2/reports?ids={ids_value}&startDate={start_dt}&endDate={end_dt}&metrics=videoThumbnailImpressions,videoThumbnailImpressionsClickRate&dimensions=day,video&sort=day&maxResults=200"
  );
  let url_thumb_video_only = format!(
    "{base}/v2/reports?ids={ids_value}&startDate={start_dt}&endDate={end_dt}&metrics=videoThumbnailImpressions,videoThumbnailImpressionsClickRate&dimensions=video&sort=-videoThumbnailImpressions&maxResults=50"
  );
  let url_thumb_traffic_source = format!(
    "{base}/v2/reports?ids={ids_value}&startDate={start_dt}&endDate={end_dt}&metrics=videoThumbnailImpressions,videoThumbnailImpressionsClickRate&dimensions=insightTrafficSourceType&sort=-videoThumbnailImpressions&maxResults=50"
  );
  let url_thumb_no_dims_one_day = format!(
    "{base}/v2/reports?ids={ids_value}&startDate={end_dt}&endDate={end_dt}&metrics=videoThumbnailImpressions,videoThumbnailImpressionsClickRate&maxResults=1"
  );
  let url_thumb_no_dims_one_day_with_views = format!(
    "{base}/v2/reports?ids={ids_value}&startDate={end_dt}&endDate={end_dt}&metrics=views,videoThumbnailImpressions,videoThumbnailImpressionsClickRate&maxResults=1"
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
  let thumb_res = fetch_report_json_by_url(&tokens.access_token, &url_thumb).await;
  let thumb_video_res = fetch_report_json_by_url(&tokens.access_token, &url_thumb_video).await;
  let thumb_video_only_res = fetch_report_json_by_url(&tokens.access_token, &url_thumb_video_only).await;
  let thumb_traffic_res =
    fetch_report_json_by_url(&tokens.access_token, &url_thumb_traffic_source).await;
  let thumb_no_dims_one_day_res =
    fetch_report_json_by_url(&tokens.access_token, &url_thumb_no_dims_one_day).await;
  let thumb_no_dims_one_day_with_views_res =
    fetch_report_json_by_url(&tokens.access_token, &url_thumb_no_dims_one_day_with_views).await;

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
  if let Ok(impr_json) = &thumb_res {
    println!(
      "debug_raw thumb_rows={} first_day={} last_day={}",
      rows_len(impr_json),
      first_day(impr_json),
      last_day(impr_json),
    );
  }
  if let Ok(impr_video_json) = &thumb_video_res {
    println!(
      "debug_raw thumb_video_rows={} first_day={} last_day={}",
      rows_len(impr_video_json),
      first_day(impr_video_json),
      last_day(impr_video_json),
    );
  }
  if let Ok(impr_video_only_json) = &thumb_video_only_res {
    let n = rows_len(impr_video_only_json);
    println!("debug_raw thumb_video_only_rows={}", n);
  }
  if let Ok(json) = &thumb_traffic_res {
    println!(
      "debug_raw thumb_traffic_rows={} first_day={} last_day={}",
      rows_len(json),
      first_day(json),
      last_day(json),
    );
  }
  if let Ok(json) = &thumb_no_dims_one_day_res {
    println!(
      "debug_raw thumb_no_dims_one_day_rows={} first_day={} last_day={}",
      rows_len(json),
      first_day(json),
      last_day(json),
    );
  }
  if let Ok(json) = &thumb_no_dims_one_day_with_views_res {
    println!(
      "debug_raw thumb_no_dims_one_day_with_views_rows={} first_day={} last_day={}",
      rows_len(json),
      first_day(json),
      last_day(json),
    );
  }

  if views_res.is_err() || rev_res.is_err() {
    println!(
      "debug_raw_error views_only_err={} rev_views_err={}",
      views_res.err().map(|e| e.to_string()).unwrap_or_else(|| "null".to_string()),
      rev_res.err().map(|e| e.to_string()).unwrap_or_else(|| "null".to_string()),
    );
  }
  if thumb_res.is_err() || thumb_video_res.is_err() || thumb_video_only_res.is_err() {
    println!(
      "debug_raw_error thumb_err={} thumb_video_err={} thumb_video_only_err={}",
      thumb_res.err().map(|e| e.to_string()).unwrap_or_else(|| "null".to_string()),
      thumb_video_res.err().map(|e| e.to_string()).unwrap_or_else(|| "null".to_string()),
      thumb_video_only_res.err().map(|e| e.to_string()).unwrap_or_else(|| "null".to_string()),
    );
  }
  if thumb_no_dims_one_day_res.is_err() {
    println!(
      "debug_raw_error thumb_no_dims_one_day_err={}",
      thumb_no_dims_one_day_res
        .err()
        .map(|e| e.to_string())
        .unwrap_or_else(|| "null".to_string()),
    );
  }
  if thumb_no_dims_one_day_with_views_res.is_err() {
    println!(
      "debug_raw_error thumb_no_dims_one_day_with_views_err={}",
      thumb_no_dims_one_day_with_views_res
        .err()
        .map(|e| e.to_string())
        .unwrap_or_else(|| "null".to_string()),
    );
  }
  if thumb_traffic_res.is_err() {
    println!(
      "debug_raw_error thumb_traffic_err={}",
      thumb_traffic_res
        .err()
        .map(|e| e.to_string())
        .unwrap_or_else(|| "null".to_string()),
    );
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
      row.impressions_ctr,
      row.views,
    )
    .await?;
    upserts += 1;
  }

  match ingest_channel_reach_basic_a1(
    pool,
    tenant_id.trim(),
    channel_id.trim(),
    &tokens.access_token,
    start_dt,
    end_dt,
  )
  .await
  {
    Ok(summary) => {
      println!(
        "reach_ingest ok=true report_type_id={} job_id={} reports_listed={} reports_selected={} reports_downloaded={} rows_upserted={}",
        summary.report_type_id,
        summary.job_id,
        summary.reports_listed,
        summary.reports_selected,
        summary.reports_downloaded,
        summary.rows_upserted
      );
    }
    Err(err) => {
      eprintln!("reach_ingest ok=false err={}", err);
    }
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

  let (max_dt, sum_views, sum_impressions, sum_revenue): (Option<NaiveDate>, i64, i64, f64) = sqlx::query_as(
    r#"
      SELECT MAX(dt) AS max_dt,
             CAST(COALESCE(SUM(views), 0) AS SIGNED) AS sum_views,
             CAST(COALESCE(SUM(impressions), 0) AS SIGNED) AS sum_impressions,
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
  println!(
    "tidb_sum_views={} tidb_sum_impressions={} tidb_sum_revenue_usd={}",
    sum_views, sum_impressions, sum_revenue
  );

  let (rows_with_ctr,): (i64,) = sqlx::query_as(
    r#"
      SELECT CAST(COALESCE(SUM(CASE WHEN impressions_ctr IS NOT NULL THEN 1 ELSE 0 END), 0) AS SIGNED) AS rows_with_ctr
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

  println!("tidb_rows_with_ctr={}", rows_with_ctr);

  if let Some(sample) = metrics.last() {
    println!(
      "sample dt={} video_id={} views={} impressions={} impressions_ctr={:?} revenue_usd={}",
      sample.dt,
      sample.video_id,
      sample.views,
      sample.impressions,
      sample.impressions_ctr,
      sample.estimated_revenue_usd
    );
  }

  Ok(())
}

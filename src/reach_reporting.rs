use chrono::{Duration, NaiveDate, SecondsFormat, Utc};
use vercel_runtime::Error;

use crate::db::upsert_video_daily_reach_metrics;
use crate::providers::youtube_reporting::{
  download_report_file, ensure_job_for_report_type_channel, list_reports_channel,
};

#[derive(Debug, Clone)]
pub struct ReachIngestSummary {
  pub report_type_id: String,
  pub job_id: String,
  pub reports_listed: usize,
  pub reports_selected: usize,
  pub reports_downloaded: usize,
  pub rows_upserted: usize,
}

fn normalize_header_name(input: &str) -> String {
  let mut out = String::with_capacity(input.len());
  let mut last_was_sep = false;
  for ch in input.trim().chars() {
    if ch.is_ascii_alphanumeric() {
      out.push(ch.to_ascii_lowercase());
      last_was_sep = false;
    } else if !last_was_sep {
      out.push('_');
      last_was_sep = true;
    }
  }
  out.trim_matches('_').to_string()
}

fn parse_i64_field(raw: &str) -> Option<i64> {
  let cleaned = raw.trim().replace(',', "");
  cleaned.parse::<i64>().ok()
}

fn parse_f64_field(raw: &str) -> Option<f64> {
  let cleaned = raw.trim().replace(',', "");
  cleaned.parse::<f64>().ok()
}

fn maybe_gunzip_bytes(input: &[u8]) -> Result<Vec<u8>, std::io::Error> {
  use std::io::Read;

  let is_gzip = input.len() >= 2 && input[0] == 0x1f && input[1] == 0x8b;
  if !is_gzip {
    return Ok(input.to_vec());
  }

  let mut decoder = flate2::read::GzDecoder::new(input);
  let mut out = Vec::new();
  decoder.read_to_end(&mut out)?;
  Ok(out)
}

fn rfc3339_created_after(start_dt: NaiveDate, backfill_days: i64) -> String {
  let dt = chrono::DateTime::<Utc>::from_naive_utc_and_offset(
    start_dt.and_hms_opt(0, 0, 0).unwrap(),
    Utc,
  ) - Duration::days(backfill_days);

  dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub async fn ingest_channel_reach_basic_a1(
  pool: &sqlx::MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  access_token: &str,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> Result<ReachIngestSummary, Error> {
  if start_dt > end_dt {
    return Err(Box::new(std::io::Error::other("start_dt must be <= end_dt")) as Error);
  }

  let report_type_id = "channel_reach_basic_a1".to_string();
  let job_id = ensure_job_for_report_type_channel(access_token, &report_type_id)
    .await
    .map_err(|e| Box::new(e) as Error)?;

  let created_after = rfc3339_created_after(start_dt, 90);
  let reports = list_reports_channel(access_token, &job_id, Some(&created_after))
    .await
    .map_err(|e| Box::new(e) as Error)?;

  // Pick the latest report per day (some days can be regenerated).
  let mut by_dt: std::collections::BTreeMap<NaiveDate, (Option<String>, String)> = std::collections::BTreeMap::new();
  for r in reports.iter() {
    let Some(start_time) = r.start_time.as_deref() else {
      continue;
    };
    let Ok(start_time) = chrono::DateTime::parse_from_rfc3339(start_time) else {
      continue;
    };
    let dt = start_time.with_timezone(&Utc).date_naive();
    if dt < start_dt || dt > end_dt {
      continue;
    }

    let download_url = match r.download_url.as_deref() {
      Some(v) if !v.trim().is_empty() => v.trim().to_string(),
      _ => continue,
    };

    let create_time = r.create_time.clone();

    let replace = match by_dt.get(&dt) {
      None => true,
      Some((existing_create_time, _existing_url)) => {
        // Prefer the newest create_time when available.
        match (&create_time, existing_create_time) {
          (Some(new), Some(old)) => new > old,
          (Some(_), None) => true,
          _ => false,
        }
      }
    };

    if replace {
      by_dt.insert(dt, (create_time, download_url));
    }
  }

  let mut reports_downloaded = 0usize;
  let mut rows_upserted = 0usize;

  // Aggregate channel totals while parsing per-video rows.
  let mut totals_by_dt: std::collections::BTreeMap<NaiveDate, (i64, i64, f64)> = std::collections::BTreeMap::new();

  for (dt, (_create_time, download_url)) in by_dt.iter() {
    let bytes = download_report_file(access_token, download_url)
      .await
      .map_err(|e| Box::new(e) as Error)?;
    reports_downloaded += 1;

    let decoded = maybe_gunzip_bytes(&bytes).map_err(|e| Box::new(e) as Error)?;

    let mut rdr = csv::ReaderBuilder::new()
      .has_headers(true)
      .flexible(true)
      .from_reader(decoded.as_slice());

    let headers = rdr
      .headers()
      .map_err(|e| Box::new(std::io::Error::other(format!("invalid reporting csv headers: {e}"))) as Error)?
      .clone();

    let mut idx: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (i, h) in headers.iter().enumerate() {
      idx.insert(normalize_header_name(h), i);
    }

    let find_idx = |candidates: &[&str]| -> Option<usize> {
      for c in candidates {
        if let Some(i) = idx.get(*c) {
          return Some(*i);
        }
      }
      None
    };

    let dt_idx = find_idx(&["date", "day"]);
    let video_idx = find_idx(&["video_id", "video"]);
    let views_idx = find_idx(&["views", "view"]);
    let impressions_idx = find_idx(&["video_thumbnail_impressions"]);
    let ctr_idx = find_idx(&["video_thumbnail_impressions_ctr", "video_thumbnail_impressions_click_through_rate"]);

    if dt_idx.is_none() || video_idx.is_none() || views_idx.is_none() || impressions_idx.is_none() {
      return Err(Box::new(std::io::Error::other(
        "reach report missing required columns (date/video_id/views/video_thumbnail_impressions)",
      )) as Error);
    }

    for (row_i, rec) in rdr.records().enumerate() {
      let rec = rec.map_err(|e| {
        Box::new(std::io::Error::other(format!("invalid reach csv row {}: {}", row_i + 1, e))) as Error
      })?;

      let row_dt_raw = rec.get(dt_idx.unwrap()).unwrap_or("").trim();
      let row_dt = NaiveDate::parse_from_str(row_dt_raw, "%Y-%m-%d").unwrap_or(*dt);
      if row_dt < start_dt || row_dt > end_dt {
        continue;
      }

      let video_id = rec
        .get(video_idx.unwrap())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "__CHANNEL_TOTAL__".to_string());

      let views = rec
        .get(views_idx.unwrap())
        .and_then(parse_i64_field)
        .unwrap_or(0)
        .max(0);

      let impressions = rec
        .get(impressions_idx.unwrap())
        .and_then(parse_i64_field)
        .unwrap_or(0)
        .max(0);

      let impressions_ctr = ctr_idx
        .and_then(|i| rec.get(i))
        .and_then(parse_f64_field);

      upsert_video_daily_reach_metrics(
        pool,
        tenant_id,
        channel_id,
        row_dt,
        &video_id,
        impressions,
        impressions_ctr,
        views,
      )
      .await?;
      rows_upserted += 1;

      if impressions > 0 {
        let entry = totals_by_dt.entry(row_dt).or_insert((0, 0, 0.0));
        entry.0 += views;
        entry.1 += impressions;
        if let Some(ctr) = impressions_ctr {
          entry.2 += ctr * (impressions as f64);
        }
      } else {
        let entry = totals_by_dt.entry(row_dt).or_insert((0, 0, 0.0));
        entry.0 += views;
      }
    }
  }

  // Ensure we have a channel-total row per day for faster dashboard queries.
  for (dt, (views_sum, impr_sum, ctr_weighted_sum)) in totals_by_dt.into_iter() {
    let blended_ctr = if impr_sum > 0 {
      Some(ctr_weighted_sum / (impr_sum as f64))
    } else {
      None
    };
    upsert_video_daily_reach_metrics(
      pool,
      tenant_id,
      channel_id,
      dt,
      "__CHANNEL_TOTAL__",
      impr_sum,
      blended_ctr,
      views_sum,
    )
    .await?;
  }

  Ok(ReachIngestSummary {
    report_type_id,
    job_id,
    reports_listed: reports.len(),
    reports_selected: by_dt.len(),
    reports_downloaded,
    rows_upserted,
  })
}

use bytes::Bytes;
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode};
use serde::Deserialize;
use sha2::Digest;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::db::{
  decision_daily_exists,
  ensure_geo_monitor_run,
  fetch_geo_monitor_project,
  fetch_geo_monitor_prompt,
  fetch_new_video_publish_counts_by_dt,
  fetch_policy_params_json,
  fetch_revenue_sum_usd_7d,
  fetch_tenant_gemini_model,
  fetch_top_video_ids_by_revenue,
  fetch_youtube_channel_id,
  fetch_youtube_connection_tokens,
  fetch_or_seed_youtube_oauth_app_config,
  get_pool,
  finalize_geo_monitor_run_if_complete,
  insert_geo_monitor_run_result,
  insert_usage_event,
  update_youtube_connection_tokens,
  upsert_decision_outcome,
  upsert_observed_action,
  upsert_policy_eval_report,
  upsert_policy_params,
  upsert_video_daily_metric,
};
use globa_flux_rust::decision_engine::{compute_decision, DecisionEngineConfig};
use globa_flux_rust::outcome_engine::compute_outcome_label;
use globa_flux_rust::providers::gemini::{
  generate_text as gemini_generate_text, pricing_for_model as gemini_pricing_for_model, GeminiConfig,
};
use globa_flux_rust::providers::youtube::{refresh_tokens, youtube_oauth_client_from_config};
use globa_flux_rust::providers::youtube_analytics::{
  fetch_video_daily_metrics_for_channel, youtube_analytics_error_to_vercel_error,
};
use globa_flux_rust::providers::youtube_videos::{
  set_video_thumbnail_from_url, update_video_publish_at, update_video_title,
};
use globa_flux_rust::providers::youtube_reporting::{
  download_report_file,
  ensure_job_for_report_type,
  list_report_types,
  list_reports,
};
use globa_flux_rust::youtube_alerts::evaluate_youtube_alerts;
use globa_flux_rust::{
  cost::compute_cost_usd,
  geo_monitor::{
    contains_any_case_insensitive, extract_rank_from_markdown_list, normalize_aliases,
    parse_string_list_json,
  },
};
use globa_flux_rust::reach_reporting::ingest_channel_reach_basic_a1;

fn bearer_token(header_value: Option<&str>) -> Option<&str> {
  let value = header_value?;
  value.strip_prefix("Bearer ").or_else(|| value.strip_prefix("bearer "))
}

fn json_response(status: StatusCode, value: serde_json::Value) -> Result<Response<ResponseBody>, Error> {
  Ok(
    Response::builder()
      .status(status)
      .header("content-type", "application/json; charset=utf-8")
      .body(ResponseBody::from(value))?,
  )
}

fn has_tidb_url() -> bool {
  std::env::var("TIDB_DATABASE_URL")
    .or_else(|_| std::env::var("DATABASE_URL"))
    .map(|v| !v.is_empty())
    .unwrap_or(false)
}

fn truncate_string(value: &str, max_chars: usize) -> String {
  if max_chars == 0 {
    return String::new();
  }
  let mut out = String::new();
  for (idx, ch) in value.chars().enumerate() {
    if idx >= max_chars {
      break;
    }
    out.push(ch);
  }
  out
}

fn youtube_reporting_enable_url_from_error(err_text: &str) -> Option<String> {
  // Typical error contains:
  // "... enable it by visiting https://console.developers.google.com/apis/api/youtubereporting.googleapis.com/overview?project=1076253714959 ..."
  let find_digits_after = |haystack: &str, needle: &str| -> Option<String> {
    let idx = haystack.find(needle)?;
    let rest = &haystack[idx + needle.len()..];
    let digits = rest.chars().take_while(|c| c.is_ascii_digit()).collect::<String>();
    if digits.len() < 6 {
      return None;
    }
    Some(digits)
  };

  let project_id = find_digits_after(err_text, "project=")
    .or_else(|| {
      let lower = err_text.to_ascii_lowercase();
      let idx = lower.find("project ")?;
      let rest = &err_text[idx + "project ".len()..];
      let digits = rest.chars().skip_while(|c| !c.is_ascii_digit()).take_while(|c| c.is_ascii_digit()).collect::<String>();
      if digits.len() < 6 { None } else { Some(digits) }
    })?;

  Some(format!(
    "https://console.developers.google.com/apis/api/youtubereporting.googleapis.com/overview?project={}",
    project_id
  ))
}

fn worker_id() -> String {
  std::env::var("VERCEL_REGION")
    .or_else(|_| std::env::var("VERCEL_ENV"))
    .unwrap_or_else(|_| "local".to_string())
}

fn query_value<'a>(query: Option<&'a str>, key: &str) -> Option<&'a str> {
  let query = query?;
  for part in query.split('&') {
    let (k, v) = part.split_once('=')?;
    if k == key {
      return Some(v);
    }
  }
  None
}

const GLOBAL_TENANT_ID: &str = "global";
const YOUTUBE_REPORTING_BACKFILL_DAYS: i64 = 90;

fn parse_youtube_reporting_report_task_key(value: &str) -> Option<(String, String)> {
  let (content_owner_id, report_id) = value.split_once(':')?;
  if content_owner_id.is_empty() || report_id.is_empty() {
    return None;
  }
  Some((content_owner_id.to_string(), report_id.to_string()))
}

fn parse_video_ids_json(raw: &str) -> Vec<String> {
  serde_json::from_str::<Vec<String>>(raw)
    .unwrap_or_default()
    .into_iter()
    .map(|v| v.trim().to_string())
    .filter(|v| !v.is_empty())
    .collect()
}

fn json_string_field(payload: &serde_json::Value, key: &str) -> Option<String> {
  payload
    .get(key)
    .and_then(|v| v.as_str())
    .map(|v| v.trim().to_string())
    .filter(|v| !v.is_empty())
}

#[derive(Debug, Clone, Copy, Default)]
struct AggMetrics {
  revenue_usd: f64,
  impressions: i64,
  ctr_num: f64,
  ctr_denom: i64,
  views: i64,
}

fn agg_ctr(m: AggMetrics) -> Option<f64> {
  if m.ctr_denom > 0 {
    Some(m.ctr_num / (m.ctr_denom as f64))
  } else {
    None
  }
}

fn agg_rpm(m: AggMetrics) -> Option<f64> {
  if m.views > 0 {
    Some((m.revenue_usd / (m.views as f64)) * 1000.0)
  } else {
    None
  }
}

async fn aggregate_metrics_for_videos(
  pool: &sqlx::MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  video_ids: &[String],
  start_dt: NaiveDate,
  end_dt: NaiveDate,
) -> Result<AggMetrics, Error> {
  if start_dt > end_dt || video_ids.is_empty() {
    return Ok(AggMetrics::default());
  }

  let mut qb = sqlx::QueryBuilder::<sqlx::MySql>::new(
    r#"
      SELECT CAST(COALESCE(SUM(estimated_revenue_usd), 0) AS DOUBLE) AS revenue_usd,
             CAST(COALESCE(SUM(impressions), 0) AS SIGNED) AS impressions,
             CAST(COALESCE(SUM(impressions_ctr * impressions), 0) AS DOUBLE) AS ctr_num,
             CAST(COALESCE(SUM(CASE WHEN impressions_ctr IS NOT NULL THEN impressions ELSE 0 END), 0) AS SIGNED) AS ctr_denom,
             CAST(COALESCE(SUM(views), 0) AS SIGNED) AS views
      FROM video_daily_metrics
      WHERE tenant_id =
    "#,
  );
  qb.push_bind(tenant_id);
  qb.push(" AND channel_id = ");
  qb.push_bind(channel_id);
  qb.push(" AND dt BETWEEN ");
  qb.push_bind(start_dt);
  qb.push(" AND ");
  qb.push_bind(end_dt);
  qb.push(" AND video_id IN (");
  {
    let mut separated = qb.separated(", ");
    for vid in video_ids {
      separated.push_bind(vid);
    }
  }
  qb.push(");");

  let (revenue_usd, impressions, ctr_num, ctr_denom, views) = qb
    .build_query_as::<(f64, i64, f64, i64, i64)>()
    .fetch_one(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

  Ok(AggMetrics {
    revenue_usd,
    impressions,
    ctr_num,
    ctr_denom,
    views,
  })
}

async fn upsert_alert(
  pool: &sqlx::MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  alert_key: &str,
  kind: &str,
  severity: &str,
  message: &str,
  details_json: Option<&str>,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO yt_alerts (
        tenant_id, channel_id, alert_key,
        kind, severity, message, details_json,
        detected_at, resolved_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3), NULL)
      ON DUPLICATE KEY UPDATE
        kind = VALUES(kind),
        severity = VALUES(severity),
        message = VALUES(message),
        details_json = COALESCE(VALUES(details_json), details_json),
        detected_at = CURRENT_TIMESTAMP(3),
        resolved_at = NULL,
        updated_at = CURRENT_TIMESTAMP(3);
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(alert_key)
  .bind(kind)
  .bind(severity)
  .bind(message)
  .bind(details_json)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

async fn evaluate_running_experiments_for_channel(
  pool: &sqlx::MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  access_token: &str,
  run_for_dt: NaiveDate,
) -> Result<(), Error> {
  let last_complete_dt = run_for_dt - Duration::days(1);

  let rows = sqlx::query_as::<
    _,
    (
      i64,
      String,
      String,
      Option<f64>,
      Option<i64>,
      Option<DateTime<Utc>>,
      Option<DateTime<Utc>>,
    ),
  >(
    r#"
      SELECT id, type, video_ids_json,
             stop_loss_pct, planned_duration_days,
             started_at, ended_at
      FROM yt_experiments
      WHERE tenant_id = ?
        AND channel_id = ?
        AND state = 'running'
      ORDER BY created_at DESC
      LIMIT 50;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .fetch_all(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  for (id, exp_type, video_ids_json, stop_loss_pct, planned_duration_days, started_at, ended_at) in rows {
    let Some(started_at) = started_at else {
      continue;
    };

    let video_ids = parse_video_ids_json(&video_ids_json);
    if video_ids.len() != 1 {
      continue;
    }
    let primary_video_id = video_ids[0].trim().to_string();

    let start_dt = started_at.date_naive();
    let baseline_start_dt = start_dt - Duration::days(7);
    let baseline_end_dt = start_dt - Duration::days(1);
    let ended_dt = ended_at.map(|dt| dt.date_naive());
    let current_end_dt = ended_dt.unwrap_or(last_complete_dt).min(last_complete_dt);

    let baseline = aggregate_metrics_for_videos(
      pool,
      tenant_id,
      channel_id,
      &video_ids,
      baseline_start_dt,
      baseline_end_dt,
    )
    .await?;
    let current =
      aggregate_metrics_for_videos(pool, tenant_id, channel_id, &video_ids, start_dt, current_end_dt).await?;

    let (metric_name, baseline_metric, current_metric, sample_ok) = match exp_type.as_str() {
      "publish_time" => {
        let base = agg_rpm(baseline).unwrap_or(0.0);
        let cur = agg_rpm(current).unwrap_or(0.0);
        let ok = baseline.views >= 1000 && current.views >= 1000 && base > 0.0;
        ("RPM", base, cur, ok)
      }
      _ => {
        let base_opt = agg_ctr(baseline);
        let cur_opt = agg_ctr(current);
        let base = base_opt.unwrap_or(0.0);
        let cur = cur_opt.unwrap_or(0.0);
        let ok = baseline.impressions >= 5000
          && current.impressions >= 5000
          && baseline.ctr_denom > 0
          && current.ctr_denom > 0
          && base_opt.is_some()
          && cur_opt.is_some()
          && base > 0.0;
        ("CTR", base, cur, ok)
      }
    };

    if !sample_ok {
      continue;
    }

    let uplift = ((current_metric - baseline_metric) / baseline_metric).max(-1.0);

    let stop_loss_threshold = stop_loss_pct
      .filter(|v| *v > 0.0)
      .map(|v| -v / 100.0);

    if stop_loss_threshold.is_some_and(|t| uplift <= t) {
      let baseline_payload_json = sqlx::query_scalar::<_, String>(
        r#"
          SELECT payload_json
          FROM yt_experiment_variants
          WHERE experiment_id = ?
            AND variant_id = 'A'
          LIMIT 1;
        "#,
      )
      .bind(id)
      .fetch_optional(pool)
      .await
      .map_err(|e| -> Error { Box::new(e) })?;

      let baseline_payload = baseline_payload_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .filter(|v| v.is_object())
        .unwrap_or_else(|| serde_json::json!({}));

      let rollback_err: Option<String> = match exp_type.as_str() {
        "title" => match json_string_field(&baseline_payload, "title") {
          None => Some("baseline variant A missing title".to_string()),
          Some(title) => update_video_title(access_token, &primary_video_id, &title)
            .await
            .err()
            .map(|e| e.to_string()),
        },
        "thumbnail" => match json_string_field(&baseline_payload, "thumbnail_url")
          .or_else(|| json_string_field(&baseline_payload, "thumbnailUrl"))
        {
          None => Some("baseline variant A missing thumbnail_url".to_string()),
          Some(url) => set_video_thumbnail_from_url(access_token, &primary_video_id, &url)
            .await
            .err()
            .map(|e| e.to_string()),
        },
        "publish_time" => match json_string_field(&baseline_payload, "publish_at")
          .or_else(|| json_string_field(&baseline_payload, "publishAt"))
        {
          None => Some("baseline variant A missing publish_at".to_string()),
          Some(publish_at) => update_video_publish_at(access_token, &primary_video_id, &publish_at)
            .await
            .err()
            .map(|e| e.to_string()),
        },
        _ => None,
      };

      let mut tx = pool.begin().await.map_err(|e| -> Error { Box::new(e) })?;
      let updated = sqlx::query(
        r#"
          UPDATE yt_experiments
          SET state = 'stopped',
              ended_at = CURRENT_TIMESTAMP(3),
              updated_at = CURRENT_TIMESTAMP(3)
          WHERE id = ? AND tenant_id = ? AND state = 'running';
        "#,
      )
      .bind(id)
      .bind(tenant_id)
      .execute(&mut *tx)
      .await
      .map_err(|e| -> Error { Box::new(e) })?;

      if updated.rows_affected() > 0 {
        sqlx::query(
          r#"
            UPDATE yt_experiment_variants
            SET status = CASE
              WHEN variant_id = 'A' THEN 'won'
              WHEN variant_id = 'B' THEN 'lost'
              ELSE status
            END,
            updated_at = CURRENT_TIMESTAMP(3)
            WHERE experiment_id = ?;
          "#,
        )
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|e| -> Error { Box::new(e) })?;

        let mut msg = match metric_name {
          "RPM" => format!(
            "Experiment exp_{id} stop-loss triggered: RPM {:+.0}% vs baseline (current ${:.2}, baseline ${:.2}; views {}/{}).",
            uplift * 100.0,
            current_metric,
            baseline_metric,
            current.views,
            baseline.views
          ),
          _ => format!(
            "Experiment exp_{id} stop-loss triggered: CTR {:+.0}% vs baseline (current {:.2}%, baseline {:.2}%; impressions {}/{}).",
            uplift * 100.0,
            current_metric * 100.0,
            baseline_metric * 100.0,
            current.impressions,
            baseline.impressions
          ),
        };
        if let Some(err) = rollback_err.as_deref() {
          msg.push_str(&format!(" Rollback failed: {err}"));
        }

        tx.commit().await.map_err(|e| -> Error { Box::new(e) })?;

        let severity = if rollback_err.is_some() { "error" } else { "warning" };
        let _ = upsert_alert(
          pool,
          tenant_id,
          channel_id,
          &format!("exp_{id}_stoploss"),
          "Experiment stop-loss",
          severity,
          &msg,
          None,
        )
        .await;
      } else {
        tx.rollback().await.map_err(|e| -> Error { Box::new(e) })?;
      }

      continue;
    }

    if let Some(days) = planned_duration_days.filter(|v| *v > 0) {
      let elapsed_days = if current_end_dt >= start_dt {
        (current_end_dt - start_dt).num_days() + 1
      } else {
        0
      };

      if elapsed_days >= days {
        let (state, winner, loser) = if uplift >= 0.0 {
          ("won", "B", "A")
        } else {
          ("lost", "A", "B")
        };

        let baseline_payload_json = sqlx::query_scalar::<_, String>(
          r#"
            SELECT payload_json
            FROM yt_experiment_variants
            WHERE experiment_id = ?
              AND variant_id = 'A'
            LIMIT 1;
          "#,
        )
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| -> Error { Box::new(e) })?;

        let baseline_payload = baseline_payload_json
          .as_deref()
          .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
          .filter(|v| v.is_object())
          .unwrap_or_else(|| serde_json::json!({}));

        let rollback_err: Option<String> = if state == "lost" {
          match exp_type.as_str() {
            "title" => match json_string_field(&baseline_payload, "title") {
              None => Some("baseline variant A missing title".to_string()),
              Some(title) => update_video_title(access_token, &primary_video_id, &title)
                .await
                .err()
                .map(|e| e.to_string()),
            },
            "thumbnail" => match json_string_field(&baseline_payload, "thumbnail_url")
              .or_else(|| json_string_field(&baseline_payload, "thumbnailUrl"))
            {
              None => Some("baseline variant A missing thumbnail_url".to_string()),
              Some(url) => set_video_thumbnail_from_url(access_token, &primary_video_id, &url)
                .await
                .err()
                .map(|e| e.to_string()),
            },
            "publish_time" => match json_string_field(&baseline_payload, "publish_at")
              .or_else(|| json_string_field(&baseline_payload, "publishAt"))
            {
              None => Some("baseline variant A missing publish_at".to_string()),
              Some(publish_at) => update_video_publish_at(access_token, &primary_video_id, &publish_at)
                .await
                .err()
                .map(|e| e.to_string()),
            },
            _ => None,
          }
        } else {
          None
        };

        let mut tx = pool.begin().await.map_err(|e| -> Error { Box::new(e) })?;
        let updated = sqlx::query(
          r#"
            UPDATE yt_experiments
            SET state = ?,
                ended_at = CURRENT_TIMESTAMP(3),
                updated_at = CURRENT_TIMESTAMP(3)
            WHERE id = ? AND tenant_id = ? AND state = 'running';
          "#,
        )
        .bind(state)
        .bind(id)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| -> Error { Box::new(e) })?;

        if updated.rows_affected() > 0 {
          sqlx::query(
            r#"
              UPDATE yt_experiment_variants
              SET status = CASE
                WHEN variant_id = ? THEN 'won'
                WHEN variant_id = ? THEN 'lost'
                ELSE status
              END,
              updated_at = CURRENT_TIMESTAMP(3)
              WHERE experiment_id = ?;
            "#,
          )
          .bind(winner)
          .bind(loser)
          .bind(id)
          .execute(&mut *tx)
          .await
          .map_err(|e| -> Error { Box::new(e) })?;

          let mut msg = match metric_name {
            "RPM" => format!(
              "Experiment exp_{id} finished: {winner} wins ({metric_name} {:+.0}% vs baseline; current ${:.2}, baseline ${:.2}).",
              uplift * 100.0,
              current_metric,
              baseline_metric
            ),
            _ => format!(
              "Experiment exp_{id} finished: {winner} wins ({metric_name} {:+.0}% vs baseline; current {:.2}%, baseline {:.2}%).",
              uplift * 100.0,
              current_metric * 100.0,
              baseline_metric * 100.0
            ),
          };
          if let Some(err) = rollback_err.as_deref() {
            msg.push_str(&format!(" Rollback failed: {err}"));
          }

          tx.commit().await.map_err(|e| -> Error { Box::new(e) })?;

          let severity = if rollback_err.is_some() { "error" } else { "info" };
          let _ = upsert_alert(
            pool,
            tenant_id,
            channel_id,
            &format!("exp_{id}_result"),
            "Experiment result",
            severity,
            &msg,
            None,
          )
          .await;
        } else {
          tx.rollback().await.map_err(|e| -> Error { Box::new(e) })?;
        }
      }
    }
  }

  Ok(())
}

fn youtube_reporting_created_after_rfc3339(
  run_for_dt: chrono::NaiveDate,
  backfill_days: i64,
) -> String {
  let dt = chrono::DateTime::<Utc>::from_naive_utc_and_offset(
    run_for_dt.and_hms_opt(0, 0, 0).unwrap(),
    Utc,
  ) - chrono::Duration::days(backfill_days);

  dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn yt_reporting_wide_table_name(report_type_id: &str) -> String {
  let base = globa_flux_rust::db::sanitize_sql_identifier(report_type_id);
  let hash = sha2::Sha256::digest(report_type_id.as_bytes());
  let suffix = format!("{:x}", hash);
  let suffix8 = &suffix[..8];

  let mut name = format!("yt_rpt_{base}_{suffix8}");
  if name.len() > 64 {
    name.truncate(64);
    while name.ends_with('_') {
      name.pop();
    }
  }
  name
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

fn parse_rfc3339_utc(value: Option<&str>) -> Option<chrono::DateTime<Utc>> {
  let value = value?;
  chrono::DateTime::parse_from_rfc3339(value)
    .ok()
    .map(|dt| dt.with_timezone(&Utc))
}

async fn upsert_yt_reporting_wide_table_metadata(
  pool: &sqlx::MySqlPool,
  report_type_id: &str,
  table_name: &str,
  columns_json: &str,
  parse_version: &str,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO yt_reporting_wide_tables (report_type_id, table_name, columns_json, parse_version)
      VALUES (?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        table_name = VALUES(table_name),
        columns_json = VALUES(columns_json),
        parse_version = VALUES(parse_version),
        updated_at = CURRENT_TIMESTAMP(3);
    "#,
  )
  .bind(report_type_id)
  .bind(table_name)
  .bind(columns_json)
  .bind(parse_version)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

async fn ensure_yt_reporting_wide_table(
  pool: &sqlx::MySqlPool,
  table_name: &str,
  columns: &[String],
) -> Result<(), Error> {
  let mut ddl = String::new();
  ddl.push_str(&format!(
    "CREATE TABLE IF NOT EXISTS `{table_name}` (\
      tenant_id VARCHAR(128) NOT NULL,\
      content_owner_id VARCHAR(128) NOT NULL,\
      report_type_id VARCHAR(256) NOT NULL,\
      job_id VARCHAR(256) NOT NULL,\
      report_id VARCHAR(256) NOT NULL,\
      row_no BIGINT NOT NULL,\
      created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\
      updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"
  ));

  for col in columns {
    ddl.push_str(&format!(", `{}` LONGTEXT NULL", col));
  }

  ddl.push_str(
    ", PRIMARY KEY (tenant_id, content_owner_id, report_id, row_no),\
       KEY idx_owner_type (tenant_id, content_owner_id, report_type_id)\
     );",
  );

  sqlx::query(&ddl)
    .execute(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

  for col in columns {
    let alter = format!(
      "ALTER TABLE `{table_name}` ADD COLUMN IF NOT EXISTS `{col}` LONGTEXT NULL;"
    );
    sqlx::query(&alter)
      .execute(pool)
      .await
      .map_err(|e| -> Error { Box::new(e) })?;
  }

  Ok(())
}

async fn insert_yt_reporting_wide_rows_batch(
  pool: &sqlx::MySqlPool,
  table_name: &str,
  columns: &[String],
  tenant_id: &str,
  content_owner_id: &str,
  report_type_id: &str,
  job_id: &str,
  report_id: &str,
  rows: &[(i64, Vec<Option<String>>)],
) -> Result<(), Error> {
  if rows.is_empty() {
    return Ok(());
  }

  let mut qb = sqlx::QueryBuilder::<sqlx::MySql>::new("INSERT INTO ");
  qb.push(format!("`{table_name}`"));
  qb.push(" (tenant_id, content_owner_id, report_type_id, job_id, report_id, row_no");
  for col in columns {
    qb.push(", `");
    qb.push(col);
    qb.push("`");
  }
  qb.push(") ");

  qb.push_values(rows.iter(), |mut b, (row_no, values)| {
    b.push_bind(tenant_id);
    b.push_bind(content_owner_id);
    b.push_bind(report_type_id);
    b.push_bind(job_id);
    b.push_bind(report_id);
    b.push_bind(*row_no);
    for idx in 0..columns.len() {
      let v = values.get(idx).cloned().unwrap_or(None);
      b.push_bind(v);
    }
  });

  qb.push(" ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3)");
  qb.push(";");

  qb.build()
    .execute(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DispatchSchedule {
  Daily,
  Weekly,
  YoutubeReporting,
}

impl DispatchSchedule {
  fn from_query(query: Option<&str>) -> Self {
    let value = query_value(query, "schedule").unwrap_or("");
    match value {
      "weekly" | "Weekly" | "WEEKLY" => DispatchSchedule::Weekly,
      "youtube_reporting" | "youtubeReporting" | "YouTubeReporting" => DispatchSchedule::YoutubeReporting,
      _ => DispatchSchedule::Daily,
    }
  }

  fn job_type(&self) -> &'static str {
    match self {
      DispatchSchedule::Daily => "daily_channel",
      DispatchSchedule::Weekly => "weekly_channel",
      DispatchSchedule::YoutubeReporting => "youtube_reporting_owner",
    }
  }
}

fn candidate_select_sql(schedule: DispatchSchedule, has_tenant_filter: bool) -> &'static str {
  match (schedule, has_tenant_filter) {
    (DispatchSchedule::YoutubeReporting, true) => {
      r#"
        SELECT DISTINCT tenant_id, content_owner_id
        FROM channel_connections
        WHERE tenant_id = ?
          AND oauth_provider = 'youtube'
          AND content_owner_id IS NOT NULL
          AND content_owner_id <> '';
      "#
    }
    (DispatchSchedule::YoutubeReporting, false) => {
      r#"
        SELECT DISTINCT tenant_id, content_owner_id
        FROM channel_connections
        WHERE oauth_provider = 'youtube'
          AND content_owner_id IS NOT NULL
          AND content_owner_id <> '';
      "#
    }
    (_, true) => {
      r#"
        SELECT tenant_id, channel_id
        FROM channel_connections
        WHERE tenant_id = ?
          AND oauth_provider = 'youtube'
          AND channel_id IS NOT NULL
          AND channel_id <> '';
      "#
    }
    (_, false) => {
      r#"
        SELECT tenant_id, channel_id
        FROM channel_connections
        WHERE oauth_provider = 'youtube'
          AND channel_id IS NOT NULL
          AND channel_id <> '';
      "#
    }
  }
}

#[derive(Deserialize)]
struct DispatchRequest {
  now_ms: i64,
  #[serde(default)]
  tenant_id: Option<String>,
  #[serde(default)]
  channel_id: Option<String>,
  #[serde(default)]
  run_for_dt: Option<String>,
  #[serde(default)]
  backfill_weeks: Option<i64>,
}

#[derive(Deserialize)]
struct TickRequest {
  now_ms: i64,
  #[serde(default)]
  limit: Option<i64>,
  #[serde(default)]
  tenant_id: Option<String>,
}

#[derive(Deserialize)]
struct DecisionEngineConfigJson {
  #[serde(default)]
  min_days_with_data: Option<usize>,
  #[serde(default)]
  high_concentration_threshold: Option<f64>,
  #[serde(default)]
  trend_down_threshold_usd: Option<f64>,
  #[serde(default)]
  top_n_for_new_asset: Option<usize>,
}

fn default_policy_params_json(cfg: &DecisionEngineConfig) -> String {
  serde_json::json!({
    "min_days_with_data": cfg.min_days_with_data,
    "high_concentration_threshold": cfg.high_concentration_threshold,
    "trend_down_threshold_usd": cfg.trend_down_threshold_usd,
    "top_n_for_new_asset": cfg.top_n_for_new_asset,
  })
  .to_string()
}

fn cfg_from_policy_params_json(raw: &str) -> Option<DecisionEngineConfig> {
  let parsed: DecisionEngineConfigJson = serde_json::from_str(raw).ok()?;
  let mut cfg = DecisionEngineConfig::default();

  if let Some(v) = parsed.min_days_with_data {
    cfg.min_days_with_data = v;
  }
  if let Some(v) = parsed.high_concentration_threshold {
    cfg.high_concentration_threshold = v;
  }
  if let Some(v) = parsed.trend_down_threshold_usd {
    cfg.trend_down_threshold_usd = v;
  }
  if let Some(v) = parsed.top_n_for_new_asset {
    cfg.top_n_for_new_asset = v;
  }

  Some(cfg)
}

async fn handle_dispatch(
  schedule: DispatchSchedule,
  force: bool,
  method: &Method,
  headers: &HeaderMap,
  body: Bytes,
) -> Result<Response<ResponseBody>, Error> {
  if method != Method::POST {
    return json_response(
      StatusCode::METHOD_NOT_ALLOWED,
      serde_json::json!({"ok": false, "error": "method_not_allowed"}),
    );
  }

  let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
  let provided = bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

  if expected.is_empty() || provided != expected {
    return json_response(
      StatusCode::UNAUTHORIZED,
      serde_json::json!({"ok": false, "error": "unauthorized"}),
    );
  }

  if !has_tidb_url() {
    return json_response(
      StatusCode::NOT_IMPLEMENTED,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
    );
  }

  let parsed: DispatchRequest = match serde_json::from_slice(&body) {
    Ok(v) => v,
    Err(e) => {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": format!("invalid json body: {e}")}),
      );
    }
  };

  if parsed.now_ms <= 0 {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "now_ms is required"}),
    );
  }

  let now = Utc
    .timestamp_millis_opt(parsed.now_ms)
    .single()
    .unwrap_or_else(Utc::now);
  let run_for_dt = parsed
    .run_for_dt
    .as_deref()
    .map(str::trim)
    .filter(|v| !v.is_empty())
    .map(|v| chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d"))
    .transpose()
    .map_err(|e| -> Error { Box::new(std::io::Error::other(format!("invalid run_for_dt: {e}"))) })?
    .unwrap_or_else(|| now.date_naive());

  let pool = get_pool().await?;

  let tenant_filter = parsed
    .tenant_id
    .as_deref()
    .map(str::trim)
    .filter(|v| !v.is_empty())
    .map(str::to_string);

  let channel_filter = parsed
    .channel_id
    .as_deref()
    .map(str::trim)
    .filter(|v| !v.is_empty())
    .map(str::to_string);

  let channels: Vec<(String, String)> = if let Some(channel_id) = channel_filter.as_deref() {
    let tenant_id = tenant_filter.as_deref().ok_or_else(|| {
      Box::new(std::io::Error::other("tenant_id is required when channel_id is provided")) as Error
    })?;

    let exists: Option<i64> = if schedule == DispatchSchedule::YoutubeReporting {
      sqlx::query_scalar(
        r#"
          SELECT 1
          FROM channel_connections
          WHERE tenant_id = ?
            AND oauth_provider = 'youtube'
            AND content_owner_id = ?
          LIMIT 1;
        "#,
      )
      .bind(tenant_id)
      .bind(channel_id)
      .fetch_optional(pool)
      .await
      .map_err(|e| -> Error { Box::new(e) })?
    } else {
      sqlx::query_scalar(
        r#"
          SELECT 1
          FROM channel_connections
          WHERE tenant_id = ?
            AND oauth_provider = 'youtube'
            AND channel_id = ?
          LIMIT 1;
        "#,
      )
      .bind(tenant_id)
      .bind(channel_id)
      .fetch_optional(pool)
      .await
      .map_err(|e| -> Error { Box::new(e) })?
    };

    if exists.is_none() {
      return json_response(
        StatusCode::NOT_FOUND,
        serde_json::json!({"ok": false, "error": "not_connected", "message": "No matching YouTube connection for tenant/channel"}),
      );
    }

    vec![(tenant_id.to_string(), channel_id.to_string())]
  } else if let Some(tenant_id) = tenant_filter.as_deref() {
    sqlx::query_as(candidate_select_sql(schedule, true))
      .bind(tenant_id)
      .fetch_all(pool)
      .await
      .map_err(|e| -> Error { Box::new(e) })?
  } else {
    sqlx::query_as(candidate_select_sql(schedule, false))
      .fetch_all(pool)
      .await
      .map_err(|e| -> Error { Box::new(e) })?
  };

  let job_type = schedule.job_type();
  let mut enqueued: usize = 0;
  let backfill_weeks = parsed
    .backfill_weeks
    .unwrap_or(0)
    .clamp(0, 52);

  for (tenant_id, channel_id) in channels.iter() {
    let mut run_for_dts: Vec<chrono::NaiveDate> = vec![run_for_dt];

    // First sync should backfill enough history for baseline comparisons + reports.
    // Only do this when the channel has no metrics yet.
    if schedule == DispatchSchedule::Daily {
      if backfill_weeks > 1 {
        // Insert newest first so the worker processes current data first (ORDER BY id ASC).
        run_for_dts = (0..backfill_weeks)
          .map(|i| run_for_dt - Duration::days((i * 7) as i64))
          .collect();
      } else {
      let max_dt: Option<chrono::NaiveDate> = sqlx::query_scalar(
        r#"
          SELECT MAX(dt) AS max_dt
          FROM video_daily_metrics
          WHERE tenant_id = ? AND channel_id = ?;
        "#,
      )
      .bind(tenant_id)
      .bind(channel_id)
      .fetch_one(pool)
      .await
      .unwrap_or(None);

      if max_dt.is_none() {
        // Insert newest first so the worker processes current data first (ORDER BY id ASC).
        run_for_dts = (0..4)
          .map(|i| run_for_dt - Duration::days((i * 7) as i64))
          .collect();
      }
      }
    }

    for run_for_dt in run_for_dts.into_iter() {
      enqueued += 1;
      let dedupe_key = format!("{tenant_id}:{job_type}:{channel_id}:{run_for_dt}");

      if force {
        sqlx::query(
        r#"
          INSERT INTO job_tasks (tenant_id, job_type, channel_id, run_for_dt, dedupe_key, status, attempt, max_attempt, run_after)
          VALUES (?, ?, ?, ?, ?, 'pending', 0, 3, ?)
          ON DUPLICATE KEY UPDATE
            updated_at = CURRENT_TIMESTAMP(3),
            max_attempt = CASE
              WHEN max_attempt < 3 THEN 3
              ELSE max_attempt
            END,
            run_after = CASE
              WHEN status = 'running' THEN run_after
              ELSE ?
            END,
            status = CASE
              WHEN status = 'running' THEN status
              ELSE 'pending'
            END,
            attempt = CASE
              WHEN status = 'running' THEN attempt
              ELSE 0
            END,
            last_error = CASE
              WHEN status = 'running' THEN last_error
              ELSE NULL
            END,
            locked_by = CASE
              WHEN status = 'running' THEN locked_by
              ELSE NULL
            END,
            locked_at = CASE
              WHEN status = 'running' THEN locked_at
              ELSE NULL
            END;
        "#,
        )
        .bind(tenant_id)
        .bind(job_type)
        .bind(channel_id)
        .bind(run_for_dt)
        .bind(dedupe_key)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .map_err(|e| -> Error { Box::new(e) })?;
      } else {
        sqlx::query(
        r#"
          INSERT INTO job_tasks (tenant_id, job_type, channel_id, run_for_dt, dedupe_key, status, attempt, max_attempt, run_after)
          VALUES (?, ?, ?, ?, ?, 'pending', 0, 3, ?)
          ON DUPLICATE KEY UPDATE
            updated_at = CURRENT_TIMESTAMP(3),
            max_attempt = CASE
              WHEN max_attempt < 3 THEN 3
              ELSE max_attempt
            END,
            attempt = CASE
              WHEN status = 'dead' THEN 0
              ELSE attempt
            END,
            last_error = CASE
              WHEN status = 'dead' THEN NULL
              ELSE last_error
            END,
            locked_by = CASE
              WHEN status = 'dead' THEN NULL
              ELSE locked_by
            END,
            locked_at = CASE
              WHEN status = 'dead' THEN NULL
              ELSE locked_at
            END,
            run_after = CASE
              WHEN status IN ('pending','retrying','dead') THEN ?
              ELSE run_after
            END,
            status = CASE
              WHEN status = 'dead' THEN 'pending'
              ELSE status
            END;
        "#,
        )
        .bind(tenant_id)
        .bind(job_type)
        .bind(channel_id)
        .bind(run_for_dt)
        .bind(dedupe_key)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .map_err(|e| -> Error { Box::new(e) })?;
      }
    }
  }

  json_response(
    StatusCode::OK,
    serde_json::json!({
      "ok": true,
      "tenant_id": tenant_filter,
      "job_type": job_type,
      "run_for_dt": run_for_dt.to_string(),
      "force": force,
      "candidates": channels.len(),
      "enqueued": enqueued
    }),
  )
}

async fn handle_tick(method: &Method, headers: &HeaderMap, body: Bytes) -> Result<Response<ResponseBody>, Error> {
  if method != Method::POST {
    return json_response(
      StatusCode::METHOD_NOT_ALLOWED,
      serde_json::json!({"ok": false, "error": "method_not_allowed"}),
    );
  }

  let expected = std::env::var("RUST_INTERNAL_TOKEN").unwrap_or_default();
  let provided = bearer_token(headers.get("authorization").and_then(|v| v.to_str().ok())).unwrap_or("");

  if expected.is_empty() || provided != expected {
    return json_response(
      StatusCode::UNAUTHORIZED,
      serde_json::json!({"ok": false, "error": "unauthorized"}),
    );
  }

  if !has_tidb_url() {
    return json_response(
      StatusCode::NOT_IMPLEMENTED,
      serde_json::json!({"ok": false, "error": "not_configured", "message": "Missing TIDB_DATABASE_URL (or DATABASE_URL)"}),
    );
  }

  let parsed: TickRequest = match serde_json::from_slice(&body) {
    Ok(v) => v,
    Err(e) => {
      return json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "error": "bad_request", "message": format!("invalid json body: {e}")}),
      );
    }
  };

  if parsed.now_ms <= 0 {
    return json_response(
      StatusCode::BAD_REQUEST,
      serde_json::json!({"ok": false, "error": "bad_request", "message": "now_ms is required"}),
    );
  }

  let limit = parsed.limit.unwrap_or(10).clamp(1, 50) as i64;
  let tenant_filter = parsed
    .tenant_id
    .as_deref()
    .map(str::trim)
    .filter(|v| !v.is_empty());

  let now = Utc
    .timestamp_millis_opt(parsed.now_ms)
    .single()
    .unwrap_or_else(Utc::now);
  let pool = get_pool().await?;

  let lock_ttl_secs: i64 = std::env::var("JOB_TASK_LOCK_TTL_SECS")
    .ok()
    .and_then(|v| v.parse().ok())
    .unwrap_or(600)
    .clamp(60, 3600);
  let stale_before = now - Duration::seconds(lock_ttl_secs);

  let reclaimed = if let Some(tenant_id) = tenant_filter {
    sqlx::query(
      r#"
        UPDATE job_tasks
        SET status='retrying', run_after=?, locked_by=NULL, locked_at=NULL
        WHERE tenant_id = ?
          AND status='running'
          AND locked_at IS NOT NULL
          AND locked_at < ?;
      "#,
    )
    .bind(now)
    .bind(tenant_id)
    .bind(stale_before)
    .execute(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?
    .rows_affected()
  } else {
    sqlx::query(
      r#"
        UPDATE job_tasks
        SET status='retrying', run_after=?, locked_by=NULL, locked_at=NULL
        WHERE status='running' AND locked_at IS NOT NULL AND locked_at < ?;
      "#,
    )
    .bind(now)
    .bind(stale_before)
    .execute(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?
    .rows_affected()
  };

  let worker_id = worker_id();

  let mut tx = pool.begin().await.map_err(|e| -> Error { Box::new(e) })?;
  let claimed: Vec<(i64, String, String, String, Option<chrono::NaiveDate>, i32, i32)> =
    if let Some(tenant_id) = tenant_filter {
      sqlx::query_as(
        r#"
          SELECT id, tenant_id, job_type, channel_id, run_for_dt, attempt, max_attempt
          FROM job_tasks
          WHERE tenant_id = ?
            AND status IN ('pending','retrying')
            AND run_after <= ?
          ORDER BY id ASC
          LIMIT ?
          FOR UPDATE;
        "#,
      )
      .bind(tenant_id)
      .bind(now)
      .bind(limit)
      .fetch_all(&mut *tx)
      .await
      .map_err(|e| -> Error { Box::new(e) })?
    } else {
      sqlx::query_as(
        r#"
          SELECT id, tenant_id, job_type, channel_id, run_for_dt, attempt, max_attempt
          FROM job_tasks
          WHERE status IN ('pending','retrying')
            AND run_after <= ?
          ORDER BY id ASC
          LIMIT ?
          FOR UPDATE;
        "#,
      )
      .bind(now)
      .bind(limit)
      .fetch_all(&mut *tx)
      .await
      .map_err(|e| -> Error { Box::new(e) })?
    };

  for (id, _tenant_id, _job_type, _channel_id, _run_for_dt, _attempt, _max_attempt) in claimed.iter() {
    sqlx::query(
      r#"
        UPDATE job_tasks
        SET status='running', attempt=attempt+1, locked_by=?, locked_at=?
        WHERE id=?;
      "#,
    )
    .bind(&worker_id)
    .bind(now)
    .bind(id)
    .execute(&mut *tx)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;
  }

  tx.commit().await.map_err(|e| -> Error { Box::new(e) })?;

  let mut succeeded = 0usize;
  let mut retried = 0usize;
  let mut dead = 0usize;
  let mut last_error: Option<String> = None;

  for (id, tenant_id, job_type, channel_id, run_for_dt, attempt, max_attempt) in claimed.iter() {
    let attempt_next = attempt.saturating_add(1);

    let result: Result<(), Error> = match job_type.as_str() {
      "geo_monitor_prompt" => {
        (|| async {
          let run_for_dt = run_for_dt.ok_or_else(|| {
            Box::new(std::io::Error::other("geo_monitor_prompt task missing run_for_dt")) as Error
          })?;

          let mut parts = channel_id.split(':');
          let project_id: i64 = parts
            .next()
            .unwrap_or("")
            .parse()
            .map_err(|_| Box::new(std::io::Error::other("geo_monitor_prompt invalid project_id")) as Error)?;
          let prompt_id: i64 = parts
            .next()
            .unwrap_or("")
            .parse()
            .map_err(|_| Box::new(std::io::Error::other("geo_monitor_prompt invalid prompt_id")) as Error)?;

          let project = fetch_geo_monitor_project(pool, tenant_id, project_id)
            .await?
            .ok_or_else(|| Box::new(std::io::Error::other("missing geo monitor project")) as Error)?;
          let prompt = fetch_geo_monitor_prompt(pool, tenant_id, project_id, prompt_id)
            .await?
            .ok_or_else(|| Box::new(std::io::Error::other("missing geo monitor prompt")) as Error)?;

          let prompt_total: i32 = sqlx::query_scalar(
            r#"
              SELECT COUNT(*) FROM geo_monitor_prompts
              WHERE tenant_id = ? AND project_id = ? AND enabled = 1;
            "#,
          )
          .bind(tenant_id)
          .bind(project_id)
          .fetch_one(pool)
          .await
          .map_err(|e| -> Error { Box::new(e) })?;

          let mut gemini_cfg = GeminiConfig::from_env_optional()?
            .ok_or_else(|| Box::new(std::io::Error::other("Missing GEMINI_API_KEY")) as Error)?;

          let db_model = fetch_tenant_gemini_model(pool, GLOBAL_TENANT_ID).await?;
          let model = db_model
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
              Box::new(std::io::Error::other("Missing gemini_model for tenant_id=global")) as Error
            })?;

          gemini_cfg.model = model.to_string();

          let run = ensure_geo_monitor_run(
            pool,
            tenant_id,
            project_id,
            run_for_dt,
            "gemini",
            model,
            prompt_total,
          )
          .await?;

          let aliases = parse_string_list_json(project.brand_aliases_json.as_deref());
          let needles = normalize_aliases(&project.name, aliases.as_slice());

          let system = "You are a helpful assistant.";
          let temperature = 0.2;
          let max_output_tokens: u32 = 1024;

          let idempotency_key =
            format!("{tenant_id}:geo_monitor_prompt:{project_id}:{run_for_dt}:{prompt_id}");

          let provider = "gemini";
          let pricing = gemini_pricing_for_model(model);

          match gemini_generate_text(
            &gemini_cfg,
            system,
            &prompt.prompt_text,
            temperature,
            max_output_tokens,
          )
          .await
          {
            Ok((text, usage)) => {
              let presence = contains_any_case_insensitive(&text, needles.as_slice());
              let rank = extract_rank_from_markdown_list(&text, needles.as_slice());

              let (prompt_tokens, completion_tokens, cost_usd) = if let (Some(u), Some(p)) = (usage, pricing) {
                let cost = compute_cost_usd(p, u.prompt_tokens as u32, u.completion_tokens as u32);
                (u.prompt_tokens, u.completion_tokens, cost)
              } else {
                (0, 0, 0.0)
              };

              if prompt_tokens > 0 || completion_tokens > 0 {
                if let Err(err) = insert_usage_event(
                  pool,
                  tenant_id,
                  "geo_monitor_prompt",
                  &idempotency_key,
                  provider,
                  model,
                  prompt_tokens,
                  completion_tokens,
                  cost_usd,
                )
                .await
                {
                  if err
                    .as_database_error()
                    .is_some_and(|e| e.is_unique_violation())
                  {
                    // idempotent replay: ignore
                  } else {
                    return Err(Box::new(err) as Error);
                  }
                }
              }

              let _ = insert_geo_monitor_run_result(
                pool,
                tenant_id,
                project_id,
                run_for_dt,
                run.id,
                prompt_id,
                &prompt.prompt_text,
                Some(&text),
                presence,
                rank,
                cost_usd,
                None,
              )
              .await?;
              let _ = finalize_geo_monitor_run_if_complete(pool, run.id).await?;

              Ok(())
            }
            Err(err) => {
              let msg = truncate_string(&err.to_string(), 2000);
              let _ = insert_geo_monitor_run_result(
                pool,
                tenant_id,
                project_id,
                run_for_dt,
                run.id,
                prompt_id,
                &prompt.prompt_text,
                None,
                false,
                None,
                0.0,
                Some(&msg),
              )
              .await?;
              let _ = finalize_geo_monitor_run_if_complete(pool, run.id).await?;
              Ok(())
            }
          }
        })()
        .await
      }
      "daily_channel" => {
        (|| async {
          let run_for_dt = run_for_dt.ok_or_else(|| {
            Box::new(std::io::Error::other("daily_channel task missing run_for_dt")) as Error
          })?;

          let start_dt = run_for_dt - chrono::Duration::days(7);
          let end_dt = run_for_dt - chrono::Duration::days(1);

          let mut tokens = fetch_youtube_connection_tokens(pool, tenant_id, channel_id)
            .await?
            .ok_or_else(|| {
              Box::new(std::io::Error::other(format!(
                "missing youtube channel connection: tenant_id={tenant_id} channel_id={channel_id}"
              ))) as Error
            })?;

          let active_cfg_default = DecisionEngineConfig::default();
          let active_params_json = fetch_policy_params_json(pool, tenant_id, channel_id, "active").await?;
          let cfg = active_params_json
            .as_deref()
            .and_then(cfg_from_policy_params_json)
            .unwrap_or_else(DecisionEngineConfig::default);

          if active_params_json.is_none() {
            let params_json = default_policy_params_json(&active_cfg_default);
            upsert_policy_params(pool, tenant_id, channel_id, "active", &params_json, "system").await?;
          }

          // Proactive refresh if expired (best-effort).
          let now_dt = now;
          let needs_refresh = tokens
            .expires_at
            .map(|t| t <= now_dt)
            .unwrap_or(false);

          if needs_refresh {
            if let Some(refresh) = tokens.refresh_token.clone() {
              let app = fetch_or_seed_youtube_oauth_app_config(pool, tenant_id)
                .await?
                .ok_or_else(|| Box::new(std::io::Error::other("missing youtube oauth app config")) as Error)?;
              let client_secret = app
                .client_secret
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                  Box::new(std::io::Error::other("missing youtube oauth client_secret")) as Error
                })?;
              let (client, _redirect) =
                youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
              let refreshed = refresh_tokens(&client, &refresh).await?;
              update_youtube_connection_tokens(pool, tenant_id, channel_id, &refreshed).await?;
              tokens.access_token = refreshed.access_token;
              tokens.refresh_token = refreshed.refresh_token.or(Some(refresh));
            }
          }

          let metrics = match fetch_video_daily_metrics_for_channel(&tokens.access_token, channel_id, start_dt, end_dt).await {
            Ok(rows) => rows,
            Err(err) if err.status == Some(401) => {
              if let Some(refresh) = tokens.refresh_token.clone() {
                let app = fetch_or_seed_youtube_oauth_app_config(pool, tenant_id)
                  .await?
                  .ok_or_else(|| Box::new(std::io::Error::other("missing youtube oauth app config")) as Error)?;
                let client_secret = app
                  .client_secret
                  .as_deref()
                  .map(str::trim)
                  .filter(|v| !v.is_empty())
                  .ok_or_else(|| {
                    Box::new(std::io::Error::other("missing youtube oauth client_secret")) as Error
                  })?;
                let (client, _redirect) =
                  youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
                let refreshed = refresh_tokens(&client, &refresh).await?;
                update_youtube_connection_tokens(pool, tenant_id, channel_id, &refreshed).await?;
                tokens.access_token = refreshed.access_token;

                fetch_video_daily_metrics_for_channel(&tokens.access_token, channel_id, start_dt, end_dt)
                  .await
                  .map_err(youtube_analytics_error_to_vercel_error)?
              } else {
                return Err(youtube_analytics_error_to_vercel_error(err));
              }
            }
            Err(err) => return Err(youtube_analytics_error_to_vercel_error(err)),
          };

          for row in metrics.iter() {
            upsert_video_daily_metric(
              pool,
              tenant_id,
              channel_id,
              row.dt,
              &row.video_id,
              row.estimated_revenue_usd,
              row.impressions,
              row.impressions_ctr,
              row.views,
            )
            .await?;
          }

          // Reach metrics (impressions/CTR) are only available via the YouTube Reporting API bulk reports.
          // Best-effort: sync the last window to unlock CTR + impressions guardrails and dashboard verification.
          if let Err(err) = ingest_channel_reach_basic_a1(
            pool,
            tenant_id,
            channel_id,
            &tokens.access_token,
            start_dt,
            end_dt,
          )
          .await
          {
            eprintln!(
              "daily_channel: reach ingest failed tenant_id={} channel_id={} window={}..{} err={}",
              tenant_id,
              channel_id,
              start_dt,
              end_dt,
              err
            );

            let err_text = truncate_string(&err.to_string(), 1400);
            let (severity, message) = if err_text.contains("YouTube Reporting API has not been used in project")
              || err_text.contains("is disabled")
            {
              (
                "warning",
                "Impressions/Impr. CTR unavailable: enable the YouTube Reporting API for this OAuth project, then re-sync.",
              )
            } else if err_text.contains("forbidden") || err_text.contains("Forbidden") {
              (
                "warning",
                "Impressions/Impr. CTR unavailable: missing YouTube Reporting permission for this channel/account.",
              )
            } else {
              ("warning", "Impressions/Impr. CTR sync failed (best-effort).")
            };

            let mut help = serde_json::json!({
              "docs": "https://developers.google.com/youtube/reporting",
              "gcp_api": "YouTube Reporting API",
            });

            if let Some(enable_url) = youtube_reporting_enable_url_from_error(&err_text) {
              help["enable_url"] = serde_json::Value::String(enable_url);
            }

            let details_json = serde_json::json!({
              "window": { "start_dt": start_dt.to_string(), "end_dt": end_dt.to_string() },
              "error": err_text,
              "help": help,
            }).to_string();

            let _ = upsert_alert(
              pool,
              tenant_id,
              channel_id,
              "reach_reporting_unavailable",
              "Data reach",
              severity,
              message,
              Some(&details_json),
            )
            .await;
          }

          let publish_counts =
            fetch_new_video_publish_counts_by_dt(pool, tenant_id, channel_id, start_dt, end_dt).await?;
          for (dt, new_videos) in publish_counts.into_iter() {
            if new_videos <= 0 {
              continue;
            }
            let meta_json = serde_json::json!({ "new_videos": new_videos }).to_string();
            upsert_observed_action(pool, tenant_id, channel_id, dt, "publish", Some(&meta_json)).await?;
          }

          let decision = compute_decision(
            metrics.as_slice(),
            run_for_dt,
            start_dt,
            end_dt,
            cfg.clone(),
          );

          let evidence_json = serde_json::to_string(&decision.evidence).unwrap_or_else(|_| "[]".to_string());
          let forbidden_json = serde_json::to_string(&decision.forbidden).unwrap_or_else(|_| "[]".to_string());
          let reevaluate_json = serde_json::to_string(&decision.reevaluate).unwrap_or_else(|_| "[]".to_string());

          sqlx::query(
            r#"
              INSERT INTO decision_daily (
                tenant_id, channel_id, as_of_dt,
                direction, confidence,
                evidence_json, forbidden_json, reevaluate_json
              )
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)
              ON DUPLICATE KEY UPDATE
                direction = VALUES(direction),
                confidence = VALUES(confidence),
                evidence_json = VALUES(evidence_json),
                forbidden_json = VALUES(forbidden_json),
                reevaluate_json = VALUES(reevaluate_json),
                updated_at = CURRENT_TIMESTAMP(3);
            "#,
          )
          .bind(tenant_id)
          .bind(channel_id)
          .bind(run_for_dt)
          .bind(&decision.direction)
          .bind(decision.confidence)
          .bind(evidence_json)
          .bind(forbidden_json)
          .bind(reevaluate_json)
          .execute(pool)
          .await
          .map_err(|e| -> Error { Box::new(e) })?;

          let decision_dt = run_for_dt - chrono::Duration::days(7);
          if decision_daily_exists(pool, tenant_id, channel_id, decision_dt).await? {
            let pre_start_dt = decision_dt - chrono::Duration::days(7);
            let pre_end_dt = decision_dt - chrono::Duration::days(1);
            let post_start_dt = decision_dt;
            let post_end_dt = decision_dt + chrono::Duration::days(6);

            let pre_sum =
              fetch_revenue_sum_usd_7d(pool, tenant_id, channel_id, pre_start_dt, pre_end_dt).await?;
            let post_sum = fetch_revenue_sum_usd_7d(
              pool,
              tenant_id,
              channel_id,
              post_start_dt,
              post_end_dt,
            )
            .await?;

            let top_n = (cfg.top_n_for_new_asset as i64).clamp(1, 10);
            let pre_top =
              fetch_top_video_ids_by_revenue(pool, tenant_id, channel_id, pre_start_dt, pre_end_dt, top_n).await?;
            let post_top =
              fetch_top_video_ids_by_revenue(pool, tenant_id, channel_id, post_start_dt, post_end_dt, top_n).await?;

            let outcome = compute_outcome_label(pre_sum, post_sum, &pre_top, &post_top);
            let notes = serde_json::json!({
              "pre_window": { "start_dt": pre_start_dt.to_string(), "end_dt": pre_end_dt.to_string(), "revenue_sum_usd_7d": pre_sum },
              "post_window": { "start_dt": post_start_dt.to_string(), "end_dt": post_end_dt.to_string(), "revenue_sum_usd_7d": post_sum },
              "top_n": top_n,
            })
            .to_string();

            upsert_decision_outcome(
              pool,
              tenant_id,
              channel_id,
              decision_dt,
              run_for_dt,
              outcome.revenue_change_pct_7d,
              outcome.catastrophic_flag,
              outcome.new_top_asset_flag,
              Some(&notes),
            )
            .await?;
          }

          if let Err(err) = evaluate_running_experiments_for_channel(
            pool,
            tenant_id,
            channel_id,
            &tokens.access_token,
            run_for_dt,
          )
          .await
          {
            eprintln!(
              "daily_channel: evaluate_running_experiments_for_channel error: {}",
              err
            );
          }

          // Keep guardrails fresh after the latest sync window completes.
          // For initial backfills we may run multiple `daily_channel` tasks; evaluate only once (today's run).
          if run_for_dt == now.date_naive() {
            if let Err(err) = evaluate_youtube_alerts(pool, tenant_id, channel_id).await {
              eprintln!("daily_channel: evaluate_youtube_alerts error: {}", err);
            }
          }

          Ok(())
        })()
        .await
      }
      "weekly_channel" => {
        (|| async {
          let run_for_dt = run_for_dt.ok_or_else(|| {
            Box::new(std::io::Error::other("weekly_channel task missing run_for_dt")) as Error
          })?;

          let default_cfg = DecisionEngineConfig::default();
          let params_json = default_policy_params_json(&default_cfg);

          upsert_policy_params(pool, tenant_id, channel_id, "active", &params_json, "system").await?;

          let candidate_version = format!("candidate-{run_for_dt}");
          upsert_policy_params(pool, tenant_id, channel_id, &candidate_version, &params_json, "system").await?;

          let replay_metrics_json = serde_json::json!({
            "ok": true,
            "note": "v1 scaffold: replay gate not implemented yet",
            "candidate_version": candidate_version,
            "run_for_dt": run_for_dt.to_string(),
          })
          .to_string();

          upsert_policy_eval_report(pool, tenant_id, channel_id, &candidate_version, &replay_metrics_json, false).await?;

          Ok(())
        })()
        .await
      }
      "youtube_reporting_owner" => {
        (|| async {
          let run_for_dt = run_for_dt.ok_or_else(|| {
            Box::new(std::io::Error::other("youtube_reporting_owner task missing run_for_dt")) as Error
          })?;

          let content_owner_id = channel_id.trim();
          if content_owner_id.is_empty() {
            return Err(Box::new(std::io::Error::other(
              "youtube_reporting_owner task missing content_owner_id",
            )) as Error);
          }

          let channel_id_for_tokens = fetch_youtube_channel_id(pool, tenant_id)
            .await?
            .ok_or_else(|| {
              Box::new(std::io::Error::other(format!(
                "missing youtube channel connection: tenant_id={tenant_id}"
              ))) as Error
            })?;

          let mut tokens = fetch_youtube_connection_tokens(pool, tenant_id, &channel_id_for_tokens)
            .await?
            .ok_or_else(|| {
              Box::new(std::io::Error::other(format!(
                "missing youtube channel connection: tenant_id={tenant_id} channel_id={channel_id_for_tokens}"
              ))) as Error
            })?;

          // Proactive refresh if expired (best-effort).
          let needs_refresh = tokens
            .expires_at
            .map(|t| t <= now)
            .unwrap_or(false);
        if needs_refresh {
          if let Some(refresh) = tokens.refresh_token.clone() {
            let app = fetch_or_seed_youtube_oauth_app_config(pool, tenant_id)
              .await?
              .ok_or_else(|| Box::new(std::io::Error::other("missing youtube oauth app config")) as Error)?;
            let client_secret = app
              .client_secret
              .as_deref()
              .map(str::trim)
              .filter(|v| !v.is_empty())
              .ok_or_else(|| {
                Box::new(std::io::Error::other("missing youtube oauth client_secret")) as Error
              })?;
            let (client, _redirect) =
              youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
            let refreshed = refresh_tokens(&client, &refresh).await?;
            update_youtube_connection_tokens(pool, tenant_id, &channel_id_for_tokens, &refreshed).await?;
            tokens.access_token = refreshed.access_token;
            tokens.refresh_token = refreshed.refresh_token.or(Some(refresh));
          }
        }

          let created_after = youtube_reporting_created_after_rfc3339(
            run_for_dt,
            YOUTUBE_REPORTING_BACKFILL_DAYS,
          );

          let report_types = list_report_types(&tokens.access_token, content_owner_id)
            .await
            .map_err(|e| -> Error {
              Box::new(std::io::Error::other(format!(
                "youtube reporting list_report_types error: {e}"
              )))
            })?;

          for rt in report_types {
            let system_managed = if rt.system_managed { 1i8 } else { 0i8 };
            sqlx::query(
              r#"
                INSERT INTO yt_reporting_report_types
                  (content_owner_id, report_type_id, report_type_name, system_managed)
                VALUES
                  (?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                  report_type_name = VALUES(report_type_name),
                  system_managed = VALUES(system_managed),
                  updated_at = CURRENT_TIMESTAMP(3);
              "#,
            )
            .bind(content_owner_id)
            .bind(&rt.report_type_id)
            .bind(rt.report_type_name.as_deref())
            .bind(system_managed)
            .execute(pool)
            .await
            .map_err(|e| -> Error { Box::new(e) })?;

            let job_id = match ensure_job_for_report_type(
              &tokens.access_token,
              content_owner_id,
              &rt.report_type_id,
            )
            .await
            {
              Ok(v) => v,
              Err(err) => {
                eprintln!(
                  "youtube_reporting_owner: ensure_job failed for report_type_id={}: {}",
                  rt.report_type_id, err
                );
                continue;
              }
            };

            sqlx::query(
              r#"
                INSERT INTO yt_reporting_jobs
                  (tenant_id, content_owner_id, report_type_id, job_id)
                VALUES
                  (?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                  job_id = VALUES(job_id),
                  updated_at = CURRENT_TIMESTAMP(3);
              "#,
            )
            .bind(tenant_id)
            .bind(content_owner_id)
            .bind(&rt.report_type_id)
            .bind(&job_id)
            .execute(pool)
            .await
            .map_err(|e| -> Error { Box::new(e) })?;

            let reports = match list_reports(
              &tokens.access_token,
              &job_id,
              content_owner_id,
              Some(created_after.as_str()),
            )
            .await
            {
              Ok(v) => v,
              Err(err) => {
                eprintln!(
                  "youtube_reporting_owner: list_reports failed for report_type_id={} job_id={}: {}",
                  rt.report_type_id, job_id, err
                );
                continue;
              }
            };

            for rep in reports {
              let start_time = parse_rfc3339_utc(rep.start_time.as_deref());
              let end_time = parse_rfc3339_utc(rep.end_time.as_deref());
              let create_time = parse_rfc3339_utc(rep.create_time.as_deref());

              sqlx::query(
                r#"
                  INSERT INTO yt_reporting_report_files
                    (tenant_id, content_owner_id, report_type_id, job_id, report_id, download_url, start_time, end_time, create_time)
                  VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?)
                  ON DUPLICATE KEY UPDATE
                    download_url = COALESCE(VALUES(download_url), download_url),
                    start_time = COALESCE(VALUES(start_time), start_time),
                    end_time = COALESCE(VALUES(end_time), end_time),
                    create_time = COALESCE(VALUES(create_time), create_time),
                    updated_at = CURRENT_TIMESTAMP(3);
                "#,
              )
              .bind(tenant_id)
              .bind(content_owner_id)
              .bind(&rt.report_type_id)
              .bind(&job_id)
              .bind(&rep.report_id)
              .bind(rep.download_url.as_deref())
              .bind(start_time)
              .bind(end_time)
              .bind(create_time)
              .execute(pool)
              .await
              .map_err(|e| -> Error { Box::new(e) })?;

              let task_channel_id = format!("{content_owner_id}:{}", rep.report_id);
              let dedupe_key = format!(
                "{tenant_id}:youtube_reporting_report:{content_owner_id}:{}",
                rep.report_id
              );
              sqlx::query(
                r#"
                  INSERT INTO job_tasks (tenant_id, job_type, channel_id, run_for_dt, dedupe_key, status)
                  VALUES (?, 'youtube_reporting_report', ?, ?, ?, 'pending')
                  ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
                "#,
              )
              .bind(tenant_id)
              .bind(task_channel_id)
              .bind(run_for_dt)
              .bind(dedupe_key)
              .execute(pool)
              .await
              .map_err(|e| -> Error { Box::new(e) })?;
            }
          }

          Ok(())
        })()
        .await
      }
      "youtube_reporting_report" => {
        (|| async {
          let (content_owner_id, report_id) = parse_youtube_reporting_report_task_key(channel_id)
            .ok_or_else(|| {
              Box::new(std::io::Error::other("youtube_reporting_report invalid channel_id")) as Error
            })?;

          let channel_id_for_tokens = fetch_youtube_channel_id(pool, tenant_id)
            .await?
            .ok_or_else(|| {
              Box::new(std::io::Error::other(format!(
                "missing youtube channel connection: tenant_id={tenant_id}"
              ))) as Error
            })?;

          let mut tokens = fetch_youtube_connection_tokens(pool, tenant_id, &channel_id_for_tokens)
            .await?
            .ok_or_else(|| {
              Box::new(std::io::Error::other(format!(
                "missing youtube channel connection: tenant_id={tenant_id} channel_id={channel_id_for_tokens}"
              ))) as Error
            })?;

          // Proactive refresh if expired (best-effort).
          let needs_refresh = tokens
            .expires_at
            .map(|t| t <= now)
            .unwrap_or(false);
          if needs_refresh {
            if let Some(refresh) = tokens.refresh_token.clone() {
              let app = fetch_or_seed_youtube_oauth_app_config(pool, tenant_id)
                .await?
                .ok_or_else(|| Box::new(std::io::Error::other("missing youtube oauth app config")) as Error)?;
              let client_secret = app
                .client_secret
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                  Box::new(std::io::Error::other("missing youtube oauth client_secret")) as Error
                })?;
              let (client, _redirect) =
                youtube_oauth_client_from_config(&app.client_id, client_secret, &app.redirect_uri)?;
              let refreshed = refresh_tokens(&client, &refresh).await?;
              update_youtube_connection_tokens(pool, tenant_id, &channel_id_for_tokens, &refreshed).await?;
              tokens.access_token = refreshed.access_token;
              tokens.refresh_token = refreshed.refresh_token.or(Some(refresh));
            }
          }

          let row = sqlx::query_as::<_, (String, String, Option<String>, Option<Vec<u8>>, String)>(
            r#"
              SELECT report_type_id, job_id, download_url, raw_bytes, parse_status
              FROM yt_reporting_report_files
              WHERE tenant_id = ?
                AND content_owner_id = ?
                AND report_id = ?
              LIMIT 1;
            "#,
          )
          .bind(tenant_id)
          .bind(&content_owner_id)
          .bind(&report_id)
          .fetch_optional(pool)
          .await
          .map_err(|e| -> Error { Box::new(e) })?;

          let Some((report_type_id, job_id, download_url, raw_bytes, parse_status)) = row else {
            return Err(Box::new(std::io::Error::other(
              "missing yt_reporting_report_files row",
            )) as Error);
          };

          if parse_status == "parsed" {
            return Ok(());
          }

          let bytes = match raw_bytes {
            Some(b) => b,
            None => {
              let url = download_url.ok_or_else(|| {
                Box::new(std::io::Error::other("missing download_url")) as Error
              })?;

              let downloaded = download_report_file(&tokens.access_token, &url)
                .await
                .map_err(|e| -> Error {
                  Box::new(std::io::Error::other(format!(
                    "youtube reporting download_report_file error: {e}"
                  )))
                })?;

              let vec = downloaded.to_vec();
              let sha256 = format!("{:x}", sha2::Sha256::digest(&vec));
              let len = vec.len() as i64;

              sqlx::query(
                r#"
                  UPDATE yt_reporting_report_files
                  SET raw_sha256 = ?, raw_bytes = ?, raw_bytes_len = ?, downloaded_at = CURRENT_TIMESTAMP(3)
                  WHERE tenant_id = ?
                    AND content_owner_id = ?
                    AND report_id = ?
                    AND raw_bytes IS NULL;
                "#,
              )
              .bind(sha256)
              .bind(&vec)
              .bind(len)
              .bind(tenant_id)
              .bind(&content_owner_id)
              .bind(&report_id)
              .execute(pool)
              .await
              .map_err(|e| -> Error { Box::new(e) })?;

              vec
            }
          };

          let parse_result: Result<(), Error> = (|| async {
            let decoded = maybe_gunzip_bytes(&bytes).map_err(|e| -> Error { Box::new(e) })?;

            let mut rdr = csv::ReaderBuilder::new()
              .has_headers(true)
              .from_reader(decoded.as_slice());

            let headers = rdr
              .headers()
              .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?
              .iter()
              .map(|h| h.trim_start_matches('\u{feff}').to_string())
              .collect::<Vec<_>>();

            let columns = globa_flux_rust::db::dedupe_columns(&headers);
            let table_name = yt_reporting_wide_table_name(&report_type_id);
            let columns_json = serde_json::to_string(&columns).unwrap_or_else(|_| "[]".to_string());
            let parse_version = "v1";

            upsert_yt_reporting_wide_table_metadata(
              pool,
              &report_type_id,
              &table_name,
              &columns_json,
              parse_version,
            )
            .await?;

            ensure_yt_reporting_wide_table(pool, &table_name, &columns).await?;

            let binds_per_row = 6usize.saturating_add(columns.len());
            let max_rows = (65000usize / binds_per_row).max(1);
            let batch_size = max_rows.min(200);

            let mut row_no: i64 = 0;
            let mut batch: Vec<(i64, Vec<Option<String>>)> = Vec::with_capacity(batch_size);

            for result in rdr.records() {
              let record = result
                .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?;
              row_no += 1;

              let mut values: Vec<Option<String>> = Vec::with_capacity(columns.len());
              for idx in 0..columns.len() {
                let v = record.get(idx).unwrap_or("");
                if v.is_empty() {
                  values.push(None);
                } else {
                  values.push(Some(v.to_string()));
                }
              }

              batch.push((row_no, values));
              if batch.len() >= batch_size {
                insert_yt_reporting_wide_rows_batch(
                  pool,
                  &table_name,
                  &columns,
                  tenant_id,
                  &content_owner_id,
                  &report_type_id,
                  &job_id,
                  &report_id,
                  batch.as_slice(),
                )
                .await?;
                batch.clear();
              }
            }

            if !batch.is_empty() {
              insert_yt_reporting_wide_rows_batch(
                pool,
                &table_name,
                &columns,
                tenant_id,
                &content_owner_id,
                &report_type_id,
                &job_id,
                &report_id,
                batch.as_slice(),
              )
              .await?;
            }

            Ok(())
          })()
          .await;

          match parse_result {
            Ok(()) => {
              sqlx::query(
                r#"
                  UPDATE yt_reporting_report_files
                  SET parse_status = 'parsed',
                      parse_version = 'v1',
                      parsed_at = CURRENT_TIMESTAMP(3),
                      parse_error = NULL
                  WHERE tenant_id = ?
                    AND content_owner_id = ?
                    AND report_id = ?;
                "#,
              )
              .bind(tenant_id)
              .bind(&content_owner_id)
              .bind(&report_id)
              .execute(pool)
              .await
              .map_err(|e| -> Error { Box::new(e) })?;

              Ok(())
            }
            Err(err) => {
              let message = truncate_string(&err.to_string(), 2000);
              sqlx::query(
                r#"
                  UPDATE yt_reporting_report_files
                  SET parse_status = 'error',
                      parse_version = 'v1',
                      parsed_at = CURRENT_TIMESTAMP(3),
                      parse_error = ?
                  WHERE tenant_id = ?
                    AND content_owner_id = ?
                    AND report_id = ?;
                "#,
              )
              .bind(message)
              .bind(tenant_id)
              .bind(&content_owner_id)
              .bind(&report_id)
              .execute(pool)
              .await
              .map_err(|e| -> Error { Box::new(e) })?;

              // Parsing errors are not retried; the raw blob remains for replay.
              Ok(())
            }
          }
        })()
        .await
      }
      other => Err(Box::new(std::io::Error::other(format!(
        "unknown job_type: {other}"
      ))) as Error),
    };

    match result {
      Ok(()) => {
        sqlx::query(
          r#"
            UPDATE job_tasks
            SET status='succeeded', locked_by=NULL, locked_at=NULL, last_error=NULL
            WHERE id=?;
          "#,
        )
        .bind(id)
        .execute(pool)
        .await
        .map_err(|e| -> Error { Box::new(e) })?;

        succeeded += 1;
      }
      Err(err) => {
        let message = truncate_string(&err.to_string(), 2000);
        if last_error.is_none() {
          last_error = Some(message.clone());
        }

        if attempt_next >= *max_attempt {
          sqlx::query(
            r#"
              UPDATE job_tasks
              SET status='dead', locked_by=NULL, locked_at=NULL, last_error=?
              WHERE id=?;
            "#,
          )
          .bind(message)
          .bind(id)
          .execute(pool)
          .await
          .map_err(|e| -> Error { Box::new(e) })?;

          dead += 1;
        } else {
          let backoff_seconds = (attempt_next as i64).saturating_mul(60);
          let run_after = now + Duration::seconds(backoff_seconds);
          sqlx::query(
            r#"
              UPDATE job_tasks
              SET status='retrying', run_after=?, locked_by=NULL, locked_at=NULL, last_error=?
              WHERE id=?;
            "#,
          )
          .bind(run_after)
          .bind(message)
          .bind(id)
          .execute(pool)
          .await
          .map_err(|e| -> Error { Box::new(e) })?;

          retried += 1;
        }
      }
    }
  }

  json_response(
    StatusCode::OK,
    serde_json::json!({
      "ok": true,
      "worker_id": worker_id,
      "tenant_id": tenant_filter,
      "reclaimed": reclaimed,
      "claimed": claimed.len(),
      "succeeded": succeeded,
      "retried": retried,
      "dead": dead,
      "last_error": last_error,
    }),
  )
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let action = query_value(req.uri().query(), "action").unwrap_or("tick");
  let result = match action {
    "dispatch" => {
      let schedule = DispatchSchedule::from_query(req.uri().query());
      let force = query_value(req.uri().query(), "force")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes"))
        .unwrap_or(false);
      let method = req.method().clone();
      let headers = req.headers().clone();
      let bytes = req.into_body().collect().await?.to_bytes();
      handle_dispatch(schedule, force, &method, &headers, bytes).await
    }
    "" | "tick" => {
      let method = req.method().clone();
      let headers = req.headers().clone();
      let bytes = req.into_body().collect().await?.to_bytes();
      handle_tick(&method, &headers, bytes).await
    }
    _ => json_response(
      StatusCode::NOT_FOUND,
      serde_json::json!({"ok": false, "error": "not_found"}),
    ),
  };

  match result {
    Ok(resp) => Ok(resp),
    Err(err) => {
      let message = truncate_string(&err.to_string(), 2000);
      json_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::json!({"ok": false, "error": "internal_error", "message": message}),
      )
    }
  }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  run(service_fn(handler)).await
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parses_youtube_reporting_report_task_key() {
    assert_eq!(
      parse_youtube_reporting_report_task_key("CMS123:rep_1"),
      Some(("CMS123".to_string(), "rep_1".to_string()))
    );
    assert_eq!(parse_youtube_reporting_report_task_key("CMS123:"), None);
    assert_eq!(parse_youtube_reporting_report_task_key(":rep_1"), None);
    assert_eq!(parse_youtube_reporting_report_task_key("nope"), None);
  }

  #[test]
  fn formats_created_after_for_backfill() {
    let run_for_dt = chrono::NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
    let expected = chrono::Utc
      .with_ymd_and_hms(2026, 2, 1, 0, 0, 0)
      .unwrap()
      - chrono::Duration::days(90);
    assert_eq!(
      youtube_reporting_created_after_rfc3339(run_for_dt, 90),
      expected.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    );
  }

  #[test]
  fn reporting_wide_table_name_is_mysql_safe() {
    let name = yt_reporting_wide_table_name("channel_basic_a2");
    assert!(name.starts_with("yt_rpt_"));
    assert!(name.len() <= 64);
    assert!(name
      .chars()
      .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'));
  }

  #[test]
  fn gunzips_when_magic_header_present() {
    use std::io::Write;

    let plain = b"a,b\n1,2\n";

    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(plain).unwrap();
    let gz = enc.finish().unwrap();

    assert_eq!(maybe_gunzip_bytes(&gz).unwrap(), plain);
    assert_eq!(maybe_gunzip_bytes(plain).unwrap(), plain);
  }

  #[test]
  fn parses_rfc3339_timestamps_as_utc() {
    let dt = parse_rfc3339_utc(Some("2026-01-01T00:00:00Z")).unwrap();
    assert_eq!(
      dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
      "2026-01-01T00:00:00Z"
    );
    assert_eq!(parse_rfc3339_utc(Some("nope")), None);
    assert_eq!(parse_rfc3339_utc(None), None);
  }

  #[tokio::test]
  async fn dispatch_returns_unauthorized_when_missing_internal_token() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");

    let headers = HeaderMap::new();
    let response = handle_dispatch(DispatchSchedule::Daily, false, &Method::POST, &headers, Bytes::new())
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  }

  #[tokio::test]
  async fn returns_unauthorized_when_missing_internal_token() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");

    let headers = HeaderMap::new();
    let response = handle_tick(&Method::POST, &headers, Bytes::new())
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  }

  #[tokio::test]
  async fn returns_not_configured_when_tidb_env_missing_with_tenant_filter() {
    std::env::set_var("RUST_INTERNAL_TOKEN", "secret");
    std::env::remove_var("TIDB_DATABASE_URL");
    std::env::remove_var("DATABASE_URL");

    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer secret".parse().unwrap());
    headers.insert("content-type", "application/json".parse().unwrap());

    let body = Bytes::from(r#"{"now_ms":1700000000000,"tenant_id":"t1"}"#);
    let response = handle_tick(&Method::POST, &headers, body).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
  }
}

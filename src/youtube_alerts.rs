use chrono::{Duration, NaiveDate, Utc};
use sqlx::MySqlPool;
use vercel_runtime::Error;

use crate::guardrails::{evaluate_guardrails, GuardrailAlert, GuardrailInput, WindowAgg};

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

fn round2(v: f64) -> f64 {
  (v * 100.0).round() / 100.0
}

async fn upsert_alert(
  pool: &MySqlPool,
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

async fn auto_resolve_alert(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  alert_key: &str,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      UPDATE yt_alerts
      SET resolved_at = CURRENT_TIMESTAMP(3),
          updated_at = CURRENT_TIMESTAMP(3)
      WHERE tenant_id = ?
        AND channel_id = ?
        AND alert_key = ?
        AND resolved_at IS NULL;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(alert_key)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

pub async fn evaluate_youtube_alerts(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
) -> Result<(), Error> {
  use std::collections::HashMap;
  use std::collections::HashSet;

  async fn sum_rev_views_window(
    pool: &MySqlPool,
    tenant_id: &str,
    channel_id: &str,
    start_dt: NaiveDate,
    end_dt: NaiveDate,
  ) -> Result<(f64, i64, &'static str), Error> {
    let (rows_n, rev, views) = sqlx::query_as::<_, (i64, f64, i64)>(
      r#"
          SELECT CAST(COUNT(*) AS SIGNED) AS rows_n,
                 CAST(COALESCE(SUM(estimated_revenue_usd), 0) AS DOUBLE) AS revenue_usd,
                 CAST(COALESCE(SUM(views), 0) AS SIGNED) AS views
          FROM video_daily_metrics
          WHERE tenant_id = ?
            AND channel_id = ?
            AND dt BETWEEN ? AND ?
            AND video_id IN ('__CHANNEL_TOTAL__','csv_channel_total');
        "#,
    )
    .bind(tenant_id)
    .bind(channel_id)
    .bind(start_dt)
    .bind(end_dt)
    .fetch_one(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

    if rows_n > 0 {
      return Ok((rev, views, "channel_total"));
    }

    let (rev, views) = sqlx::query_as::<_, (f64, i64)>(
      r#"
          SELECT CAST(COALESCE(SUM(estimated_revenue_usd), 0) AS DOUBLE) AS revenue_usd,
                 CAST(COALESCE(SUM(views), 0) AS SIGNED) AS views
          FROM video_daily_metrics
          WHERE tenant_id = ?
            AND channel_id = ?
            AND dt BETWEEN ? AND ?
            AND video_id NOT IN ('__CHANNEL_TOTAL__','csv_channel_total');
        "#,
    )
    .bind(tenant_id)
    .bind(channel_id)
    .bind(start_dt)
    .bind(end_dt)
    .fetch_one(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

    Ok((rev, views, "video_sum"))
  }

  let today = Utc::now().date_naive();
  let current_start = today - Duration::days(7);
  let current_end = today - Duration::days(1);
  let baseline_start = today - Duration::days(14);
  let baseline_end = today - Duration::days(8);

  let (cur_rev, cur_views, cur_source) =
    sum_rev_views_window(pool, tenant_id, channel_id, current_start, current_end).await?;
  let (base_rev, base_views, base_source) =
    sum_rev_views_window(pool, tenant_id, channel_id, baseline_start, baseline_end).await?;

  let total_rev_7d = if cur_rev.is_finite() { Some(cur_rev) } else { None };

  let top_video_7d = if total_rev_7d.unwrap_or(0.0) >= 20.0 {
    sqlx::query_as::<_, (String, f64)>(
      r#"
        SELECT video_id, CAST(SUM(estimated_revenue_usd) AS DOUBLE) AS rev
        FROM video_daily_metrics
        WHERE tenant_id = ?
          AND channel_id = ?
          AND dt BETWEEN ? AND ?
          AND video_id NOT IN ('__CHANNEL_TOTAL__','csv_channel_total')
        GROUP BY video_id
        ORDER BY rev DESC
        LIMIT 1;
      "#,
    )
    .bind(tenant_id)
    .bind(channel_id)
    .bind(current_start)
    .bind(current_end)
    .fetch_optional(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?
  } else {
    None
  };

  let top1_concentration_7d = match (top_video_7d.as_ref(), total_rev_7d) {
    (Some((_video_id, top_rev)), Some(total_rev)) if total_rev > 0.0 => {
      Some((*top_rev / total_rev).clamp(0.0, 1.0))
    }
    _ => None,
  };
  let can_compute_concentration = top1_concentration_7d.is_some() && total_rev_7d.is_some();

  let mut daily_totals = sqlx::query_as::<_, (NaiveDate, f64)>(
    r#"
      SELECT dt,
             CAST(COALESCE(
               SUM(CASE WHEN video_id='csv_channel_total' THEN estimated_revenue_usd END),
               SUM(CASE WHEN video_id='__CHANNEL_TOTAL__' THEN estimated_revenue_usd END),
               0
             ) AS DOUBLE) AS rev
      FROM video_daily_metrics
      WHERE tenant_id = ?
        AND channel_id = ?
        AND dt BETWEEN ? AND ?
        AND video_id IN ('__CHANNEL_TOTAL__','csv_channel_total')
      GROUP BY dt
      ORDER BY dt ASC;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(current_start)
  .bind(current_end)
  .fetch_all(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  if daily_totals.is_empty() {
    daily_totals = sqlx::query_as::<_, (NaiveDate, f64)>(
      r#"
        SELECT dt, CAST(SUM(estimated_revenue_usd) AS DOUBLE) AS rev
        FROM video_daily_metrics
        WHERE tenant_id = ?
          AND channel_id = ?
          AND dt BETWEEN ? AND ?
          AND video_id NOT IN ('__CHANNEL_TOTAL__','csv_channel_total')
        GROUP BY dt
        ORDER BY dt ASC;
      "#,
    )
    .bind(tenant_id)
    .bind(channel_id)
    .bind(current_start)
    .bind(current_end)
    .fetch_all(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;
  }

  let daily_revs: Vec<f64> = daily_totals
    .iter()
    .map(|(_, rev)| *rev)
    .filter(|rev| rev.is_finite())
    .collect();

  let (rev_mean_7d, rev_stddev_7d) = if daily_revs.len() >= 5 {
    let n = daily_revs.len() as f64;
    let mean = daily_revs.iter().sum::<f64>() / n;
    let var = daily_revs
      .iter()
      .map(|v| {
        let d = v - mean;
        d * d
      })
      .sum::<f64>()
      / n;
    (Some(mean), Some(var.sqrt()))
  } else {
    (None, None)
  };
  let can_compute_volatility = rev_mean_7d.is_some() && rev_stddev_7d.is_some();

  let max_dt = sqlx::query_scalar::<_, Option<NaiveDate>>(
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
  .map_err(|e| -> Error { Box::new(e) })?;

  let input = GuardrailInput {
    today,
    current: WindowAgg {
      revenue_usd: cur_rev,
      views: cur_views,
    },
    baseline: WindowAgg {
      revenue_usd: base_rev,
      views: base_views,
    },
    max_metric_dt: max_dt,
    top1_concentration_7d,
    total_revenue_usd_7d: total_rev_7d,
    revenue_mean_usd_7d: rev_mean_7d,
    revenue_stddev_usd_7d: rev_stddev_7d,
  };

  let mut desired = evaluate_guardrails(&input);

  let latest_job = sqlx::query_as::<_, (String, Option<NaiveDate>, i32, i32, Option<String>)>(
    r#"
      SELECT status, run_for_dt, attempt, max_attempt, last_error
      FROM job_tasks
      WHERE tenant_id = ?
        AND channel_id = ?
        AND job_type = 'daily_channel'
        AND run_for_dt IS NOT NULL
      ORDER BY run_for_dt DESC, id DESC
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let mut forbidden = false;
  let mut unsupported = false;
  let mut latest_job_details: Option<serde_json::Value> = None;
  if let Some((status, run_for_dt, attempt, max_attempt, last_error)) = latest_job.as_ref() {
    latest_job_details = Some(serde_json::json!({
      "job_type": "daily_channel",
      "status": status,
      "run_for_dt": run_for_dt.map(|d| d.to_string()),
      "attempt": attempt,
      "max_attempt": max_attempt,
      "last_error": last_error.as_deref().map(|v| truncate_string(v, 600)),
    }));

    if status != "succeeded" {
      if let Some(err) = last_error.as_deref() {
        let msg = err.to_ascii_lowercase();
        forbidden = msg.contains("status 403") || msg.contains("reason\": \"forbidden\"");
        unsupported = msg.contains("status 400") && msg.contains("not supported");

        if forbidden {
          desired.push(GuardrailAlert {
            key: "youtube_analytics_forbidden",
            kind: "YouTube Analytics",
            severity: "warning",
            message: "YouTube Analytics blocked revenue metrics (403 Forbidden). Reconnect YouTube or upload CSV for revenue/RPM guardrails.".to_string(),
          });
        }

        if unsupported {
          desired.push(GuardrailAlert {
            key: "youtube_analytics_query_unsupported",
            kind: "YouTube Analytics",
            severity: "info",
            message: "YouTube Analytics does not support this report for your channel (400). We'll sync views-only; upload CSV for revenue/RPM.".to_string(),
          });
        }
      }
    }
  }

  let revenue_missing = !forbidden && !unsupported && cur_views >= 10_000 && cur_rev <= 0.01;
  if revenue_missing {
    desired.push(GuardrailAlert {
      key: "revenue_missing_7d",
      kind: "Revenue missing",
      severity: "info",
      message: "Views are present but revenue is zero (last 7d). Channel may not be monetized or monetary Analytics access is unavailable; upload CSV if you need revenue/RPM.".to_string(),
    });
  }

  let cur_rpm = if cur_views > 0 {
    (cur_rev / (cur_views as f64)) * 1000.0
  } else {
    0.0
  };
  let base_rpm = if base_views > 0 {
    (base_rev / (base_views as f64)) * 1000.0
  } else {
    0.0
  };
  let rpm_drop_pct = if base_rpm > 0.0 {
    ((base_rpm - cur_rpm) / base_rpm).max(-1.0)
  } else {
    0.0
  };

  let mut details_by_key: HashMap<&'static str, String> = HashMap::new();

  details_by_key.insert(
    "rpm_drop_7d",
    serde_json::json!({
      "window": {
        "current": { "start_dt": current_start.to_string(), "end_dt": current_end.to_string(), "revenue_usd": round2(cur_rev), "views": cur_views, "rpm": round2(cur_rpm), "source": cur_source },
        "baseline": { "start_dt": baseline_start.to_string(), "end_dt": baseline_end.to_string(), "revenue_usd": round2(base_rev), "views": base_views, "rpm": round2(base_rpm), "source": base_source },
      },
      "rpm_drop_pct": (rpm_drop_pct * 10000.0).round() / 10000.0,
    })
    .to_string(),
  );

  let stale_age_days = max_dt.map(|dt| (today - dt).num_days());
  details_by_key.insert(
    "metrics_stale",
    serde_json::json!({
      "today": today.to_string(),
      "max_metric_dt": max_dt.map(|d| d.to_string()),
      "age_days": stale_age_days,
    })
    .to_string(),
  );

  if can_compute_concentration {
    details_by_key.insert(
      "rev_concentration_top1_7d",
      serde_json::json!({
        "window": { "start_dt": current_start.to_string(), "end_dt": current_end.to_string() },
        "total_revenue_usd_7d": total_rev_7d.map(round2),
        "top_video": top_video_7d.as_ref().map(|(video_id, rev)| serde_json::json!({"video_id": video_id, "revenue_usd": round2(*rev)})),
        "top1_concentration_7d": top1_concentration_7d,
      })
      .to_string(),
    );
  }

  if can_compute_volatility {
    let daily: Vec<serde_json::Value> = daily_totals
      .iter()
      .map(|(dt, rev)| serde_json::json!({"dt": dt.to_string(), "revenue_usd": round2(*rev)}))
      .collect();
    details_by_key.insert(
      "rev_volatility_7d",
      serde_json::json!({
        "window": { "start_dt": current_start.to_string(), "end_dt": current_end.to_string() },
        "revenue_mean_usd_7d": rev_mean_7d.map(round2),
        "revenue_stddev_usd_7d": rev_stddev_7d.map(round2),
        "daily_revenue_usd": daily,
      })
      .to_string(),
    );
  }

  if let Some(job) = latest_job_details.as_ref() {
    if forbidden {
      details_by_key.insert("youtube_analytics_forbidden", serde_json::json!({"job": job}).to_string());
    }
    if unsupported {
      details_by_key.insert(
        "youtube_analytics_query_unsupported",
        serde_json::json!({"job": job}).to_string(),
      );
    }
  }

  if revenue_missing {
    details_by_key.insert(
      "revenue_missing_7d",
      serde_json::json!({
        "window": { "start_dt": current_start.to_string(), "end_dt": current_end.to_string() },
        "revenue_usd": round2(cur_rev),
        "views": cur_views,
        "rpm": round2(cur_rpm),
        "source": cur_source,
        "threshold": { "views": 10_000, "revenue_usd": 0.01 },
      })
      .to_string(),
    );
  }

  let desired_keys: HashSet<&str> = desired.iter().map(|a| a.key).collect();

  for alert in desired.iter() {
    let details_json = details_by_key.get(alert.key).map(|v| v.as_str());
    upsert_alert(
      pool,
      tenant_id,
      channel_id,
      alert.key,
      alert.kind,
      alert.severity,
      &alert.message,
      details_json,
    )
    .await?;
  }

  if !desired_keys.contains("metrics_stale") {
    auto_resolve_alert(pool, tenant_id, channel_id, "metrics_stale").await?;
  }

  let can_compare = cur_views >= 1000 && base_views >= 1000 && base_rpm > 0.0;
  if can_compare && !desired_keys.contains("rpm_drop_7d") {
    auto_resolve_alert(pool, tenant_id, channel_id, "rpm_drop_7d").await?;
  }

  if can_compute_concentration && !desired_keys.contains("rev_concentration_top1_7d") {
    auto_resolve_alert(pool, tenant_id, channel_id, "rev_concentration_top1_7d").await?;
  }

  if can_compute_volatility && !desired_keys.contains("rev_volatility_7d") {
    auto_resolve_alert(pool, tenant_id, channel_id, "rev_volatility_7d").await?;
  }

  if !desired_keys.contains("youtube_analytics_forbidden") {
    auto_resolve_alert(pool, tenant_id, channel_id, "youtube_analytics_forbidden").await?;
  }

  if !desired_keys.contains("youtube_analytics_query_unsupported") {
    auto_resolve_alert(pool, tenant_id, channel_id, "youtube_analytics_query_unsupported").await?;
  }

  if !desired_keys.contains("revenue_missing_7d") {
    auto_resolve_alert(pool, tenant_id, channel_id, "revenue_missing_7d").await?;
  }

  Ok(())
}

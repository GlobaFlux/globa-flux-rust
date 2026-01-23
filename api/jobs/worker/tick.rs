use bytes::Bytes;
use chrono::{Duration, TimeZone, Utc};
use http_body_util::BodyExt;
use hyper::{HeaderMap, Method, StatusCode};
use serde::Deserialize;
use vercel_runtime::{run, service_fn, Error, Request, Response, ResponseBody};

use globa_flux_rust::db::{
  decision_daily_exists,
  fetch_new_video_publish_counts_by_dt,
  fetch_policy_params_json,
  fetch_revenue_sum_usd_7d,
  fetch_top_video_ids_by_revenue,
  fetch_youtube_connection_tokens,
  get_pool,
  update_youtube_connection_tokens,
  upsert_decision_outcome,
  upsert_observed_action,
  upsert_policy_eval_report,
  upsert_policy_params,
  upsert_video_daily_metric,
};
use globa_flux_rust::decision_engine::{compute_decision, DecisionEngineConfig};
use globa_flux_rust::outcome_engine::compute_outcome_label;
use globa_flux_rust::providers::youtube::{refresh_tokens, youtube_oauth_client_from_env};
use globa_flux_rust::providers::youtube_analytics::{fetch_video_daily_metrics, youtube_analytics_error_to_vercel_error};

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

fn worker_id() -> String {
  std::env::var("VERCEL_REGION")
    .or_else(|_| std::env::var("VERCEL_ENV"))
    .unwrap_or_else(|_| "local".to_string())
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

  let parsed: TickRequest = serde_json::from_slice(&body).map_err(|e| -> Error {
    Box::new(std::io::Error::other(format!("invalid json body: {e}")))
  })?;

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

  for (id, tenant_id, job_type, channel_id, run_for_dt, attempt, max_attempt) in claimed.iter() {
    let attempt_next = attempt.saturating_add(1);

    let result: Result<(), Error> = match job_type.as_str() {
      "daily_channel" => {
        let run_for_dt = run_for_dt.ok_or_else(|| {
          Box::new(std::io::Error::other("daily_channel task missing run_for_dt")) as Error
        })?;

        let start_dt = run_for_dt - chrono::Duration::days(7);
        let end_dt = run_for_dt - chrono::Duration::days(1);

        let mut tokens = fetch_youtube_connection_tokens(pool, tenant_id, channel_id)
          .await?
          .ok_or_else(|| Box::new(std::io::Error::other("missing youtube channel connection")) as Error)?;

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
            let (client, _redirect) = youtube_oauth_client_from_env()?;
            let refreshed = refresh_tokens(&client, &refresh).await?;
            update_youtube_connection_tokens(pool, tenant_id, channel_id, &refreshed).await?;
            tokens.access_token = refreshed.access_token;
            tokens.refresh_token = refreshed.refresh_token.or(Some(refresh));
          }
        }

        let metrics = match fetch_video_daily_metrics(&tokens.access_token, start_dt, end_dt).await {
          Ok(rows) => rows,
          Err(err) if err.status == Some(401) => {
            if let Some(refresh) = tokens.refresh_token.clone() {
              let (client, _redirect) = youtube_oauth_client_from_env()?;
              let refreshed = refresh_tokens(&client, &refresh).await?;
              update_youtube_connection_tokens(pool, tenant_id, channel_id, &refreshed).await?;
              tokens.access_token = refreshed.access_token;

              fetch_video_daily_metrics(&tokens.access_token, start_dt, end_dt)
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
            row.views,
          )
          .await?;
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

        Ok(())
      }
      "weekly_channel" => {
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
    }),
  )
}

async fn handler(req: Request) -> Result<Response<ResponseBody>, Error> {
  let method = req.method().clone();
  let headers = req.headers().clone();
  let bytes = req.into_body().collect().await?.to_bytes();
  handle_tick(&method, &headers, bytes).await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  run(service_fn(handler)).await
}

#[cfg(test)]
mod tests {
  use super::*;

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

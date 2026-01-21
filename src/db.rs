use chrono::{DateTime, Datelike, TimeZone, Utc};
use sqlx::{mysql::MySqlPoolOptions, MySqlPool};
use tokio::sync::OnceCell;
use vercel_runtime::Error;

static POOL: OnceCell<MySqlPool> = OnceCell::const_new();

#[derive(Debug, Clone)]
pub struct UsageEventRow {
  pub provider: String,
  pub model: String,
  pub prompt_tokens: i32,
  pub completion_tokens: i32,
  pub cost_usd: f64,
}

fn utc_day_bounds(now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
  let day_start = Utc
    .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
    .single()
    .unwrap_or_else(|| Utc.timestamp_opt(now.timestamp(), 0).single().unwrap());
  (day_start, day_start + chrono::Duration::days(1))
}

async fn ensure_schema(pool: &MySqlPool) -> Result<(), Error> {
  // Keep schema creation idempotent; avoids footguns in early MVP.
  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS usage_events (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        event_type VARCHAR(64) NOT NULL,
        idempotency_key VARCHAR(128) NOT NULL,
        provider VARCHAR(32) NOT NULL,
        model VARCHAR(64) NOT NULL,
        prompt_tokens INT NOT NULL,
        completion_tokens INT NOT NULL,
        cost_usd DECIMAL(12,6) NOT NULL,
        occurred_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_usage_events_idem (tenant_id, event_type, idempotency_key),
        KEY idx_usage_events_day (tenant_id, occurred_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS usage_daily_counters (
        tenant_id VARCHAR(128) NOT NULL,
        day_key DATE NOT NULL,
        event_type VARCHAR(64) NOT NULL,
        used INT NOT NULL,
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        PRIMARY KEY (tenant_id, day_key, event_type)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS channel_connections (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        oauth_provider VARCHAR(32) NOT NULL,
        channel_id VARCHAR(128) NULL,
        access_token TEXT NOT NULL,
        refresh_token TEXT NULL,
        token_type VARCHAR(32) NOT NULL,
        scope TEXT NULL,
        expires_at TIMESTAMP(3) NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_channel_connections_provider (tenant_id, oauth_provider),
        KEY idx_channel_connections_updated (tenant_id, updated_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS tenant_trials (
        tenant_id VARCHAR(128) PRIMARY KEY,
        trial_started_at_ms BIGINT NOT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS job_tasks (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        job_type VARCHAR(32) NOT NULL,
        channel_id VARCHAR(128) NOT NULL,
        run_for_dt DATE NULL,
        dedupe_key VARCHAR(256) NOT NULL,
        status VARCHAR(16) NOT NULL DEFAULT 'pending',
        attempt INT NOT NULL DEFAULT 0,
        max_attempt INT NOT NULL DEFAULT 3,
        run_after TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        locked_by VARCHAR(128) NULL,
        locked_at TIMESTAMP(3) NULL,
        last_error TEXT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_job_tasks_dedupe (dedupe_key),
        KEY idx_job_tasks_claim (status, run_after),
        KEY idx_job_tasks_tenant (tenant_id, channel_id, run_for_dt)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS decision_daily (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        channel_id VARCHAR(128) NOT NULL,
        as_of_dt DATE NOT NULL,
        direction VARCHAR(16) NOT NULL,
        confidence DOUBLE NOT NULL,
        evidence_json TEXT NOT NULL,
        forbidden_json TEXT NOT NULL,
        reevaluate_json TEXT NOT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_decision_daily (tenant_id, channel_id, as_of_dt),
        KEY idx_decision_daily_created (tenant_id, channel_id, created_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS billing_events (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        provider VARCHAR(32) NOT NULL,
        provider_event_id VARCHAR(128) NOT NULL,
        topic VARCHAR(128) NOT NULL,
        shop_domain VARCHAR(255) NULL,
        raw_payload TEXT NOT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_billing_events_provider (provider, provider_event_id),
        KEY idx_billing_events_topic (provider, topic, created_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS video_daily_metrics (
        tenant_id VARCHAR(128) NOT NULL,
        channel_id VARCHAR(128) NOT NULL,
        dt DATE NOT NULL,
        video_id VARCHAR(128) NOT NULL,
        estimated_revenue_usd DECIMAL(12,6) NOT NULL DEFAULT 0,
        impressions BIGINT NOT NULL DEFAULT 0,
        views BIGINT NOT NULL DEFAULT 0,
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        PRIMARY KEY (tenant_id, channel_id, dt, video_id),
        KEY idx_video_daily_metrics_day (tenant_id, channel_id, dt),
        KEY idx_video_daily_metrics_video (tenant_id, channel_id, video_id, dt)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS sync_run_log (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        channel_id VARCHAR(128) NOT NULL,
        start_dt DATE NOT NULL,
        end_dt DATE NOT NULL,
        status VARCHAR(16) NOT NULL,
        started_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        finished_at TIMESTAMP(3) NULL,
        error TEXT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        KEY idx_sync_run_log_tenant (tenant_id, channel_id, started_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS observed_actions (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        channel_id VARCHAR(128) NOT NULL,
        dt DATE NOT NULL,
        action_type VARCHAR(64) NOT NULL,
        action_meta_json TEXT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_observed_actions (tenant_id, channel_id, dt, action_type),
        KEY idx_observed_actions_day (tenant_id, channel_id, dt)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS decision_outcome (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        channel_id VARCHAR(128) NOT NULL,
        decision_dt DATE NOT NULL,
        outcome_dt DATE NOT NULL,
        revenue_change_pct_7d DOUBLE NULL,
        catastrophic_flag TINYINT NOT NULL DEFAULT 0,
        new_top_asset_flag TINYINT NOT NULL DEFAULT 0,
        notes TEXT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_decision_outcome (tenant_id, channel_id, decision_dt, outcome_dt),
        KEY idx_decision_outcome_channel (tenant_id, channel_id, outcome_dt)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS policy_params (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        channel_id VARCHAR(128) NOT NULL,
        version VARCHAR(64) NOT NULL,
        params_json TEXT NOT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        created_by VARCHAR(32) NOT NULL DEFAULT 'system',
        UNIQUE KEY uq_policy_params (tenant_id, channel_id, version)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS policy_eval_report (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        channel_id VARCHAR(128) NOT NULL,
        candidate_version VARCHAR(64) NOT NULL,
        replay_metrics_json TEXT NOT NULL,
        approved TINYINT NOT NULL DEFAULT 0,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_policy_eval_report (tenant_id, channel_id, candidate_version)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS plans (
        id VARCHAR(64) PRIMARY KEY,
        name VARCHAR(128) NOT NULL,
        entitlements_json TEXT NOT NULL,
        shopify_product_id VARCHAR(64) NULL,
        shopify_variant_id VARCHAR(64) NULL,
        shopify_selling_plan_id VARCHAR(64) NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS subscriptions (
        tenant_id VARCHAR(128) PRIMARY KEY,
        status VARCHAR(16) NOT NULL,
        provider VARCHAR(32) NOT NULL DEFAULT 'shopify',
        provider_customer_id VARCHAR(128) NULL,
        provider_subscription_id VARCHAR(128) NULL,
        current_period_end TIMESTAMP(3) NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        KEY idx_subscriptions_status (status, updated_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS entitlements (
        tenant_id VARCHAR(128) PRIMARY KEY,
        overrides_json TEXT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  // Best-effort schema upgrades for existing tables (TiDB supports IF NOT EXISTS).
  sqlx::query(
    r#"
      ALTER TABLE channel_connections
      ADD COLUMN IF NOT EXISTS channel_id VARCHAR(128) NULL;
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

pub async fn get_pool() -> Result<&'static MySqlPool, Error> {
  POOL
    .get_or_try_init(|| async {
      let url = std::env::var("TIDB_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map_err(|_| -> Error {
          Box::new(std::io::Error::other(
            "Missing TIDB_DATABASE_URL (or DATABASE_URL)",
          ))
        })?;

      let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .map_err(|e| -> Error { Box::new(e) })?;

      ensure_schema(&pool).await?;
      Ok::<_, Error>(pool)
    })
    .await
}

pub async fn sum_spent_usd_today(pool: &MySqlPool, tenant_id: &str, now: DateTime<Utc>) -> Result<f64, Error> {
  let (start, end) = utc_day_bounds(now);

  let spent: f64 = sqlx::query_scalar(
    r#"
      SELECT COALESCE(CAST(SUM(cost_usd) AS DOUBLE), 0) AS spent_usd
      FROM usage_events
      WHERE tenant_id = ?
        AND occurred_at >= ? AND occurred_at < ?;
    "#,
  )
  .bind(tenant_id)
  .bind(start)
  .bind(end)
  .fetch_one(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(spent)
}

pub async fn fetch_usage_event(
  pool: &MySqlPool,
  tenant_id: &str,
  event_type: &str,
  idempotency_key: &str,
) -> Result<Option<UsageEventRow>, Error> {
  let row = sqlx::query_as::<_, (String, String, i32, i32, f64)>(
    r#"
      SELECT provider, model, prompt_tokens, completion_tokens, CAST(cost_usd AS DOUBLE) AS cost_usd
      FROM usage_events
      WHERE tenant_id = ? AND event_type = ? AND idempotency_key = ?
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .bind(event_type)
  .bind(idempotency_key)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.map(|(provider, model, prompt_tokens, completion_tokens, cost_usd)| UsageEventRow {
    provider,
    model,
    prompt_tokens,
    completion_tokens,
    cost_usd,
  }))
}

pub async fn fetch_daily_usage_used(
  pool: &MySqlPool,
  tenant_id: &str,
  event_type: &str,
  day: chrono::NaiveDate,
) -> Result<i64, Error> {
  let used = sqlx::query_scalar::<_, i64>(
    r#"
      SELECT CAST(used AS SIGNED) AS used
      FROM usage_daily_counters
      WHERE tenant_id = ? AND day_key = ? AND event_type = ?
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .bind(day)
  .bind(event_type)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?
  .unwrap_or(0);

  Ok(used)
}

pub struct ConsumeDailyUsageResult {
  pub day_key: String,
  pub used: i64,
  pub allowed: bool,
}

pub async fn consume_daily_usage_event(
  pool: &MySqlPool,
  tenant_id: &str,
  event_type: &str,
  idempotency_key: &str,
  limit: i64,
  now: DateTime<Utc>,
) -> Result<ConsumeDailyUsageResult, Error> {
  let day = now.date_naive();
  let day_key = day.format("%Y-%m-%d").to_string();

  let mut tx = pool
    .begin()
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      INSERT INTO usage_daily_counters (tenant_id, day_key, event_type, used)
      VALUES (?, ?, ?, 0)
      ON DUPLICATE KEY UPDATE used = used;
    "#,
  )
  .bind(tenant_id)
  .bind(day)
  .bind(event_type)
  .execute(&mut *tx)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let used: i64 = sqlx::query_scalar(
    r#"
      SELECT CAST(used AS SIGNED) AS used
      FROM usage_daily_counters
      WHERE tenant_id = ? AND day_key = ? AND event_type = ?
      FOR UPDATE;
    "#,
  )
  .bind(tenant_id)
  .bind(day)
  .bind(event_type)
  .fetch_one(&mut *tx)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let insert_result = sqlx::query(
    r#"
      INSERT INTO usage_events
        (tenant_id, event_type, idempotency_key, provider, model, prompt_tokens, completion_tokens, cost_usd)
      VALUES
        (?, ?, ?, 'yra', 'count', 0, 0, 0);
    "#,
  )
  .bind(tenant_id)
  .bind(event_type)
  .bind(idempotency_key)
  .execute(&mut *tx)
  .await;

  match insert_result {
    Ok(_) => {
      if used >= limit {
        tx.rollback().await.map_err(|e| -> Error { Box::new(e) })?;
        return Ok(ConsumeDailyUsageResult {
          day_key,
          used,
          allowed: false,
        });
      }

      sqlx::query(
        r#"
          UPDATE usage_daily_counters
          SET used = used + 1
          WHERE tenant_id = ? AND day_key = ? AND event_type = ?;
        "#,
      )
      .bind(tenant_id)
      .bind(day)
      .bind(event_type)
      .execute(&mut *tx)
      .await
      .map_err(|e| -> Error { Box::new(e) })?;

      tx.commit().await.map_err(|e| -> Error { Box::new(e) })?;

      Ok(ConsumeDailyUsageResult {
        day_key,
        used: used + 1,
        allowed: true,
      })
    }
    Err(err) => {
      if err.as_database_error().is_some_and(|e| e.is_unique_violation()) {
        tx.commit().await.map_err(|e| -> Error { Box::new(e) })?;
        return Ok(ConsumeDailyUsageResult {
          day_key,
          used,
          allowed: true,
        });
      }

      tx.rollback().await.map_err(|e| -> Error { Box::new(e) })?;
      Err(Box::new(err))
    }
  }
}

pub async fn insert_usage_event(
  pool: &MySqlPool,
  tenant_id: &str,
  event_type: &str,
  idempotency_key: &str,
  provider: &str,
  model: &str,
  prompt_tokens: i32,
  completion_tokens: i32,
  cost_usd: f64,
) -> Result<(), sqlx::Error> {
  sqlx::query(
    r#"
      INSERT INTO usage_events
        (tenant_id, event_type, idempotency_key, provider, model, prompt_tokens, completion_tokens, cost_usd)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?);
    "#,
  )
  .bind(tenant_id)
  .bind(event_type)
  .bind(idempotency_key)
  .bind(provider)
  .bind(model)
  .bind(prompt_tokens)
  .bind(completion_tokens)
  .bind(cost_usd)
  .execute(pool)
  .await?;

  Ok(())
}

pub async fn ensure_trial_started(
  pool: &MySqlPool,
  tenant_id: &str,
  now_ms: i64,
) -> Result<i64, Error> {
  sqlx::query(
    r#"
      INSERT INTO tenant_trials (tenant_id, trial_started_at_ms)
      VALUES (?, ?)
      ON DUPLICATE KEY UPDATE trial_started_at_ms = trial_started_at_ms;
    "#,
  )
  .bind(tenant_id)
  .bind(now_ms)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let trial_started_at_ms: i64 = sqlx::query_scalar(
    r#"
      SELECT trial_started_at_ms
      FROM tenant_trials
      WHERE tenant_id = ?
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .fetch_one(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(trial_started_at_ms)
}

pub async fn fetch_youtube_channel_id(
  pool: &MySqlPool,
  tenant_id: &str,
) -> Result<Option<String>, Error> {
  let row = sqlx::query_as::<_, (Option<String>,)>(
    r#"
      SELECT channel_id
      FROM channel_connections
      WHERE tenant_id = ? AND oauth_provider = 'youtube'
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.and_then(|(channel_id,)| channel_id))
}

#[derive(Debug, Clone)]
pub struct YoutubeConnectionTokens {
  pub access_token: String,
  pub refresh_token: Option<String>,
  pub expires_at: Option<DateTime<Utc>>,
}

pub async fn fetch_youtube_connection_tokens(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
) -> Result<Option<YoutubeConnectionTokens>, Error> {
  let row = sqlx::query_as::<_, (String, Option<String>, Option<DateTime<Utc>>)>(
    r#"
      SELECT access_token, refresh_token, expires_at
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
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.map(|(access_token, refresh_token, expires_at)| YoutubeConnectionTokens {
    access_token,
    refresh_token,
    expires_at,
  }))
}

pub async fn update_youtube_connection_tokens(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  tokens: &crate::providers::youtube::YoutubeOAuthTokens,
) -> Result<(), Error> {
  let expires_at = tokens
    .expires_in_seconds
    .map(|secs| chrono::Utc::now() + chrono::Duration::seconds(secs as i64));

  sqlx::query(
    r#"
      UPDATE channel_connections
      SET access_token = ?,
          refresh_token = COALESCE(?, refresh_token),
          token_type = ?,
          scope = ?,
          expires_at = ?,
          updated_at = CURRENT_TIMESTAMP(3)
      WHERE tenant_id = ?
        AND oauth_provider = 'youtube'
        AND channel_id = ?;
    "#,
  )
  .bind(&tokens.access_token)
  .bind(tokens.refresh_token.as_deref())
  .bind(&tokens.token_type)
  .bind(tokens.scope.as_deref())
  .bind(expires_at)
  .bind(tenant_id)
  .bind(channel_id)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

pub async fn upsert_video_daily_metric(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  dt: chrono::NaiveDate,
  video_id: &str,
  estimated_revenue_usd: f64,
  impressions: i64,
  views: i64,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO video_daily_metrics
        (tenant_id, channel_id, dt, video_id, estimated_revenue_usd, impressions, views)
      VALUES
        (?, ?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        estimated_revenue_usd = VALUES(estimated_revenue_usd),
        impressions = VALUES(impressions),
        views = VALUES(views),
        updated_at = CURRENT_TIMESTAMP(3);
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(dt)
  .bind(video_id)
  .bind(estimated_revenue_usd)
  .bind(impressions)
  .bind(views)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

pub async fn fetch_new_video_publish_counts_by_dt(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  start_dt: chrono::NaiveDate,
  end_dt: chrono::NaiveDate,
) -> Result<Vec<(chrono::NaiveDate, i64)>, Error> {
  let rows = sqlx::query_as::<_, (chrono::NaiveDate, i64)>(
    r#"
      SELECT first_dt AS dt, COUNT(*) AS new_videos
      FROM (
        SELECT video_id, MIN(dt) AS first_dt
        FROM video_daily_metrics
        WHERE tenant_id = ?
          AND channel_id = ?
        GROUP BY video_id
      ) AS v
      WHERE first_dt BETWEEN ? AND ?
      GROUP BY first_dt
      ORDER BY first_dt ASC;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(start_dt)
  .bind(end_dt)
  .fetch_all(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(rows)
}

pub async fn upsert_observed_action(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  dt: chrono::NaiveDate,
  action_type: &str,
  action_meta_json: Option<&str>,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO observed_actions
        (tenant_id, channel_id, dt, action_type, action_meta_json)
      VALUES
        (?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        action_meta_json = VALUES(action_meta_json);
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(dt)
  .bind(action_type)
  .bind(action_meta_json)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

pub async fn decision_daily_exists(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  as_of_dt: chrono::NaiveDate,
) -> Result<bool, Error> {
  let row = sqlx::query_as::<_, (i32,)>(
    r#"
      SELECT 1
      FROM decision_daily
      WHERE tenant_id = ?
        AND channel_id = ?
        AND as_of_dt = ?
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(as_of_dt)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.is_some())
}

pub async fn fetch_revenue_sum_usd_7d(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  start_dt: chrono::NaiveDate,
  end_dt: chrono::NaiveDate,
) -> Result<f64, Error> {
  let (sum_usd,): (f64,) = sqlx::query_as(
    r#"
      SELECT COALESCE(SUM(CAST(estimated_revenue_usd AS DOUBLE)), 0) AS revenue_sum_usd
      FROM video_daily_metrics
      WHERE tenant_id = ?
        AND channel_id = ?
        AND dt BETWEEN ? AND ?;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(start_dt)
  .bind(end_dt)
  .fetch_one(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(sum_usd)
}

pub async fn fetch_top_video_ids_by_revenue(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  start_dt: chrono::NaiveDate,
  end_dt: chrono::NaiveDate,
  limit: i64,
) -> Result<Vec<String>, Error> {
  let limit = limit.clamp(1, 50);
  let rows = sqlx::query_as::<_, (String,)>(
    r#"
      SELECT video_id
      FROM video_daily_metrics
      WHERE tenant_id = ?
        AND channel_id = ?
        AND dt BETWEEN ? AND ?
      GROUP BY video_id
      ORDER BY SUM(CAST(estimated_revenue_usd AS DOUBLE)) DESC
      LIMIT ?;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(start_dt)
  .bind(end_dt)
  .bind(limit)
  .fetch_all(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(rows.into_iter().map(|(video_id,)| video_id).collect())
}

pub async fn upsert_decision_outcome(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  decision_dt: chrono::NaiveDate,
  outcome_dt: chrono::NaiveDate,
  revenue_change_pct_7d: Option<f64>,
  catastrophic_flag: bool,
  new_top_asset_flag: bool,
  notes: Option<&str>,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO decision_outcome
        (tenant_id, channel_id, decision_dt, outcome_dt, revenue_change_pct_7d, catastrophic_flag, new_top_asset_flag, notes)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        revenue_change_pct_7d = VALUES(revenue_change_pct_7d),
        catastrophic_flag = VALUES(catastrophic_flag),
        new_top_asset_flag = VALUES(new_top_asset_flag),
        notes = VALUES(notes);
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(decision_dt)
  .bind(outcome_dt)
  .bind(revenue_change_pct_7d)
  .bind(if catastrophic_flag { 1 } else { 0 })
  .bind(if new_top_asset_flag { 1 } else { 0 })
  .bind(notes)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

pub async fn fetch_policy_params_json(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  version: &str,
) -> Result<Option<String>, Error> {
  let row = sqlx::query_as::<_, (String,)>(
    r#"
      SELECT params_json
      FROM policy_params
      WHERE tenant_id = ?
        AND channel_id = ?
        AND version = ?
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(version)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.map(|(json,)| json))
}

pub async fn upsert_policy_params(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  version: &str,
  params_json: &str,
  created_by: &str,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO policy_params
        (tenant_id, channel_id, version, params_json, created_by)
      VALUES
        (?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        params_json = VALUES(params_json),
        created_by = VALUES(created_by);
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(version)
  .bind(params_json)
  .bind(created_by)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

pub async fn upsert_policy_eval_report(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  candidate_version: &str,
  replay_metrics_json: &str,
  approved: bool,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO policy_eval_report
        (tenant_id, channel_id, candidate_version, replay_metrics_json, approved)
      VALUES
        (?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        replay_metrics_json = VALUES(replay_metrics_json),
        approved = VALUES(approved);
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(candidate_version)
  .bind(replay_metrics_json)
  .bind(if approved { 1 } else { 0 })
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

#[derive(Debug, Clone)]
pub struct SubscriptionRow {
  pub status: String,
  pub current_period_end: Option<DateTime<Utc>>,
}

pub async fn fetch_subscription(
  pool: &MySqlPool,
  tenant_id: &str,
) -> Result<Option<SubscriptionRow>, Error> {
  let row = sqlx::query_as::<_, (String, Option<DateTime<Utc>>)>(
    r#"
      SELECT status, current_period_end
      FROM subscriptions
      WHERE tenant_id = ?
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.map(|(status, current_period_end)| SubscriptionRow {
    status,
    current_period_end,
  }))
}

pub async fn upsert_subscription(
  pool: &MySqlPool,
  tenant_id: &str,
  status: &str,
  provider_customer_id: Option<&str>,
  provider_subscription_id: Option<&str>,
  current_period_end: Option<DateTime<Utc>>,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO subscriptions
        (tenant_id, status, provider, provider_customer_id, provider_subscription_id, current_period_end)
      VALUES
        (?, ?, 'shopify', ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        status = VALUES(status),
        provider_customer_id = COALESCE(VALUES(provider_customer_id), provider_customer_id),
        provider_subscription_id = COALESCE(VALUES(provider_subscription_id), provider_subscription_id),
        current_period_end = COALESCE(VALUES(current_period_end), current_period_end),
        updated_at = CURRENT_TIMESTAMP(3);
    "#,
  )
  .bind(tenant_id)
  .bind(status)
  .bind(provider_customer_id)
  .bind(provider_subscription_id)
  .bind(current_period_end)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

pub async fn upsert_youtube_connection(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  tokens: &crate::providers::youtube::YoutubeOAuthTokens,
) -> Result<(), sqlx::Error> {
  let expires_at = tokens
    .expires_in_seconds
    .map(|secs| chrono::Utc::now() + chrono::Duration::seconds(secs as i64));

  sqlx::query(
    r#"
      INSERT INTO channel_connections
        (tenant_id, oauth_provider, channel_id, access_token, refresh_token, token_type, scope, expires_at)
      VALUES
        (?, 'youtube', ?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        channel_id = VALUES(channel_id),
        access_token = VALUES(access_token),
        refresh_token = COALESCE(VALUES(refresh_token), refresh_token),
        token_type = VALUES(token_type),
        scope = VALUES(scope),
        expires_at = VALUES(expires_at),
        updated_at = CURRENT_TIMESTAMP(3);
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(&tokens.access_token)
  .bind(tokens.refresh_token.as_deref())
  .bind(&tokens.token_type)
  .bind(tokens.scope.as_deref())
  .bind(expires_at)
  .execute(pool)
  .await?;

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn utc_day_bounds_returns_midnight_and_next_midnight() {
    let now = Utc.with_ymd_and_hms(2026, 1, 20, 16, 30, 0).unwrap();
    let (start, end) = utc_day_bounds(now);
    assert_eq!(start, Utc.with_ymd_and_hms(2026, 1, 20, 0, 0, 0).unwrap());
    assert_eq!(end, Utc.with_ymd_and_hms(2026, 1, 21, 0, 0, 0).unwrap());
  }
}

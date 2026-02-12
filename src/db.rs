use chrono::{DateTime, Datelike, TimeZone, Utc};
use sqlx::{mysql::MySqlPoolOptions, MySqlPool};
use std::collections::HashMap;
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
        content_owner_id VARCHAR(128) NULL,
        access_token TEXT NOT NULL,
        refresh_token TEXT NULL,
        token_type VARCHAR(32) NOT NULL,
        scope TEXT NULL,
        expires_at TIMESTAMP(3) NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_channel_connections_provider (tenant_id, oauth_provider),
        KEY idx_channel_connections_owner (tenant_id, content_owner_id),
        KEY idx_channel_connections_updated (tenant_id, updated_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  // Per-tenant OAuth app configuration (BYO OAuth client).
  // Note: `client_secret` is sensitive. For now we store it like other tokens (plaintext),
  // but in production you likely want to encrypt it with a KMS/master key.
  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS oauth_apps (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        provider VARCHAR(32) NOT NULL,
        client_id VARCHAR(255) NOT NULL,
        client_secret TEXT NULL,
        redirect_uri VARCHAR(512) NOT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_oauth_apps_provider (tenant_id, provider),
        KEY idx_oauth_apps_updated (tenant_id, updated_at)
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
        impressions_ctr DOUBLE NULL,
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

  // CSV uploads (Studio export fallback) - raw file is handled on the frontend,
  // backend persists parsed status + writes rows into `video_daily_metrics`.
  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS yt_csv_uploads (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        channel_id VARCHAR(128) NOT NULL,
        filename VARCHAR(512) NOT NULL,
        rows_parsed INT NOT NULL DEFAULT 0,
        status VARCHAR(16) NOT NULL DEFAULT 'received',
        error TEXT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        KEY idx_yt_csv_uploads_tenant (tenant_id, channel_id, created_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  // Agent alerts (derived from stored metrics or external signals).
  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS yt_alerts (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        channel_id VARCHAR(128) NOT NULL,
        alert_key VARCHAR(64) NOT NULL,
        kind VARCHAR(128) NOT NULL,
        severity VARCHAR(16) NOT NULL,
        message TEXT NOT NULL,
        detected_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        resolved_at TIMESTAMP(3) NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_yt_alerts_key (tenant_id, channel_id, alert_key),
        KEY idx_yt_alerts_open (tenant_id, channel_id, resolved_at, detected_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  // Experiments (MVP: persisted experiment definitions + variants).
  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS yt_experiments (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        channel_id VARCHAR(128) NOT NULL,
        type VARCHAR(32) NOT NULL,
        state VARCHAR(16) NOT NULL DEFAULT 'running',
        video_ids_json TEXT NOT NULL,
        stop_loss_pct DOUBLE NULL,
        planned_duration_days INT NULL,
        started_at TIMESTAMP(3) NULL,
        ended_at TIMESTAMP(3) NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        KEY idx_yt_experiments_tenant (tenant_id, channel_id, created_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS yt_experiment_variants (
        experiment_id BIGINT NOT NULL,
        variant_id VARCHAR(32) NOT NULL,
        payload_json TEXT NOT NULL,
        status VARCHAR(16) NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        PRIMARY KEY (experiment_id, variant_id),
        KEY idx_yt_experiment_variants_exp (experiment_id, created_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  // YouTube Reporting / Content ID ingestion tables (raw blobs + metadata).
  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS yt_reporting_report_types (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        content_owner_id VARCHAR(128) NOT NULL,
        report_type_id VARCHAR(256) NOT NULL,
        report_type_name VARCHAR(512) NULL,
        system_managed TINYINT NOT NULL DEFAULT 0,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_yt_reporting_report_types (content_owner_id, report_type_id),
        KEY idx_yt_reporting_report_types_owner (content_owner_id, updated_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS yt_reporting_jobs (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        content_owner_id VARCHAR(128) NOT NULL,
        report_type_id VARCHAR(256) NOT NULL,
        job_id VARCHAR(256) NOT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_yt_reporting_jobs (tenant_id, content_owner_id, report_type_id),
        KEY idx_yt_reporting_jobs_tenant (tenant_id, content_owner_id, updated_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS yt_reporting_report_files (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        content_owner_id VARCHAR(128) NOT NULL,
        report_type_id VARCHAR(256) NOT NULL,
        job_id VARCHAR(256) NOT NULL,
        report_id VARCHAR(256) NOT NULL,
        download_url TEXT NULL,
        start_time TIMESTAMP(3) NULL,
        end_time TIMESTAMP(3) NULL,
        create_time TIMESTAMP(3) NULL,
        raw_sha256 CHAR(64) NULL,
        raw_bytes LONGBLOB NULL,
        raw_bytes_len BIGINT NULL,
        downloaded_at TIMESTAMP(3) NULL,
        parse_status VARCHAR(16) NOT NULL DEFAULT 'pending',
        parse_version VARCHAR(32) NULL,
        parsed_at TIMESTAMP(3) NULL,
        parse_error TEXT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_yt_reporting_report_files (tenant_id, content_owner_id, report_id),
        KEY idx_yt_reporting_report_files_job (tenant_id, content_owner_id, job_id, created_at),
        KEY idx_yt_reporting_report_files_status (parse_status, updated_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS yt_reporting_wide_tables (
        report_type_id VARCHAR(256) PRIMARY KEY,
        table_name VARCHAR(128) NOT NULL,
        columns_json LONGTEXT NOT NULL,
        parse_version VARCHAR(32) NOT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_yt_reporting_wide_tables_name (table_name)
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

  // Public share links for proof reports (HTML snapshots).
  // Purpose: send to brands/partners without requiring login.
  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS yt_report_shares (
        token CHAR(32) PRIMARY KEY,
        tenant_id VARCHAR(128) NOT NULL,
        channel_id VARCHAR(128) NOT NULL,
        start_dt DATE NOT NULL,
        end_dt DATE NOT NULL,
        filename VARCHAR(256) NULL,
        html LONGTEXT NOT NULL,
        hits BIGINT NOT NULL DEFAULT 0,
        expires_at TIMESTAMP(3) NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        KEY idx_yt_report_shares_lookup (tenant_id, channel_id, created_at),
        KEY idx_yt_report_shares_expiry (expires_at)
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

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS tenant_llm_settings (
        tenant_id VARCHAR(128) PRIMARY KEY,
        gemini_model VARCHAR(128) NOT NULL,
        updated_by VARCHAR(64) NOT NULL DEFAULT 'system',
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
      CREATE TABLE IF NOT EXISTS geo_monitor_projects (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        name VARCHAR(256) NOT NULL,
        website VARCHAR(512) NULL,
        brand_aliases_json TEXT NULL,
        competitor_names_json TEXT NULL,
        schedule VARCHAR(16) NOT NULL DEFAULT 'weekly',
        enabled TINYINT NOT NULL DEFAULT 1,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        KEY idx_geo_monitor_projects_tenant (tenant_id, updated_at)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS geo_monitor_prompts (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        project_id BIGINT NOT NULL,
        theme VARCHAR(128) NULL,
        prompt_text TEXT NOT NULL,
        enabled TINYINT NOT NULL DEFAULT 1,
        sort_order INT NOT NULL DEFAULT 0,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        KEY idx_geo_monitor_prompts_project (tenant_id, project_id, sort_order),
        KEY idx_geo_monitor_prompts_id (tenant_id, id)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS geo_monitor_runs (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        project_id BIGINT NOT NULL,
        run_for_dt DATE NOT NULL,
        provider VARCHAR(32) NOT NULL,
        model VARCHAR(64) NOT NULL,
        status VARCHAR(16) NOT NULL DEFAULT 'running',
        prompt_total INT NOT NULL DEFAULT 0,
        started_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        finished_at TIMESTAMP(3) NULL,
        last_error TEXT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_geo_monitor_runs (tenant_id, project_id, run_for_dt),
        KEY idx_geo_monitor_runs_project (tenant_id, project_id, run_for_dt)
      );
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      CREATE TABLE IF NOT EXISTS geo_monitor_run_results (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        tenant_id VARCHAR(128) NOT NULL,
        project_id BIGINT NOT NULL,
        run_for_dt DATE NOT NULL,
        run_id BIGINT NOT NULL,
        prompt_id BIGINT NOT NULL,
        prompt_text LONGTEXT NOT NULL,
        output_text LONGTEXT NULL,
        presence TINYINT NOT NULL DEFAULT 0,
        rank_int INT NULL,
        cost_usd DECIMAL(12,6) NOT NULL DEFAULT 0,
        error LONGTEXT NULL,
        created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_geo_monitor_results (tenant_id, project_id, run_for_dt, prompt_id),
        KEY idx_geo_monitor_results_run (run_id),
        KEY idx_geo_monitor_results_project (tenant_id, project_id, run_for_dt)
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

  sqlx::query(
    r#"
      ALTER TABLE channel_connections
      ADD COLUMN IF NOT EXISTS content_owner_id VARCHAR(128) NULL;
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      ALTER TABLE yt_alerts
      ADD COLUMN IF NOT EXISTS details_json TEXT NULL;
    "#,
  )
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      ALTER TABLE video_daily_metrics
      ADD COLUMN IF NOT EXISTS impressions_ctr DOUBLE NULL;
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
      WHERE tenant_id = ?
        AND oauth_provider = 'youtube'
        AND channel_id IS NOT NULL
        AND channel_id <> ''
      ORDER BY updated_at DESC
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.and_then(|(channel_id,)| channel_id))
}

pub async fn fetch_youtube_content_owner_id(
  pool: &MySqlPool,
  tenant_id: &str,
) -> Result<Option<String>, Error> {
  let row = sqlx::query_as::<_, (Option<String>,)>(
    r#"
      SELECT content_owner_id
      FROM channel_connections
      WHERE tenant_id = ?
        AND oauth_provider = 'youtube'
        AND content_owner_id IS NOT NULL
        AND content_owner_id <> ''
      ORDER BY updated_at DESC
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.and_then(|(content_owner_id,)| content_owner_id))
}

pub async fn set_youtube_channel_id(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      UPDATE channel_connections
      SET channel_id = ?,
          updated_at = CURRENT_TIMESTAMP(3)
      WHERE tenant_id = ? AND oauth_provider = 'youtube';
    "#,
  )
  .bind(channel_id)
  .bind(tenant_id)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

pub async fn set_youtube_content_owner_id(
  pool: &MySqlPool,
  tenant_id: &str,
  content_owner_id: Option<&str>,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      UPDATE channel_connections
      SET content_owner_id = ?
      WHERE tenant_id = ? AND oauth_provider = 'youtube';
    "#,
  )
  .bind(content_owner_id)
  .bind(tenant_id)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

#[derive(Debug, Clone)]
pub struct YoutubeOAuthAppConfig {
  pub client_id: String,
  pub client_secret: Option<String>,
  pub redirect_uri: String,
}

pub async fn fetch_youtube_oauth_app_config(
  pool: &MySqlPool,
  tenant_id: &str,
) -> Result<Option<YoutubeOAuthAppConfig>, Error> {
  let row = sqlx::query_as::<_, (String, Option<String>, String)>(
    r#"
      SELECT client_id, client_secret, redirect_uri
      FROM oauth_apps
      WHERE tenant_id = ? AND provider = 'youtube'
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.map(|(client_id, client_secret, redirect_uri)| YoutubeOAuthAppConfig {
    client_id,
    client_secret,
    redirect_uri,
  }))
}

pub async fn upsert_youtube_oauth_app_config(
  pool: &MySqlPool,
  tenant_id: &str,
  client_id: &str,
  client_secret: Option<&str>,
  redirect_uri: &str,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO oauth_apps (tenant_id, provider, client_id, client_secret, redirect_uri)
      VALUES (?, 'youtube', ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        client_id = VALUES(client_id),
        client_secret = COALESCE(VALUES(client_secret), client_secret),
        redirect_uri = VALUES(redirect_uri),
        updated_at = CURRENT_TIMESTAMP(3);
    "#,
  )
  .bind(tenant_id)
  .bind(client_id)
  .bind(client_secret)
  .bind(redirect_uri)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

pub fn youtube_oauth_app_config_from_env() -> Result<YoutubeOAuthAppConfig, Error> {
  let client_id = std::env::var("YOUTUBE_CLIENT_ID")
    .map_err(|_| Box::new(std::io::Error::other("Missing YOUTUBE_CLIENT_ID")) as Error)?;
  let client_secret = std::env::var("YOUTUBE_CLIENT_SECRET")
    .map_err(|_| Box::new(std::io::Error::other("Missing YOUTUBE_CLIENT_SECRET")) as Error)?;
  let redirect_uri = std::env::var("YOUTUBE_REDIRECT_URI")
    .map_err(|_| Box::new(std::io::Error::other("Missing YOUTUBE_REDIRECT_URI")) as Error)?;

  let client_id = client_id.trim().to_string();
  let client_secret = client_secret.trim().to_string();
  let redirect_uri = redirect_uri.trim().to_string();

  if client_id.is_empty() {
    return Err(Box::new(std::io::Error::other("Missing YOUTUBE_CLIENT_ID")) as Error);
  }
  if client_secret.is_empty() {
    return Err(Box::new(std::io::Error::other("Missing YOUTUBE_CLIENT_SECRET")) as Error);
  }
  if redirect_uri.is_empty() {
    return Err(Box::new(std::io::Error::other("Missing YOUTUBE_REDIRECT_URI")) as Error);
  }

  Ok(YoutubeOAuthAppConfig {
    client_id,
    client_secret: Some(client_secret),
    redirect_uri,
  })
}

pub async fn fetch_or_seed_youtube_oauth_app_config(
  pool: &MySqlPool,
  tenant_id: &str,
) -> Result<Option<YoutubeOAuthAppConfig>, Error> {
  let existing = fetch_youtube_oauth_app_config(pool, tenant_id).await?;
  if existing.is_some() {
    return Ok(existing);
  }

  let defaults = youtube_oauth_app_config_from_env();
  let Ok(defaults) = defaults else {
    return Ok(None);
  };

  let client_id = defaults.client_id.trim();
  let redirect_uri = defaults.redirect_uri.trim();
  let client_secret = defaults
    .client_secret
    .as_deref()
    .map(str::trim)
    .filter(|v| !v.is_empty());

  if client_id.is_empty() || redirect_uri.is_empty() || client_secret.is_none() {
    return Ok(None);
  }

  upsert_youtube_oauth_app_config(pool, tenant_id, client_id, client_secret, redirect_uri).await?;
  Ok(Some(defaults))
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
  impressions_ctr: Option<f64>,
  views: i64,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO video_daily_metrics
        (tenant_id, channel_id, dt, video_id, estimated_revenue_usd, impressions, impressions_ctr, views)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        estimated_revenue_usd = VALUES(estimated_revenue_usd),
        impressions = CASE WHEN VALUES(impressions) > 0 THEN VALUES(impressions) ELSE impressions END,
        impressions_ctr = COALESCE(VALUES(impressions_ctr), impressions_ctr),
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
  .bind(impressions_ctr)
  .bind(views)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(())
}

pub async fn upsert_video_daily_reach_metrics(
  pool: &MySqlPool,
  tenant_id: &str,
  channel_id: &str,
  dt: chrono::NaiveDate,
  video_id: &str,
  impressions: i64,
  impressions_ctr: Option<f64>,
  views: i64,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO video_daily_metrics
        (tenant_id, channel_id, dt, video_id, impressions, impressions_ctr, views)
      VALUES
        (?, ?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        impressions = VALUES(impressions),
        impressions_ctr = COALESCE(VALUES(impressions_ctr), impressions_ctr),
        views = CASE WHEN VALUES(views) > 0 THEN VALUES(views) ELSE views END,
        updated_at = CURRENT_TIMESTAMP(3);
    "#,
  )
  .bind(tenant_id)
  .bind(channel_id)
  .bind(dt)
  .bind(video_id)
  .bind(impressions)
  .bind(impressions_ctr)
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
          AND video_id NOT IN ('__CHANNEL_TOTAL__','csv_channel_total')
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
  let (total_rows, total_sum_usd): (i64, f64) = sqlx::query_as(
    r#"
      SELECT CAST(COUNT(*) AS SIGNED) AS rows_n,
             COALESCE(SUM(CAST(estimated_revenue_usd AS DOUBLE)), 0) AS revenue_sum_usd
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

  if total_rows > 0 {
    return Ok(total_sum_usd);
  }

  let (sum_usd,): (f64,) = sqlx::query_as(
    r#"
      SELECT COALESCE(SUM(CAST(estimated_revenue_usd AS DOUBLE)), 0) AS revenue_sum_usd
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
        AND video_id NOT IN ('__CHANNEL_TOTAL__','csv_channel_total')
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

pub async fn fetch_tenant_gemini_model(pool: &MySqlPool, tenant_id: &str) -> Result<Option<String>, Error> {
  let row = sqlx::query_as::<_, (String,)>(
    r#"
      SELECT gemini_model
      FROM tenant_llm_settings
      WHERE tenant_id = ?
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.map(|(model,)| model))
}

pub async fn upsert_tenant_gemini_model(
  pool: &MySqlPool,
  tenant_id: &str,
  model: &str,
  updated_by: &str,
) -> Result<(), Error> {
  sqlx::query(
    r#"
      INSERT INTO tenant_llm_settings
        (tenant_id, gemini_model, updated_by)
      VALUES
        (?, ?, ?)
      ON DUPLICATE KEY UPDATE
        gemini_model = VALUES(gemini_model),
        updated_by = VALUES(updated_by),
        updated_at = CURRENT_TIMESTAMP(3);
    "#,
  )
  .bind(tenant_id)
  .bind(model)
  .bind(updated_by)
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

#[derive(Debug, Clone)]
pub struct GeoMonitorProjectRow {
  pub id: i64,
  pub tenant_id: String,
  pub name: String,
  pub website: Option<String>,
  pub brand_aliases_json: Option<String>,
  pub competitor_names_json: Option<String>,
  pub schedule: String,
  pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct GeoMonitorPromptRow {
  pub id: i64,
  pub project_id: i64,
  pub theme: Option<String>,
  pub prompt_text: String,
  pub enabled: bool,
  pub sort_order: i32,
}

#[derive(Debug, Clone)]
pub struct GeoMonitorRunRow {
  pub id: i64,
  pub tenant_id: String,
  pub project_id: i64,
  pub run_for_dt: chrono::NaiveDate,
  pub provider: String,
  pub model: String,
  pub status: String,
  pub prompt_total: i32,
  pub started_at: DateTime<Utc>,
  pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct GeoMonitorRunSummary {
  pub results_total: i64,
  pub presence_count: i64,
  pub top3_count: i64,
  pub top5_count: i64,
  pub error_count: i64,
  pub cost_usd: f64,
}

pub async fn create_geo_monitor_project(
  pool: &MySqlPool,
  tenant_id: &str,
  name: &str,
  website: Option<&str>,
  brand_aliases_json: Option<&str>,
  competitor_names_json: Option<&str>,
  schedule: &str,
) -> Result<i64, Error> {
  let schedule = match schedule.trim() {
    "daily" | "Daily" | "DAILY" => "daily",
    _ => "weekly",
  };

  let res = sqlx::query(
    r#"
      INSERT INTO geo_monitor_projects
        (tenant_id, name, website, brand_aliases_json, competitor_names_json, schedule, enabled)
      VALUES
        (?, ?, ?, ?, ?, ?, 1);
    "#,
  )
  .bind(tenant_id)
  .bind(name)
  .bind(website)
  .bind(brand_aliases_json)
  .bind(competitor_names_json)
  .bind(schedule)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(res.last_insert_id() as i64)
}

pub async fn list_geo_monitor_projects(
  pool: &MySqlPool,
  tenant_id: &str,
) -> Result<Vec<GeoMonitorProjectRow>, Error> {
  let rows: Vec<(i64, String, String, Option<String>, Option<String>, Option<String>, String, i8)> =
    sqlx::query_as(
      r#"
        SELECT id, tenant_id, name, website, brand_aliases_json, competitor_names_json, schedule, enabled
        FROM geo_monitor_projects
        WHERE tenant_id = ?
        ORDER BY updated_at DESC, id DESC;
      "#,
    )
    .bind(tenant_id)
    .fetch_all(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

  Ok(
    rows
      .into_iter()
      .map(
        |(id, tenant_id, name, website, brand_aliases_json, competitor_names_json, schedule, enabled)| {
          GeoMonitorProjectRow {
            id,
            tenant_id,
            name,
            website,
            brand_aliases_json,
            competitor_names_json,
            schedule,
            enabled: enabled != 0,
          }
        },
      )
      .collect(),
  )
}

pub async fn fetch_geo_monitor_project(
  pool: &MySqlPool,
  tenant_id: &str,
  project_id: i64,
) -> Result<Option<GeoMonitorProjectRow>, Error> {
  let row: Option<(
    i64,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    i8,
  )> = sqlx::query_as(
    r#"
      SELECT id, tenant_id, name, website, brand_aliases_json, competitor_names_json, schedule, enabled
      FROM geo_monitor_projects
      WHERE tenant_id = ? AND id = ?
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .bind(project_id)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.map(
    |(id, tenant_id, name, website, brand_aliases_json, competitor_names_json, schedule, enabled)| {
      GeoMonitorProjectRow {
        id,
        tenant_id,
        name,
        website,
        brand_aliases_json,
        competitor_names_json,
        schedule,
        enabled: enabled != 0,
      }
    },
  ))
}

pub async fn replace_geo_monitor_prompts(
  pool: &MySqlPool,
  tenant_id: &str,
  project_id: i64,
  prompts: &[(Option<String>, String)],
) -> Result<(), Error> {
  let mut tx = pool.begin().await.map_err(|e| -> Error { Box::new(e) })?;

  sqlx::query(
    r#"
      DELETE FROM geo_monitor_prompts
      WHERE tenant_id = ? AND project_id = ?;
    "#,
  )
  .bind(tenant_id)
  .bind(project_id)
  .execute(&mut *tx)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  for (idx, (theme, prompt_text)) in prompts.iter().enumerate() {
    sqlx::query(
      r#"
        INSERT INTO geo_monitor_prompts
          (tenant_id, project_id, theme, prompt_text, enabled, sort_order)
        VALUES
          (?, ?, ?, ?, 1, ?);
      "#,
    )
    .bind(tenant_id)
    .bind(project_id)
    .bind(theme.as_deref())
    .bind(prompt_text)
    .bind(idx as i32)
    .execute(&mut *tx)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;
  }

  tx.commit().await.map_err(|e| -> Error { Box::new(e) })?;
  Ok(())
}

pub async fn list_geo_monitor_prompts(
  pool: &MySqlPool,
  tenant_id: &str,
  project_id: i64,
) -> Result<Vec<GeoMonitorPromptRow>, Error> {
  let rows: Vec<(i64, i64, Option<String>, String, i8, i32)> = sqlx::query_as(
    r#"
      SELECT id, project_id, theme, prompt_text, enabled, sort_order
      FROM geo_monitor_prompts
      WHERE tenant_id = ? AND project_id = ?
      ORDER BY sort_order ASC, id ASC;
    "#,
  )
  .bind(tenant_id)
  .bind(project_id)
  .fetch_all(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(
    rows
      .into_iter()
      .map(|(id, project_id, theme, prompt_text, enabled, sort_order)| GeoMonitorPromptRow {
        id,
        project_id,
        theme,
        prompt_text,
        enabled: enabled != 0,
        sort_order,
      })
      .collect(),
  )
}

pub async fn fetch_geo_monitor_prompt(
  pool: &MySqlPool,
  tenant_id: &str,
  project_id: i64,
  prompt_id: i64,
) -> Result<Option<GeoMonitorPromptRow>, Error> {
  let row: Option<(i64, i64, Option<String>, String, i8, i32)> = sqlx::query_as(
    r#"
      SELECT id, project_id, theme, prompt_text, enabled, sort_order
      FROM geo_monitor_prompts
      WHERE tenant_id = ? AND project_id = ? AND id = ?
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .bind(project_id)
  .bind(prompt_id)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.map(|(id, project_id, theme, prompt_text, enabled, sort_order)| GeoMonitorPromptRow {
    id,
    project_id,
    theme,
    prompt_text,
    enabled: enabled != 0,
    sort_order,
  }))
}

pub async fn ensure_geo_monitor_run(
  pool: &MySqlPool,
  tenant_id: &str,
  project_id: i64,
  run_for_dt: chrono::NaiveDate,
  provider: &str,
  model: &str,
  prompt_total: i32,
) -> Result<GeoMonitorRunRow, Error> {
  let existing: Option<(
    i64,
    String,
    i64,
    chrono::NaiveDate,
    String,
    String,
    String,
    i32,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
  )> = sqlx::query_as(
    r#"
      SELECT id, tenant_id, project_id, run_for_dt, provider, model, status, prompt_total, started_at, finished_at
      FROM geo_monitor_runs
      WHERE tenant_id = ? AND project_id = ? AND run_for_dt = ?
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .bind(project_id)
  .bind(run_for_dt)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  if let Some((
    id,
    tenant_id,
    project_id,
    run_for_dt,
    provider,
    model,
    status,
    prompt_total_db,
    started_at,
    finished_at,
  )) = existing
  {
    // Best-effort: keep prompt_total up to date for current prompt set, but do not reset existing runs.
    if prompt_total_db != prompt_total && prompt_total > 0 {
      sqlx::query(
        r#"
          UPDATE geo_monitor_runs
          SET prompt_total = ?, updated_at = CURRENT_TIMESTAMP(3)
          WHERE id = ?;
        "#,
      )
      .bind(prompt_total)
      .bind(id)
      .execute(pool)
      .await
      .map_err(|e| -> Error { Box::new(e) })?;
    }

    return Ok(GeoMonitorRunRow {
      id,
      tenant_id,
      project_id,
      run_for_dt,
      provider,
      model,
      status,
      prompt_total: prompt_total_db,
      started_at,
      finished_at,
    });
  }

  let res = sqlx::query(
    r#"
      INSERT INTO geo_monitor_runs
        (tenant_id, project_id, run_for_dt, provider, model, status, prompt_total)
      VALUES
        (?, ?, ?, ?, ?, 'running', ?);
    "#,
  )
  .bind(tenant_id)
  .bind(project_id)
  .bind(run_for_dt)
  .bind(provider)
  .bind(model)
  .bind(prompt_total)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let id = res.last_insert_id() as i64;
  let row: (
    i64,
    String,
    i64,
    chrono::NaiveDate,
    String,
    String,
    String,
    i32,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
  ) = sqlx::query_as(
    r#"
      SELECT id, tenant_id, project_id, run_for_dt, provider, model, status, prompt_total, started_at, finished_at
      FROM geo_monitor_runs
      WHERE id = ?
      LIMIT 1;
    "#,
  )
  .bind(id)
  .fetch_one(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(GeoMonitorRunRow {
    id: row.0,
    tenant_id: row.1,
    project_id: row.2,
    run_for_dt: row.3,
    provider: row.4,
    model: row.5,
    status: row.6,
    prompt_total: row.7,
    started_at: row.8,
    finished_at: row.9,
  })
}

pub async fn enqueue_geo_monitor_prompt_tasks(
  pool: &MySqlPool,
  tenant_id: &str,
  project_id: i64,
  run_for_dt: chrono::NaiveDate,
  prompt_ids: &[i64],
) -> Result<u64, Error> {
  let mut inserted: u64 = 0;
  for prompt_id in prompt_ids.iter().copied() {
    let dedupe_key = format!("{tenant_id}:geo_monitor_prompt:{project_id}:{run_for_dt}:{prompt_id}");
    let channel_id = format!("{project_id}:{prompt_id}");

    let res = sqlx::query(
      r#"
        INSERT INTO job_tasks (tenant_id, job_type, channel_id, run_for_dt, dedupe_key, status)
        VALUES (?, 'geo_monitor_prompt', ?, ?, ?, 'pending')
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
      "#,
    )
    .bind(tenant_id)
    .bind(channel_id)
    .bind(run_for_dt)
    .bind(dedupe_key)
    .execute(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

    inserted = inserted.saturating_add(res.rows_affected());
  }

  Ok(inserted)
}

pub async fn fetch_latest_geo_monitor_run(
  pool: &MySqlPool,
  tenant_id: &str,
  project_id: i64,
) -> Result<Option<GeoMonitorRunRow>, Error> {
  let row: Option<(
    i64,
    String,
    i64,
    chrono::NaiveDate,
    String,
    String,
    String,
    i32,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
  )> = sqlx::query_as(
    r#"
      SELECT id, tenant_id, project_id, run_for_dt, provider, model, status, prompt_total, started_at, finished_at
      FROM geo_monitor_runs
      WHERE tenant_id = ? AND project_id = ?
      ORDER BY run_for_dt DESC, id DESC
      LIMIT 1;
    "#,
  )
  .bind(tenant_id)
  .bind(project_id)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(row.map(|row| GeoMonitorRunRow {
    id: row.0,
    tenant_id: row.1,
    project_id: row.2,
    run_for_dt: row.3,
    provider: row.4,
    model: row.5,
    status: row.6,
    prompt_total: row.7,
    started_at: row.8,
    finished_at: row.9,
  }))
}

pub async fn insert_geo_monitor_run_result(
  pool: &MySqlPool,
  tenant_id: &str,
  project_id: i64,
  run_for_dt: chrono::NaiveDate,
  run_id: i64,
  prompt_id: i64,
  prompt_text: &str,
  output_text: Option<&str>,
  presence: bool,
  rank_int: Option<i32>,
  cost_usd: f64,
  error: Option<&str>,
) -> Result<bool, Error> {
  let res = sqlx::query(
    r#"
      INSERT IGNORE INTO geo_monitor_run_results
        (tenant_id, project_id, run_for_dt, run_id, prompt_id, prompt_text, output_text, presence, rank_int, cost_usd, error)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    "#,
  )
  .bind(tenant_id)
  .bind(project_id)
  .bind(run_for_dt)
  .bind(run_id)
  .bind(prompt_id)
  .bind(prompt_text)
  .bind(output_text)
  .bind(if presence { 1 } else { 0 })
  .bind(rank_int)
  .bind(cost_usd)
  .bind(error)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(res.rows_affected() > 0)
}

pub async fn finalize_geo_monitor_run_if_complete(
  pool: &MySqlPool,
  run_id: i64,
) -> Result<bool, Error> {
  let run: Option<(i32, Option<DateTime<Utc>>)> = sqlx::query_as(
    r#"
      SELECT prompt_total, finished_at
      FROM geo_monitor_runs
      WHERE id = ?
      LIMIT 1;
    "#,
  )
  .bind(run_id)
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let Some((prompt_total, finished_at)) = run else {
    return Ok(false);
  };
  if finished_at.is_some() || prompt_total <= 0 {
    return Ok(false);
  }

  let results_total: i64 = sqlx::query_scalar(
    r#"
      SELECT COUNT(*) FROM geo_monitor_run_results WHERE run_id = ?;
    "#,
  )
  .bind(run_id)
  .fetch_one(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  if results_total < prompt_total as i64 {
    return Ok(false);
  }

  let updated = sqlx::query(
    r#"
      UPDATE geo_monitor_runs
      SET status='completed', finished_at=COALESCE(finished_at, CURRENT_TIMESTAMP(3))
      WHERE id = ? AND finished_at IS NULL;
    "#,
  )
  .bind(run_id)
  .execute(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(updated.rows_affected() > 0)
}

pub async fn fetch_geo_monitor_run_summary(
  pool: &MySqlPool,
  run_id: i64,
) -> Result<GeoMonitorRunSummary, Error> {
  let row: (i64, i64, i64, i64, i64, f64) = sqlx::query_as(
    r#"
      SELECT
        COUNT(*) AS results_total,
        COALESCE(SUM(CASE WHEN presence = 1 THEN 1 ELSE 0 END), 0) AS presence_count,
        COALESCE(SUM(CASE WHEN rank_int IS NOT NULL AND rank_int <= 3 THEN 1 ELSE 0 END), 0) AS top3_count,
        COALESCE(SUM(CASE WHEN rank_int IS NOT NULL AND rank_int <= 5 THEN 1 ELSE 0 END), 0) AS top5_count,
        COALESCE(SUM(CASE WHEN error IS NOT NULL AND error <> '' THEN 1 ELSE 0 END), 0) AS error_count,
        COALESCE(CAST(SUM(cost_usd) AS DOUBLE), 0) AS cost_usd
      FROM geo_monitor_run_results
      WHERE run_id = ?;
    "#,
  )
  .bind(run_id)
  .fetch_one(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  Ok(GeoMonitorRunSummary {
    results_total: row.0,
    presence_count: row.1,
    top3_count: row.2,
    top5_count: row.3,
    error_count: row.4,
    cost_usd: row.5,
  })
}

pub async fn fetch_geo_monitor_run_results(
  pool: &MySqlPool,
  run_id: i64,
  limit: i64,
) -> Result<Vec<(i64, i64, String, Option<String>, bool, Option<i32>, f64, Option<String>)>, Error> {
  let limit = limit.clamp(1, 200);
  let rows: Vec<(i64, i64, String, Option<String>, i8, Option<i32>, f64, Option<String>)> =
    sqlx::query_as(
      r#"
        SELECT prompt_id, id, prompt_text, output_text, presence, rank_int, CAST(cost_usd AS DOUBLE) AS cost_usd, error
        FROM geo_monitor_run_results
        WHERE run_id = ?
        ORDER BY prompt_id ASC
        LIMIT ?;
      "#,
    )
    .bind(run_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| -> Error { Box::new(e) })?;

  Ok(
    rows
      .into_iter()
      .map(|(prompt_id, id, prompt_text, output_text, presence, rank_int, cost_usd, error)| {
        (
          prompt_id,
          id,
          prompt_text,
          output_text,
          presence != 0,
          rank_int,
          cost_usd,
          error,
        )
      })
      .collect(),
  )
}

pub fn sanitize_sql_identifier(header: &str) -> String {
  let mut out = String::with_capacity(header.len());
  let mut prev_underscore = false;

  for ch in header.chars() {
    let c = ch.to_ascii_lowercase();
    if c.is_ascii_alphanumeric() {
      out.push(c);
      prev_underscore = false;
    } else if !prev_underscore {
      out.push('_');
      prev_underscore = true;
    }
  }

  let trimmed = out.trim_matches('_');
  let mut normalized = if trimmed.is_empty() {
    "c".to_string()
  } else {
    trimmed.to_string()
  };

  if normalized
    .chars()
    .next()
    .map(|c| c.is_ascii_digit())
    .unwrap_or(false)
  {
    normalized = format!("c_{normalized}");
  }

  if normalized.len() > 64 {
    normalized.truncate(64);
  }

  normalized
}

pub fn dedupe_columns(headers: &[String]) -> Vec<String> {
  let mut seen: HashMap<String, usize> = HashMap::new();
  let mut out: Vec<String> = Vec::with_capacity(headers.len());

  for header in headers {
    let base = sanitize_sql_identifier(header);
    let count = seen.entry(base.clone()).or_insert(0);
    *count += 1;
    if *count == 1 {
      out.push(base);
    } else {
      out.push(format!("{base}_{}", *count));
    }
  }

  out
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

  #[test]
  fn sanitize_sql_identifier_normalizes_headers() {
    assert_eq!(
      sanitize_sql_identifier("Total Revenue ($)"),
      "total_revenue"
    );
    assert_eq!(sanitize_sql_identifier("123 Views"), "c_123_views");
    assert_eq!(sanitize_sql_identifier("视频"), "c");
  }

  #[test]
  fn dedupe_columns_appends_suffixes_for_conflicts() {
    let headers = vec!["Views".to_string(), "views".to_string(), "Views ".to_string()];
    let deduped = dedupe_columns(&headers);
    assert_eq!(deduped, vec!["views", "views_2", "views_3"]);
  }
}

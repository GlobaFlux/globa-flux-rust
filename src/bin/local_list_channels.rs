use vercel_runtime::Error;

use chrono::{DateTime, Utc};
use hyper_rustls::HttpsConnectorBuilder;

use globa_flux_rust::db::{
  fetch_or_seed_youtube_oauth_app_config, get_pool, update_youtube_connection_tokens,
};
use globa_flux_rust::providers::youtube::{refresh_tokens, youtube_oauth_client_from_config};

fn parse_flag_value(args: &[String], flag: &str) -> Option<String> {
  args
    .iter()
    .position(|a| a == flag)
    .and_then(|idx| args.get(idx + 1))
    .cloned()
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  let args: Vec<String> = std::env::args().collect();

  let tenant_id = parse_flag_value(&args, "--tenant-id")
    .or_else(|| parse_flag_value(&args, "--tenant"))
    .unwrap_or_default();
  if tenant_id.trim().is_empty() {
    eprintln!("Missing required --tenant-id");
    return Ok(());
  }

  let pool = get_pool().await?;

  let row = sqlx::query_as::<_, (String, Option<String>, Option<DateTime<Utc>>, Option<String>)>(
    r#"
      SELECT access_token, refresh_token, expires_at, channel_id
      FROM channel_connections
      WHERE tenant_id = ?
        AND oauth_provider = 'youtube'
      LIMIT 1;
    "#,
  )
  .bind(tenant_id.trim())
  .fetch_optional(pool)
  .await
  .map_err(|e| -> Error { Box::new(e) })?;

  let Some((mut access_token, refresh_token, expires_at, active_channel_id)) = row else {
    eprintln!("No YouTube connection found for this tenant.");
    return Ok(());
  };

  // Best-effort refresh if expired.
  let now = Utc::now();
  let needs_refresh = expires_at.map(|t| t <= now).unwrap_or(false);
  if needs_refresh {
    if let Some(refresh) = refresh_token.clone() {
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
      let refreshed = refresh_tokens(&client, refresh.trim()).await?;
      if let Some(ch) = active_channel_id.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
        update_youtube_connection_tokens(pool, tenant_id.trim(), ch, &refreshed).await?;
      }
      access_token = refreshed.access_token;
    }
  }

  let connector = HttpsConnectorBuilder::new()
    .with_native_roots()
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?
    .https_or_http()
    .enable_http1()
    .build();
  let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
    .build(connector);

  let mut hub = google_youtube3::YouTube::new(client, access_token.to_string());
  hub.base_url("https://youtube.googleapis.com/".to_string());
  hub.root_url("https://youtube.googleapis.com/".to_string());

  let (_, response) = hub
    .channels()
    .list(&vec!["id".into(), "snippet".into()])
    .mine(true)
    .doit()
    .await
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  let items = response.items.unwrap_or_default();
  println!("ok=true channels_count={} active_channel_id={}", items.len(), active_channel_id.unwrap_or_else(|| "null".to_string()));
  for ch in items {
    let id = ch.id.unwrap_or_else(|| "unknown".to_string());
    let title = ch
      .snippet
      .as_ref()
      .and_then(|s| s.title.clone())
      .unwrap_or_else(|| "unknown".to_string());
    println!("- {id}  {title}");
  }

  Ok(())
}

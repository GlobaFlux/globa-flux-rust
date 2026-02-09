use vercel_runtime::Error;

use chrono::{DateTime, Utc};

use globa_flux_rust::db::{fetch_youtube_channel_id, get_pool};

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
    eprintln!(
      "Example: cargo run --bin local_youtube_alerts_smoke -- --tenant-id gid://shopify/Customer/123"
    );
    return Ok(());
  }

  let channel_id_arg =
    parse_flag_value(&args, "--channel-id").or_else(|| parse_flag_value(&args, "--channel"));

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

  let result = sqlx::query_as::<_, (i64, DateTime<Utc>)>(
    r#"
      SELECT id, detected_at
      FROM yt_alerts
      WHERE tenant_id = ?
        AND channel_id = ?
      ORDER BY detected_at DESC
      LIMIT 1;
    "#,
  )
  .bind(tenant_id.trim())
  .bind(channel_id.trim())
  .fetch_optional(pool)
  .await;

  match result {
    Ok(None) => {
      println!("ok=true rows=0");
    }
    Ok(Some((id, detected_at))) => {
      println!("ok=true rows=1 id={id} detected_at={detected_at}");
    }
    Err(err) => {
      eprintln!("ok=false error={}", err);
    }
  }

  Ok(())
}

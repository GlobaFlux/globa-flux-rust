use std::sync::OnceLock;
use std::time::Duration;

static SHARED_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
static NO_PROXY_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn build_http_client(no_proxy: bool) -> Result<reqwest::Client, reqwest::Error> {
  let mut builder = reqwest::Client::builder()
    // Keep requests bounded in serverless and avoid hanging local dev sessions.
    .connect_timeout(Duration::from_secs(10))
    .timeout(Duration::from_secs(45))
    // Proxy behavior:
    // - By default, reqwest respects HTTP_PROXY / HTTPS_PROXY / NO_PROXY.
    // - We intentionally keep that default so local dev behind a proxy works.
    .user_agent("globa-flux-rust");

  if no_proxy {
    builder = builder.no_proxy();
  }

  builder.build()
}

pub fn http_client_for_url(url: &str) -> Result<&'static reqwest::Client, reqwest::Error> {
  let host = reqwest::Url::parse(url)
    .ok()
    .and_then(|u| u.host_str().map(|h| h.to_string()))
    .unwrap_or_default();

  let is_loopback = matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1");

  let lock = if is_loopback {
    &NO_PROXY_CLIENT
  } else {
    &SHARED_CLIENT
  };

  if let Some(client) = lock.get() {
    return Ok(client);
  }

  let client = build_http_client(is_loopback)?;
  let _ = lock.set(client);
  Ok(lock.get().expect("http client must be initialized"))
}

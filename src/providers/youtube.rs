use oauth2::basic::BasicClient;
use oauth2::{
  AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, EndpointNotSet, EndpointSet,
  RedirectUrl, Scope, TokenResponse, TokenUrl, RefreshToken,
};
use serde::Serialize;
use vercel_runtime::Error;

pub type YoutubeOAuthClient =
  BasicClient<EndpointSet, EndpointNotSet, EndpointNotSet, EndpointNotSet, EndpointSet>;

#[derive(Debug, Clone, Serialize)]
pub struct YoutubeOAuthTokens {
  pub access_token: String,
  pub refresh_token: Option<String>,
  pub token_type: String,
  pub scope: Option<String>,
  pub expires_in_seconds: Option<u64>,
}

pub fn youtube_oauth_client_from_config(
  client_id: &str,
  client_secret: &str,
  redirect_uri: &str,
) -> Result<(YoutubeOAuthClient, RedirectUrl), Error> {
  if client_id.trim().is_empty() {
    return Err(Box::new(std::io::Error::other("Missing YOUTUBE_CLIENT_ID")) as Error);
  }
  if client_secret.trim().is_empty() {
    return Err(Box::new(std::io::Error::other("Missing YOUTUBE_CLIENT_SECRET")) as Error);
  }
  if redirect_uri.trim().is_empty() {
    return Err(Box::new(std::io::Error::other("Missing YOUTUBE_REDIRECT_URI")) as Error);
  }

  let auth_url =
    AuthUrl::new("https://accounts.google.com/o/oauth2/v2/auth".to_string()).map_err(|e| {
      Box::new(std::io::Error::other(e.to_string())) as Error
    })?;
  let token_url =
    TokenUrl::new("https://oauth2.googleapis.com/token".to_string()).map_err(|e| {
      Box::new(std::io::Error::other(e.to_string())) as Error
    })?;

  let redirect_url = RedirectUrl::new(redirect_uri.to_string())
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  let client = BasicClient::new(ClientId::new(client_id.to_string()))
    .set_client_secret(ClientSecret::new(client_secret.to_string()))
    .set_auth_uri(auth_url)
    .set_token_uri(token_url)
    .set_redirect_uri(redirect_url.clone());

  Ok((client, redirect_url))
}

pub fn youtube_oauth_client_from_env() -> Result<(YoutubeOAuthClient, RedirectUrl), Error> {
  let client_id = std::env::var("YOUTUBE_CLIENT_ID")
    .map_err(|_| Box::new(std::io::Error::other("Missing YOUTUBE_CLIENT_ID")) as Error)?;
  let client_secret = std::env::var("YOUTUBE_CLIENT_SECRET")
    .map_err(|_| Box::new(std::io::Error::other("Missing YOUTUBE_CLIENT_SECRET")) as Error)?;
  let redirect_uri = std::env::var("YOUTUBE_REDIRECT_URI")
    .map_err(|_| Box::new(std::io::Error::other("Missing YOUTUBE_REDIRECT_URI")) as Error)?;
  youtube_oauth_client_from_config(&client_id, &client_secret, &redirect_uri)
}

pub fn build_authorize_url(client: &YoutubeOAuthClient, state: Option<String>) -> (String, String) {
  let (url, csrf) = client
    .authorize_url(|| {
      state
        .clone()
        .map(CsrfToken::new)
        .unwrap_or_else(CsrfToken::new_random)
    })
    .add_scope(Scope::new(
      "https://www.googleapis.com/auth/youtube.readonly".to_string(),
    ))
    .add_scope(Scope::new(
      "https://www.googleapis.com/auth/youtube.force-ssl".to_string(),
    ))
    .add_scope(Scope::new(
      "https://www.googleapis.com/auth/youtube.upload".to_string(),
    ))
    .add_scope(Scope::new(
      "https://www.googleapis.com/auth/yt-analytics.readonly".to_string(),
    ))
    .add_scope(Scope::new(
      "https://www.googleapis.com/auth/yt-analytics-monetary.readonly".to_string(),
    ))
    .add_scope(Scope::new(
      "https://www.googleapis.com/auth/youtubepartner".to_string(),
    ))
    .add_extra_param("access_type", "offline")
    .add_extra_param("prompt", "consent")
    .url();

  (url.to_string(), csrf.secret().to_string())
}

pub async fn exchange_code_for_tokens(
  client: &YoutubeOAuthClient,
  code: &str,
) -> Result<YoutubeOAuthTokens, Error> {
  let http_client = oauth2::reqwest::ClientBuilder::new()
    .redirect(oauth2::reqwest::redirect::Policy::none())
    .build()
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  let token = client
    .exchange_code(AuthorizationCode::new(code.to_string()))
    .request_async(&http_client)
    .await
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  Ok(YoutubeOAuthTokens {
    access_token: token.access_token().secret().to_string(),
    refresh_token: token.refresh_token().map(|t| t.secret().to_string()),
    token_type: token.token_type().as_ref().to_string(),
    scope: token
      .scopes()
      .map(|scopes| scopes.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(" ")),
    expires_in_seconds: token.expires_in().map(|d| d.as_secs()),
  })
}

pub async fn refresh_tokens(
  client: &YoutubeOAuthClient,
  refresh_token: &str,
) -> Result<YoutubeOAuthTokens, Error> {
  let http_client = oauth2::reqwest::ClientBuilder::new()
    .redirect(oauth2::reqwest::redirect::Policy::none())
    .build()
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  let token = client
    .exchange_refresh_token(&RefreshToken::new(refresh_token.to_string()))
    .request_async(&http_client)
    .await
    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Error)?;

  Ok(YoutubeOAuthTokens {
    access_token: token.access_token().secret().to_string(),
    refresh_token: token.refresh_token().map(|t| t.secret().to_string()),
    token_type: token.token_type().as_ref().to_string(),
    scope: token
      .scopes()
      .map(|scopes| scopes.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(" ")),
    expires_in_seconds: token.expires_in().map(|d| d.as_secs()),
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn builds_google_authorize_url_with_expected_params() {
    let client = BasicClient::new(ClientId::new("id".to_string()))
      .set_client_secret(ClientSecret::new("secret".to_string()))
      .set_auth_uri(AuthUrl::new("https://accounts.google.com/o/oauth2/v2/auth".to_string()).unwrap())
      .set_token_uri(TokenUrl::new("https://oauth2.googleapis.com/token".to_string()).unwrap())
      .set_redirect_uri(RedirectUrl::new("https://example.com/cb".to_string()).unwrap());

    let (url, state) = build_authorize_url(&client, Some("state123".to_string()));
    assert!(url.contains("accounts.google.com/o/oauth2/v2/auth"));
    assert!(url.contains("scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fyoutube.readonly"));
    assert!(url.contains("youtube.force-ssl"));
    assert!(url.contains("youtube.upload"));
    assert!(url.contains("yt-analytics.readonly"));
    assert!(url.contains("yt-analytics-monetary.readonly"));
    assert!(url.contains("youtubepartner"));
    assert!(url.contains("access_type=offline"));
    assert!(url.contains("prompt=consent"));
    assert_eq!(state, "state123");
  }
}

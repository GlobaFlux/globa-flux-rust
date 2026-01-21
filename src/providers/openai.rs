use async_openai::config::OpenAIConfig;
use async_openai::types::chat::{
  ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
  ChatCompletionRequestSystemMessageContent, ChatCompletionRequestUserMessageArgs,
  ChatCompletionRequestUserMessageContent, ChatCompletionStreamOptions,
  CreateChatCompletionRequest, CreateChatCompletionRequestArgs,
};
use async_openai::Client;
use crate::cost::ModelPricingUsdPerMToken;
use vercel_runtime::Error;

pub fn pricing_for_model(model: &str) -> Option<ModelPricingUsdPerMToken> {
  // Allow overriding pricing without code changes (USD per 1M tokens).
  if let (Ok(prompt), Ok(completion)) = (
    std::env::var("OPENAI_PRICE_PROMPT_USD_PER_M_TOKEN"),
    std::env::var("OPENAI_PRICE_COMPLETION_USD_PER_M_TOKEN"),
  ) {
    if let (Ok(prompt), Ok(completion)) = (prompt.parse::<f64>(), completion.parse::<f64>()) {
      return Some(ModelPricingUsdPerMToken { prompt, completion });
    }
  }

  match model {
    // Reference values; keep override support above for quick adjustments.
    "gpt-4o-mini" => Some(ModelPricingUsdPerMToken {
      prompt: 0.15,
      completion: 0.60,
    }),
    "gpt-4o" => Some(ModelPricingUsdPerMToken {
      prompt: 5.0,
      completion: 15.0,
    }),
    _ => None,
  }
}

pub struct RiskCheckMessageArgs<'a> {
  pub action_type: &'a str,
  pub note: Option<&'a str>,
}

pub fn build_risk_check_messages(args: RiskCheckMessageArgs<'_>) -> Result<Vec<ChatCompletionRequestMessage>, Error> {
  let user_note = args.note.unwrap_or("").trim();
  let note_line = if user_note.is_empty() {
    String::new()
  } else {
    format!("\n\nUser note: {}", user_note)
  };

  let system = r#"You are a YouTube creator risk-check assistant.
Return STRICT JSON only (no markdown, no commentary) with:
{"risk_level":"low|medium|high","allowed":true|false,"reason":"..."}.
Keep reason concise and actionable."#;

  let user = format!(
    "Action type: {}\nTask: Evaluate risk for this action and decide allowed vs not.\n{}",
    args.action_type,
    note_line
  );

  let system_msg = ChatCompletionRequestSystemMessageArgs::default()
    .content(ChatCompletionRequestSystemMessageContent::Text(system.to_string()))
    .build()
    .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?;

  let user_msg = ChatCompletionRequestUserMessageArgs::default()
    .content(ChatCompletionRequestUserMessageContent::Text(user))
    .build()
    .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?;

  Ok(vec![
    ChatCompletionRequestMessage::System(system_msg),
    ChatCompletionRequestMessage::User(user_msg),
  ])
}

pub struct ChatCompletionsPayloadArgs<'a> {
  pub model: &'a str,
  pub messages: Vec<ChatCompletionRequestMessage>,
  pub max_tokens: u32,
  pub stream: bool,
}

pub fn build_chat_completions_request(
  args: ChatCompletionsPayloadArgs<'_>,
) -> Result<CreateChatCompletionRequest, Error> {
  let mut builder = CreateChatCompletionRequestArgs::default();
  builder
    .model(args.model)
    .messages(args.messages)
    .temperature(0.2)
    .max_completion_tokens(args.max_tokens);

  if args.stream {
    builder.stream(true);
    builder.stream_options(ChatCompletionStreamOptions {
      include_usage: Some(true),
      include_obfuscation: None,
    });
  } else {
    builder.stream(false);
  }

  builder
    .build()
    .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })
}

pub fn openai_client(api_key: &str) -> Client<OpenAIConfig> {
  let config = OpenAIConfig::new().with_api_key(api_key);
  Client::with_config(config)
}

pub fn openai_client_with_idempotency(api_key: &str, idempotency_key: &str) -> Result<Client<OpenAIConfig>, Error> {
  let config = OpenAIConfig::new()
    .with_api_key(api_key)
    .with_header("Idempotency-Key", idempotency_key)
    .map_err(|e| -> Error { Box::new(std::io::Error::other(e.to_string())) })?;
  Ok(Client::with_config(config))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn pricing_for_gpt_4o_mini_is_available() {
    let pricing = pricing_for_model("gpt-4o-mini").expect("expected pricing");
    assert!(pricing.prompt > 0.0);
    assert!(pricing.completion > 0.0);
  }

  #[test]
  fn streaming_request_serializes_include_usage() {
    let messages = build_risk_check_messages(RiskCheckMessageArgs {
      action_type: "change_title",
      note: Some("test"),
    })
    .unwrap();
    let req = build_chat_completions_request(ChatCompletionsPayloadArgs {
      model: "gpt-4o-mini",
      messages,
      max_tokens: 12,
      stream: true,
    })
    .unwrap();

    let json = serde_json::to_value(req).unwrap();
    assert_eq!(
      json["stream_options"]["include_usage"].as_bool(),
      Some(true)
    );
  }
}

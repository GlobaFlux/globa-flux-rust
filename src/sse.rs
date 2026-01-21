pub fn sse_event(event: &str, data_json: &str) -> String {
  format!("event: {}\ndata: {}\n\n", event, data_json)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn sse_event_formats_event_and_data() {
    let out = sse_event("token", r#"{"text":"hi"}"#);
    assert_eq!(out, "event: token\ndata: {\"text\":\"hi\"}\n\n");
  }
}


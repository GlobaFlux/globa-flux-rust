use serde_json::Value;

pub fn parse_string_list_json(raw: Option<&str>) -> Vec<String> {
  let input = raw.unwrap_or("").trim();
  if input.is_empty() {
    return Vec::new();
  }

  let parsed: Value = match serde_json::from_str(input) {
    Ok(v) => v,
    Err(_) => return Vec::new(),
  };

  let arr = match parsed.as_array() {
    Some(v) => v,
    None => return Vec::new(),
  };

  arr.iter()
    .filter_map(|v| v.as_str())
    .map(|s| s.trim())
    .filter(|s| !s.is_empty())
    .map(|s| s.to_string())
    .collect()
}

pub fn normalize_aliases(primary: &str, aliases: &[String]) -> Vec<String> {
  let mut out: Vec<String> = Vec::new();

  let primary_norm = primary.trim();
  if !primary_norm.is_empty() {
    out.push(primary_norm.to_string());
  }

  for a in aliases.iter() {
    let a = a.trim();
    if a.is_empty() {
      continue;
    }
    if out.iter().any(|existing| existing.eq_ignore_ascii_case(a)) {
      continue;
    }
    out.push(a.to_string());
  }

  out
}

pub fn contains_any_case_insensitive(haystack: &str, needles: &[String]) -> bool {
  if haystack.is_empty() || needles.is_empty() {
    return false;
  }

  let hay = haystack.to_lowercase();
  needles.iter().any(|n| {
    let needle = n.trim();
    if needle.is_empty() {
      return false;
    }
    hay.contains(&needle.to_lowercase())
  })
}

fn is_numbered_list_item(line: &str) -> bool {
  let mut seen_digit = false;
  let mut chars = line.chars().peekable();

  while let Some(ch) = chars.peek().copied() {
    if ch.is_ascii_digit() {
      seen_digit = true;
      chars.next();
      continue;
    }
    break;
  }

  if !seen_digit {
    return false;
  }

  let sep = chars.next().unwrap_or('\0');
  matches!(sep, '.' | ')' | ':')
}

pub fn extract_rank_from_markdown_list(haystack: &str, needles: &[String]) -> Option<i32> {
  if haystack.is_empty() || needles.is_empty() {
    return None;
  }

  let mut rank: i32 = 0;

  for raw_line in haystack.lines() {
    let line = raw_line.trim();
    if line.is_empty() {
      continue;
    }

    let is_list_item =
      line.starts_with("- ") || line.starts_with("* ") || line.starts_with("• ") || is_numbered_list_item(line);
    if !is_list_item {
      continue;
    }

    rank += 1;
    if contains_any_case_insensitive(line, needles) {
      return Some(rank);
    }
  }

  None
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn normalize_aliases_dedupes_case_insensitive() {
    let out = normalize_aliases(
      "GlobaFlux",
      &vec!["globaflux".to_string(), "  ".to_string(), "Flux".to_string()],
    );
    assert_eq!(out, vec!["GlobaFlux".to_string(), "Flux".to_string()]);
  }

  #[test]
  fn extract_rank_from_markdown_list_detects_numbered_items() {
    let text = r#"
1. Alpha
2. Beta
3. GlobaFlux
"#;
    let needles = vec!["GlobaFlux".to_string()];
    assert_eq!(extract_rank_from_markdown_list(text, &needles), Some(3));
  }

  #[test]
  fn extract_rank_from_markdown_list_ignores_non_list_lines() {
    let text = "This mentions GlobaFlux but is not a list.";
    let needles = vec!["GlobaFlux".to_string()];
    assert_eq!(extract_rank_from_markdown_list(text, &needles), None);
  }

  #[test]
  fn parse_string_list_json_returns_empty_on_invalid_json() {
    assert!(parse_string_list_json(Some("not json")).is_empty());
  }
}


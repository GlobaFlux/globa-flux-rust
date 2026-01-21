#[derive(Debug, Clone)]
pub struct OutcomeComputed {
  pub revenue_change_pct_7d: Option<f64>,
  pub catastrophic_flag: bool,
  pub new_top_asset_flag: bool,
}

pub fn compute_outcome_label(
  pre_revenue_sum_usd_7d: f64,
  post_revenue_sum_usd_7d: f64,
  pre_top_video_ids: &[String],
  post_top_video_ids: &[String],
) -> OutcomeComputed {
  let revenue_change_pct_7d = if pre_revenue_sum_usd_7d > 0.0 {
    Some((post_revenue_sum_usd_7d - pre_revenue_sum_usd_7d) / pre_revenue_sum_usd_7d)
  } else {
    None
  };

  let catastrophic_flag = revenue_change_pct_7d
    .map(|pct| pct < -0.30)
    .unwrap_or(false);

  let pre_set: std::collections::HashSet<&str> =
    pre_top_video_ids.iter().map(|id| id.as_str()).collect();
  let new_top_asset_flag = post_top_video_ids
    .iter()
    .any(|id| !pre_set.contains(id.as_str()));

  OutcomeComputed {
    revenue_change_pct_7d,
    catastrophic_flag,
    new_top_asset_flag,
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn flags_catastrophic_when_revenue_drop_large() {
    let pre = 100.0;
    let post = 50.0;
    let computed = compute_outcome_label(pre, post, &[], &[]);
    assert!(computed.revenue_change_pct_7d.is_some());
    assert!(computed.catastrophic_flag);
  }

  #[test]
  fn marks_new_top_asset_when_post_top_changes() {
    let pre_top = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let post_top = vec!["a".to_string(), "d".to_string(), "c".to_string()];
    let computed = compute_outcome_label(10.0, 11.0, &pre_top, &post_top);
    assert!(computed.new_top_asset_flag);
  }

  #[test]
  fn avoids_divide_by_zero() {
    let computed = compute_outcome_label(0.0, 10.0, &[], &[]);
    assert!(computed.revenue_change_pct_7d.is_none());
    assert!(!computed.catastrophic_flag);
  }
}

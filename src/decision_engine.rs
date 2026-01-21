use chrono::NaiveDate;

use crate::providers::youtube_analytics::VideoDailyMetricRow;

#[derive(Debug, Clone)]
pub struct DecisionEngineConfig {
  pub min_days_with_data: usize,
  pub high_concentration_threshold: f64,
  pub trend_down_threshold_usd: f64,
  pub top_n_for_new_asset: usize,
}

impl Default for DecisionEngineConfig {
  fn default() -> Self {
    Self {
      min_days_with_data: 5,
      high_concentration_threshold: 0.6,
      trend_down_threshold_usd: -0.01,
      top_n_for_new_asset: 3,
    }
  }
}

#[derive(Debug, Clone)]
pub struct DecisionDailyComputed {
  pub as_of_dt: NaiveDate,
  pub direction: String,
  pub confidence: f64,
  pub evidence: Vec<String>,
  pub forbidden: Vec<String>,
  pub reevaluate: Vec<String>,
}

fn format_usd(value: f64) -> String {
  format!("${:.2}", value)
}

fn clamp(value: f64, min: f64, max: f64) -> f64 {
  if value < min {
    min
  } else if value > max {
    max
  } else {
    value
  }
}

fn day_range(start_dt: NaiveDate, end_dt: NaiveDate) -> Vec<NaiveDate> {
  let mut out = Vec::new();
  let mut cur = start_dt;
  while cur <= end_dt {
    out.push(cur);
    cur = cur.succ_opt().unwrap_or(cur);
    if out.len() > 40 {
      break;
    }
  }
  out
}

pub fn compute_decision(
  rows: &[VideoDailyMetricRow],
  as_of_dt: NaiveDate,
  start_dt: NaiveDate,
  end_dt: NaiveDate,
  cfg: DecisionEngineConfig,
) -> DecisionDailyComputed {
  let days = day_range(start_dt, end_dt);

  let mut days_with_data = std::collections::HashSet::<NaiveDate>::new();
  let mut revenue_by_day = std::collections::HashMap::<NaiveDate, f64>::new();
  let mut revenue_by_video = std::collections::HashMap::<String, f64>::new();
  let mut revenue_by_day_video = std::collections::HashMap::<(NaiveDate, String), f64>::new();

  for r in rows {
    if r.dt < start_dt || r.dt > end_dt {
      continue;
    }
    days_with_data.insert(r.dt);
    *revenue_by_day.entry(r.dt).or_insert(0.0) += r.estimated_revenue_usd;
    *revenue_by_video.entry(r.video_id.clone()).or_insert(0.0) += r.estimated_revenue_usd;
    *revenue_by_day_video
      .entry((r.dt, r.video_id.clone()))
      .or_insert(0.0) += r.estimated_revenue_usd;
  }

  let total_revenue_7d: f64 = days.iter().map(|d| *revenue_by_day.get(d).unwrap_or(&0.0)).sum();

  let (top_video_id, top_revenue_7d) = revenue_by_video
    .iter()
    .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
    .map(|(id, rev)| (id.clone(), *rev))
    .unwrap_or_else(|| ("".to_string(), 0.0));

  let data_insufficient = days_with_data.len() < cfg.min_days_with_data || total_revenue_7d <= 0.0 || top_video_id.is_empty();

  if data_insufficient {
    return DecisionDailyComputed {
      as_of_dt,
      direction: "PROTECT".to_string(),
      confidence: 0.6,
      evidence: vec![
        "Data insufficient for reliable signals (sync incomplete or zero revenue)".to_string(),
      ],
      forbidden: vec!["High-risk strategy changes without evidence".to_string()],
      reevaluate: vec!["After OAuth connect + first metrics sync".to_string()],
    };
  }

  let concentration = if total_revenue_7d > 0.0 {
    top_revenue_7d / total_revenue_7d
  } else {
    0.0
  };

  let first_day = *days.first().unwrap_or(&start_dt);
  let last_day = *days.last().unwrap_or(&end_dt);
  let top_first = *revenue_by_day_video
    .get(&(first_day, top_video_id.clone()))
    .unwrap_or(&0.0);
  let top_last = *revenue_by_day_video
    .get(&(last_day, top_video_id.clone()))
    .unwrap_or(&0.0);
  let top_trend_usd = top_last - top_first;

  let mut day_totals: Vec<f64> = days.iter().map(|d| *revenue_by_day.get(d).unwrap_or(&0.0)).collect();
  if day_totals.is_empty() {
    day_totals = vec![0.0; 7];
  }
  let mean = day_totals.iter().sum::<f64>() / (day_totals.len() as f64);
  let var = if mean > 0.0 {
    day_totals
      .iter()
      .map(|v| {
        let diff = v - mean;
        diff * diff
      })
      .sum::<f64>()
      / (day_totals.len() as f64)
  } else {
    0.0
  };
  let stddev = var.sqrt();
  let volatility_ratio = if mean > 0.0 { stddev / mean } else { 0.0 };

  let top_n = cfg.top_n_for_new_asset.max(1);
  let mut first_day_videos: Vec<(String, f64)> = vec![];
  let mut last_day_videos: Vec<(String, f64)> = vec![];

  for ((dt, vid), rev) in revenue_by_day_video.iter() {
    if *dt == first_day {
      first_day_videos.push((vid.clone(), *rev));
    } else if *dt == last_day {
      last_day_videos.push((vid.clone(), *rev));
    }
  }

  first_day_videos.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
  last_day_videos.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

  let first_set: std::collections::HashSet<String> = first_day_videos
    .iter()
    .take(top_n)
    .map(|(id, _)| id.clone())
    .collect();
  let new_asset_emergence = last_day_videos
    .iter()
    .take(top_n)
    .any(|(id, _)| !first_set.contains(id));

  let direction = if concentration >= cfg.high_concentration_threshold && top_trend_usd > 0.0 {
    "EXPLOIT"
  } else if top_trend_usd < cfg.trend_down_threshold_usd || new_asset_emergence {
    "EXPLORE"
  } else {
    "PROTECT"
  };

  let coverage_ratio = (days_with_data.len() as f64) / (days.len().max(1) as f64);
  let mut confidence = 0.55 + 0.25 * coverage_ratio;
  if direction == "EXPLOIT" && concentration >= 0.7 {
    confidence += 0.1;
  }
  if direction == "EXPLORE" && new_asset_emergence {
    confidence += 0.05;
  }
  if volatility_ratio > 0.6 {
    confidence -= 0.1;
  }
  confidence = clamp(confidence, 0.45, 0.9);

  let mut evidence = vec![
    format!("7d estimated revenue: {}", format_usd(total_revenue_7d)),
    format!("Top asset (7d) share: {:.0}%", concentration * 100.0),
    format!(
      "Top asset ({} → {}) change: {}",
      first_day,
      last_day,
      format_usd(top_trend_usd)
    ),
    format!("New asset emergence (Top-{top_n}): {}", if new_asset_emergence { "yes" } else { "no" }),
  ];
  if volatility_ratio > 0.0 {
    evidence.push(format!("Revenue volatility (std/mean): {:.2}", volatility_ratio));
  }

  let (forbidden, reevaluate) = match direction {
    "EXPLOIT" => (
      vec![
        "Avoid changing multiple variables at once (topic + format + cadence)".to_string(),
        "Avoid major pivots while the top asset is accelerating".to_string(),
      ],
      vec![
        "If top asset share drops materially, reconsider EXPLOIT".to_string(),
        "If 2–3 uploads fail to sustain, revisit direction".to_string(),
      ],
    ),
    "EXPLORE" => (
      vec![
        "Do not bet the whole channel on one unproven experiment".to_string(),
        "Limit experiments to 3–5 samples before judging".to_string(),
      ],
      vec![
        "If a new video enters Top-3 again, continue exploration".to_string(),
        "If top asset declines sharply, switch to PROTECT".to_string(),
      ],
    ),
    _ => (
      vec![
        "Avoid high-risk strategy changes without evidence".to_string(),
        "Prefer small optimizations (titles/thumbnails) over big pivots".to_string(),
      ],
      vec![
        "Re-evaluate after the next successful sync window".to_string(),
        "If revenue stabilizes and concentration rises, consider EXPLOIT".to_string(),
      ],
    ),
  };

  DecisionDailyComputed {
    as_of_dt,
    direction: direction.to_string(),
    confidence,
    evidence,
    forbidden,
    reevaluate,
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn row(dt: NaiveDate, video_id: &str, revenue: f64) -> VideoDailyMetricRow {
    VideoDailyMetricRow {
      dt,
      video_id: video_id.to_string(),
      estimated_revenue_usd: revenue,
      impressions: 0,
      views: 0,
    }
  }

  #[test]
  fn chooses_exploit_when_high_concentration_and_top_trending_up() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();

    let mut rows = Vec::new();
    // vidA dominates and increases.
    for (i, day) in day_range(start, end).iter().enumerate() {
      rows.push(row(*day, "vidA", 10.0 + i as f64));
      rows.push(row(*day, "vidB", 2.0));
    }

    let decision = compute_decision(rows.as_slice(), end.succ_opt().unwrap(), start, end, DecisionEngineConfig::default());
    assert_eq!(decision.direction, "EXPLOIT");
    assert!(decision.confidence >= 0.6);
  }

  #[test]
  fn chooses_explore_when_top_trending_down() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();

    let mut rows = Vec::new();
    for (i, day) in day_range(start, end).iter().enumerate() {
      rows.push(row(*day, "vidA", 20.0 - i as f64));
      rows.push(row(*day, "vidB", 8.0));
    }

    let decision = compute_decision(rows.as_slice(), end.succ_opt().unwrap(), start, end, DecisionEngineConfig::default());
    assert_eq!(decision.direction, "EXPLORE");
  }

  #[test]
  fn chooses_protect_when_insufficient_data() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();

    let rows = vec![row(start, "vidA", 1.0)];
    let decision = compute_decision(rows.as_slice(), end.succ_opt().unwrap(), start, end, DecisionEngineConfig::default());
    assert_eq!(decision.direction, "PROTECT");
  }
}


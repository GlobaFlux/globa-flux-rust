use chrono::NaiveDate;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ReplayDecision {
  pub as_of_dt: NaiveDate,
  pub direction: String,
}

#[derive(Debug, Clone)]
pub struct ReplayOutcome {
  pub decision_dt: NaiveDate,
  pub catastrophic_flag: bool,
}

#[derive(Debug, Clone)]
pub struct ReplayGateMetrics {
  pub days: usize,
  pub protect_days: usize,
  pub protect_rate: f64,
  pub switch_count: usize,
  pub switch_rate: f64,
  pub outcome_days: usize,
  pub catastrophic_days: usize,
  pub catastrophic_rate: f64,
}

pub fn compute_metrics(
  decisions: &[ReplayDecision],
  outcomes_by_decision_dt: &HashMap<NaiveDate, ReplayOutcome>,
) -> ReplayGateMetrics {
  let days = decisions.len();
  if days == 0 {
    return ReplayGateMetrics {
      days: 0,
      protect_days: 0,
      protect_rate: 0.0,
      switch_count: 0,
      switch_rate: 0.0,
      outcome_days: 0,
      catastrophic_days: 0,
      catastrophic_rate: 0.0,
    };
  }

  let protect_days = decisions
    .iter()
    .filter(|d| d.direction == "PROTECT")
    .count();

  let mut switch_count = 0usize;
  for i in 1..decisions.len() {
    if decisions[i].direction != decisions[i - 1].direction {
      switch_count += 1;
    }
  }

  let mut outcome_days = 0usize;
  let mut catastrophic_days = 0usize;
  for decision in decisions.iter() {
    if let Some(outcome) = outcomes_by_decision_dt.get(&decision.as_of_dt) {
      outcome_days += 1;
      if outcome.catastrophic_flag && decision.direction != "PROTECT" {
        catastrophic_days += 1;
      }
    }
  }

  let protect_rate = (protect_days as f64) / (days as f64);
  let switch_rate = if days >= 2 {
    (switch_count as f64) / ((days - 1) as f64)
  } else {
    0.0
  };
  let catastrophic_rate = if outcome_days > 0 {
    (catastrophic_days as f64) / (outcome_days as f64)
  } else {
    0.0
  };

  ReplayGateMetrics {
    days,
    protect_days,
    protect_rate,
    switch_count,
    switch_rate,
    outcome_days,
    catastrophic_days,
    catastrophic_rate,
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn d(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
  }

  #[test]
  fn metrics_are_zeroed_when_no_decisions() {
    let metrics = compute_metrics(&[], &HashMap::new());
    assert_eq!(metrics.days, 0);
    assert_eq!(metrics.switch_count, 0);
    assert_eq!(metrics.outcome_days, 0);
  }

  #[test]
  fn switch_rate_is_zero_when_constant_direction() {
    let decisions = vec![
      ReplayDecision {
        as_of_dt: d(2026, 1, 1),
        direction: "PROTECT".to_string(),
      },
      ReplayDecision {
        as_of_dt: d(2026, 1, 2),
        direction: "PROTECT".to_string(),
      },
      ReplayDecision {
        as_of_dt: d(2026, 1, 3),
        direction: "PROTECT".to_string(),
      },
    ];

    let metrics = compute_metrics(&decisions, &HashMap::new());
    assert_eq!(metrics.days, 3);
    assert_eq!(metrics.switch_count, 0);
    assert_eq!(metrics.switch_rate, 0.0);
    assert_eq!(metrics.protect_days, 3);
    assert_eq!(metrics.protect_rate, 1.0);
  }

  #[test]
  fn switch_rate_counts_changes_between_days() {
    let decisions = vec![
      ReplayDecision {
        as_of_dt: d(2026, 1, 1),
        direction: "PROTECT".to_string(),
      },
      ReplayDecision {
        as_of_dt: d(2026, 1, 2),
        direction: "EXPLOIT".to_string(),
      },
      ReplayDecision {
        as_of_dt: d(2026, 1, 3),
        direction: "EXPLOIT".to_string(),
      },
      ReplayDecision {
        as_of_dt: d(2026, 1, 4),
        direction: "EXPLORE".to_string(),
      },
      ReplayDecision {
        as_of_dt: d(2026, 1, 5),
        direction: "PROTECT".to_string(),
      },
    ];

    let metrics = compute_metrics(&decisions, &HashMap::new());
    assert_eq!(metrics.days, 5);
    assert_eq!(metrics.switch_count, 3);
    assert_eq!(metrics.switch_rate, 0.75);
    assert_eq!(metrics.protect_days, 2);
    assert!((metrics.protect_rate - 0.4).abs() < 1e-9);
  }

  #[test]
  fn catastrophic_rate_counts_only_when_direction_not_protect() {
    let decisions = vec![
      ReplayDecision {
        as_of_dt: d(2026, 1, 1),
        direction: "EXPLOIT".to_string(),
      },
      ReplayDecision {
        as_of_dt: d(2026, 1, 2),
        direction: "PROTECT".to_string(),
      },
    ];

    let mut outcomes = HashMap::new();
    outcomes.insert(
      d(2026, 1, 1),
      ReplayOutcome {
        decision_dt: d(2026, 1, 1),
        catastrophic_flag: true,
      },
    );
    outcomes.insert(
      d(2026, 1, 2),
      ReplayOutcome {
        decision_dt: d(2026, 1, 2),
        catastrophic_flag: true,
      },
    );

    let metrics = compute_metrics(&decisions, &outcomes);
    assert_eq!(metrics.days, 2);
    assert_eq!(metrics.outcome_days, 2);
    assert_eq!(metrics.catastrophic_days, 1);
    assert_eq!(metrics.catastrophic_rate, 0.5);
  }
}

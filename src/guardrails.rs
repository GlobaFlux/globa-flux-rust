use chrono::NaiveDate;

#[derive(Debug, Clone, Copy)]
pub struct WindowAgg {
  pub revenue_usd: f64,
  pub views: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct GuardrailInput {
  pub today: NaiveDate,
  pub current: WindowAgg,
  pub baseline: WindowAgg,
  pub max_metric_dt: Option<NaiveDate>,
  pub top1_concentration_7d: Option<f64>,
  pub total_revenue_usd_7d: Option<f64>,
  pub revenue_mean_usd_7d: Option<f64>,
  pub revenue_stddev_usd_7d: Option<f64>,
}

impl GuardrailInput {
  pub fn minimal(today: NaiveDate, max_metric_dt: NaiveDate) -> Self {
    GuardrailInput {
      today,
      current: WindowAgg {
        revenue_usd: 0.0,
        views: 0,
      },
      baseline: WindowAgg {
        revenue_usd: 0.0,
        views: 0,
      },
      max_metric_dt: Some(max_metric_dt),
      top1_concentration_7d: None,
      total_revenue_usd_7d: None,
      revenue_mean_usd_7d: None,
      revenue_stddev_usd_7d: None,
    }
  }
}

#[derive(Debug, Clone)]
pub struct GuardrailAlert {
  pub key: &'static str,
  pub kind: &'static str,
  pub severity: &'static str,
  pub message: String,
}

fn severity_for_drop(drop_pct: f64) -> &'static str {
  if drop_pct >= 0.30 {
    "critical"
  } else if drop_pct >= 0.20 {
    "error"
  } else {
    "warning"
  }
}

fn rpm(revenue_usd: f64, views: i64) -> f64 {
  if views > 0 {
    (revenue_usd / (views as f64)) * 1000.0
  } else {
    0.0
  }
}

pub fn evaluate_guardrails(input: &GuardrailInput) -> Vec<GuardrailAlert> {
  let mut out = Vec::new();

  let cur_views = input.current.views;
  let base_views = input.baseline.views;

  let cur_rpm = rpm(input.current.revenue_usd, cur_views);
  let base_rpm = rpm(input.baseline.revenue_usd, base_views);

  let can_compare = cur_views >= 1000 && base_views >= 1000 && base_rpm > 0.0;
  if can_compare {
    let drop_pct = ((base_rpm - cur_rpm) / base_rpm).max(-1.0);
    if drop_pct >= 0.10 {
      let severity = severity_for_drop(drop_pct);
      let msg = format!(
        "Revenue per mille dropped {:.0}% vs previous 7d (current ${:.2}, prev ${:.2}).",
        drop_pct * 100.0,
        cur_rpm,
        base_rpm
      );
      out.push(GuardrailAlert {
        key: "rpm_drop_7d",
        kind: "RPM drop",
        severity,
        message: msg,
      });
    }
  }

  match input.max_metric_dt {
    None => out.push(GuardrailAlert {
      key: "metrics_stale",
      kind: "Data missing",
      severity: "info",
      message: "No metrics found yet. Upload CSV or wait for the first sync.".to_string(),
    }),
    Some(dt) => {
      let age_days = (input.today - dt).num_days();
      if age_days >= 3 {
        out.push(GuardrailAlert {
          key: "metrics_stale",
          kind: "Data stale",
          severity: "warning",
          message: format!("Metrics look stale (latest day {}). Upload CSV or run a sync.", dt),
        });
      }
    }
  }

  if let (Some(concentration), Some(total_rev)) =
    (input.top1_concentration_7d, input.total_revenue_usd_7d)
  {
    if total_rev >= 20.0 && concentration >= 0.50 {
      out.push(GuardrailAlert {
        key: "rev_concentration_top1_7d",
        kind: "Revenue concentration",
        severity: "warning",
        message: format!(
          "Revenue is concentrated: top video is {:.0}% of total revenue (7d total ${:.2}).",
          concentration * 100.0,
          total_rev
        ),
      });
    }
  }

  if let (Some(mean), Some(stddev)) = (input.revenue_mean_usd_7d, input.revenue_stddev_usd_7d) {
    if mean >= 10.0 && stddev >= 0.4 * mean {
      out.push(GuardrailAlert {
        key: "rev_volatility_7d",
        kind: "Revenue volatility",
        severity: "warning",
        message: format!(
          "Revenue is volatile (7d stddev ${:.2} vs mean ${:.2}).",
          stddev, mean
        ),
      });
    }
  }

  out
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn rpm_drop_triggers_warning() {
    let input = GuardrailInput {
      current: WindowAgg {
        revenue_usd: 90.0,
        views: 10_000,
      },
      baseline: WindowAgg {
        revenue_usd: 120.0,
        views: 10_000,
      },
      max_metric_dt: Some(NaiveDate::from_ymd_opt(2026, 2, 4).unwrap()),
      today: NaiveDate::from_ymd_opt(2026, 2, 5).unwrap(),
      top1_concentration_7d: None,
      total_revenue_usd_7d: None,
      revenue_mean_usd_7d: None,
      revenue_stddev_usd_7d: None,
    };

    let alerts = evaluate_guardrails(&input);
    assert!(alerts.iter().any(|a| a.key == "rpm_drop_7d"));
  }

  #[test]
  fn stale_metrics_triggers_alert() {
    let input = GuardrailInput {
      current: WindowAgg {
        revenue_usd: 0.0,
        views: 0,
      },
      baseline: WindowAgg {
        revenue_usd: 0.0,
        views: 0,
      },
      max_metric_dt: Some(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()),
      today: NaiveDate::from_ymd_opt(2026, 2, 5).unwrap(),
      top1_concentration_7d: None,
      total_revenue_usd_7d: None,
      revenue_mean_usd_7d: None,
      revenue_stddev_usd_7d: None,
    };

    let alerts = evaluate_guardrails(&input);
    assert!(alerts.iter().any(|a| a.key == "metrics_stale"));
  }

  #[test]
  fn concentration_triggers_when_top1_over_50pct() {
    let mut input = GuardrailInput::minimal(
      NaiveDate::from_ymd_opt(2026, 2, 5).unwrap(),
      NaiveDate::from_ymd_opt(2026, 2, 4).unwrap(),
    );
    input.top1_concentration_7d = Some(0.55);
    input.total_revenue_usd_7d = Some(100.0);
    let alerts = evaluate_guardrails(&input);
    assert!(alerts
      .iter()
      .any(|a| a.key == "rev_concentration_top1_7d"));
  }

  #[test]
  fn volatility_triggers_when_stddev_high() {
    let mut input = GuardrailInput::minimal(
      NaiveDate::from_ymd_opt(2026, 2, 5).unwrap(),
      NaiveDate::from_ymd_opt(2026, 2, 4).unwrap(),
    );
    input.revenue_stddev_usd_7d = Some(30.0);
    input.revenue_mean_usd_7d = Some(50.0);
    let alerts = evaluate_guardrails(&input);
    assert!(alerts.iter().any(|a| a.key == "rev_volatility_7d"));
  }
}

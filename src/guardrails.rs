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
    };

    let alerts = evaluate_guardrails(&input);
    assert!(alerts.iter().any(|a| a.key == "metrics_stale"));
  }
}

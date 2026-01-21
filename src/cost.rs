#[derive(Clone, Copy, Debug)]
pub struct ModelPricingUsdPerMToken {
  pub prompt: f64,
  pub completion: f64,
}

pub fn compute_cost_usd(
  pricing: ModelPricingUsdPerMToken,
  prompt_tokens: u32,
  completion_tokens: u32,
) -> f64 {
  let prompt_cost = (prompt_tokens as f64 / 1_000_000.0) * pricing.prompt;
  let completion_cost = (completion_tokens as f64 / 1_000_000.0) * pricing.completion;
  prompt_cost + completion_cost
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn compute_cost_usd_applies_per_million_rates() {
    let pricing = ModelPricingUsdPerMToken {
      prompt: 10.0,
      completion: 20.0,
    };
    let cost = compute_cost_usd(pricing, 100_000, 50_000);
    assert!((cost - 2.0).abs() < 1e-9);
  }
}


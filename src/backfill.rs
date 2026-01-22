use chrono::{Duration, NaiveDate};

pub fn compute_backfill_run_for_dates(
  end_dt: NaiveDate,
  backfill_days: i64,
  chunk_days: i64,
) -> Vec<NaiveDate> {
  let backfill_days = backfill_days.clamp(1, 365);
  let chunk_days = chunk_days.clamp(1, 30);

  let chunks = ((backfill_days + chunk_days - 1) / chunk_days).max(1);
  let first_window_end = end_dt - Duration::days((chunks - 1) * chunk_days);

  let mut out = Vec::with_capacity(chunks as usize);
  for i in 0..chunks {
    let window_end = first_window_end + Duration::days(i * chunk_days);
    let run_for_dt = window_end + Duration::days(1);
    out.push(run_for_dt);
  }

  out
}

#[cfg(test)]
mod tests {
  use super::*;

  fn d(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
  }

  #[test]
  fn weekly_chunks_cover_14_days_as_two_tasks() {
    let end_dt = d(2026, 1, 8);
    let run_for = compute_backfill_run_for_dates(end_dt, 14, 7);
    assert_eq!(run_for.len(), 2);
    assert_eq!(run_for[0], d(2026, 1, 2));
    assert_eq!(run_for[1], d(2026, 1, 9));
  }

  #[test]
  fn weekly_chunks_round_up_for_non_multiple_day_counts() {
    let end_dt = d(2026, 1, 8);
    let run_for = compute_backfill_run_for_dates(end_dt, 10, 7);
    assert_eq!(run_for.len(), 2);
    assert_eq!(run_for[1], d(2026, 1, 9));
  }

  #[test]
  fn run_for_dates_are_sorted_and_unique() {
    let end_dt = d(2026, 1, 8);
    let run_for = compute_backfill_run_for_dates(end_dt, 84, 7);
    let mut sorted = run_for.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted, run_for);
  }
}


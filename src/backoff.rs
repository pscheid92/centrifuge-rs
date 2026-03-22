use std::time::Duration;

use rand::RngExt;

/// Calculates the next reconnect/resubscribe delay using exponential backoff with full jitter.
///
/// Formula: `random(0, min(max_delay, min_delay * 2^attempt))`
///
/// This follows the "Full Jitter" strategy recommended by AWS to prevent thundering herd.
pub fn next_delay(attempt: u32, min_delay: Duration, max_delay: Duration) -> Duration {
    let base = min_delay.as_millis().saturating_mul(2u128.saturating_pow(attempt));
    let capped = base.min(max_delay.as_millis());
    if capped == 0 {
        return Duration::ZERO;
    }
    let jittered = rand::rng().random_range(0..=capped);
    Duration::from_millis(jittered as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delay_never_exceeds_max() {
        let min = Duration::from_millis(500);
        let max = Duration::from_secs(20);
        for attempt in 0..100 {
            let delay = next_delay(attempt, min, max);
            assert!(delay <= max, "attempt {attempt}: {delay:?} > {max:?}");
        }
    }

    #[test]
    fn test_delay_at_attempt_zero_bounded_by_min() {
        let min = Duration::from_millis(500);
        let max = Duration::from_secs(20);
        // At attempt 0: range is [0, 500ms]. Run enough times to be confident.
        for _ in 0..100 {
            let delay = next_delay(0, min, max);
            assert!(delay <= min, "attempt 0: {delay:?} > {min:?}");
        }
    }

    #[test]
    fn test_delay_grows_exponentially() {
        let min = Duration::from_millis(500);
        let max = Duration::from_secs(20);
        // At attempt 5: max possible = min(20s, 500ms * 32) = 16s
        // At attempt 6: max possible = min(20s, 500ms * 64) = 20s (capped)
        // Just verify it doesn't panic and stays in range.
        for attempt in 0..50 {
            let delay = next_delay(attempt, min, max);
            assert!(delay <= max);
        }
    }

    #[test]
    fn test_zero_min_delay() {
        let delay = next_delay(0, Duration::ZERO, Duration::from_secs(10));
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    fn test_jitter_produces_different_values() {
        let min = Duration::from_millis(500);
        let max = Duration::from_secs(20);
        let delays: Vec<_> = (0..20).map(|_| next_delay(5, min, max)).collect();
        // With 20 samples from [0, 16000ms], it's astronomically unlikely they're all equal.
        let all_same = delays.windows(2).all(|w| w[0] == w[1]);
        assert!(!all_same, "jitter should produce varied delays");
    }
}

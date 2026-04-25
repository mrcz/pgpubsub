use std::cell::RefCell;
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::RngExt;

pub struct ExponentialBackoff {
    start_duration: f32,
    max_duration: f32,
    backoff: f32,
    dbl_jitter: f32,
    current_duration: f32,
    hard_max: bool,
    has_slept_since_last_fail: bool,
}

thread_local! {
    static RNG: RefCell<SmallRng> = RefCell::new(rand::make_rng());
}

impl ExponentialBackoff {
    #[allow(unused)]
    pub fn new(start_duration: Duration, max_duration: Duration) -> Self {
        Self::with_backoff(start_duration, max_duration, 2.0)
    }

    pub fn with_backoff(start_duration: Duration, max_duration: Duration, backoff: f32) -> Self {
        Self::with_params(start_duration, max_duration, backoff, 0.03, false)
    }

    pub fn with_params(
        start_duration: Duration,
        max_duration: Duration,
        backoff: f32,
        jitter: f32,
        hard_max: bool,
    ) -> Self {
        let dbl_jitter = jitter + jitter;
        let start_duration = start_duration.as_secs_f32();
        let max_duration = max_duration.as_secs_f32();
        assert!(start_duration.is_sign_positive()); // Converted from Duration, so always >= +0.0.
        assert!(
            backoff.is_normal() && backoff >= 1.0,
            "backoff must be 1.0 or greater and normal"
        );
        assert!(
            dbl_jitter.is_finite() && dbl_jitter >= 0.0,
            "jitter must be non-negative and finite"
        );
        assert!(
            max_duration.is_finite() && max_duration > 0.0,
            "max_duration must be non-negative and finite"
        );
        Self {
            start_duration,
            max_duration,
            backoff,
            dbl_jitter,
            current_duration: start_duration,
            has_slept_since_last_fail: false,
            hard_max,
        }
    }

    fn fail(&mut self) -> Duration {
        if self.has_slept_since_last_fail {
            return Duration::ZERO;
        }
        self.has_slept_since_last_fail = true;
        let time_to_sleep = {
            let tts = self.current_duration * self.rand_jitter();
            if self.hard_max {
                tts.min(self.max_duration)
            } else {
                tts
            }
        };
        self.current_duration = (time_to_sleep * self.backoff).min(self.max_duration);
        Duration::from_secs_f32(time_to_sleep)
    }

    fn rand_jitter(&mut self) -> f32 {
        // Jitter is pre-doubled, so instead of (2.0 * rand - 1.0) * jitter, we can just do
        // (rand - 0.5) * doubled_jitter, which saves a multiplication.
        let rand = RNG.with(|rng| rng.borrow_mut().random::<f32>());
        (rand - 0.5).mul_add(self.dbl_jitter, 1.0)
    }

    pub async fn fail_and_sleep(&mut self) {
        tokio::time::sleep(self.fail()).await;
    }

    pub fn reset(&mut self) {
        self.current_duration = self.start_duration;
        self.has_slept_since_last_fail = false;
    }
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        ExponentialBackoff::with_params(
            Duration::from_millis(100),
            Duration::from_secs(5),
            1.8,
            0.025,
            false,
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn with_params_zero_start_duration() {
        ExponentialBackoff::with_params(Duration::ZERO, Duration::from_secs(5), 1.87, 0.03, false);
    }

    #[test]
    #[should_panic]
    fn with_params_too_low_backoff_panics() {
        ExponentialBackoff::with_params(
            Duration::from_millis(150),
            Duration::from_secs(5),
            0.87,
            0.03,
            false,
        );
    }

    #[test]
    #[should_panic]
    fn with_params_nan_backoff() {
        ExponentialBackoff::with_params(
            Duration::from_millis(150),
            Duration::from_secs(5),
            f32::NAN,
            0.03,
            false,
        );
    }

    #[test]
    #[should_panic]
    fn with_params_infinite_backoff_panics() {
        ExponentialBackoff::with_params(
            Duration::from_millis(150),
            Duration::from_secs(5),
            f32::INFINITY,
            0.03,
            false,
        );
    }

    #[test]
    #[should_panic]
    fn with_params_negative_jitter_panics() {
        ExponentialBackoff::with_params(
            Duration::from_millis(150),
            Duration::from_secs(5),
            0.87,
            -0.03,
            false,
        );
    }

    #[test]
    #[should_panic]
    fn with_params_nan_jitter_panics() {
        ExponentialBackoff::with_params(
            Duration::from_millis(150),
            Duration::from_secs(5),
            0.87,
            f32::NAN,
            false,
        );
    }

    #[test]
    #[should_panic]
    fn with_params_infinite_jitter_panics() {
        ExponentialBackoff::with_params(
            Duration::from_millis(150),
            Duration::from_secs(5),
            0.87,
            f32::INFINITY,
            false,
        );
    }

    #[test]
    fn doesnt_panic_with_valid_parameters() {
        ExponentialBackoff::with_params(
            Duration::from_millis(150),
            Duration::from_secs(5),
            1.87,
            0.025,
            true,
        );
    }

    /// Helper: build a backoff with no jitter for deterministic assertions.
    fn deterministic(start_ms: u64, max_ms: u64, factor: f32) -> ExponentialBackoff {
        ExponentialBackoff::with_params(
            Duration::from_millis(start_ms),
            Duration::from_millis(max_ms),
            factor,
            0.0,
            false,
        )
    }

    /// Durations round-trip through `f32` with sub-microsecond drift, so equality
    /// comparisons use this tolerance.
    fn assert_close(actual: Duration, expected_ms: u64) {
        let drift = if actual >= Duration::from_millis(expected_ms) {
            actual - Duration::from_millis(expected_ms)
        } else {
            Duration::from_millis(expected_ms) - actual
        };
        assert!(
            drift < Duration::from_micros(10),
            "expected ~{expected_ms}ms, got {actual:?}"
        );
    }

    #[test]
    fn first_fail_returns_start_duration_and_advances_current() {
        let mut b = deterministic(100, 5_000, 2.0);
        let slept = b.fail();
        assert_close(slept, 100);
        // current_duration is the next sleep target; after one failure it has been
        // multiplied by the backoff factor (100 * 2.0 = 200ms = 0.2s).
        assert!((b.current_duration - 0.2).abs() < 1e-6);
    }

    #[test]
    fn fail_returns_zero_until_reset() {
        // The intent of `has_slept_since_last_fail` is that a single failure round
        // produces a single sleep — repeated fail() calls without an intervening
        // reset() return ZERO (no further sleeping).
        let mut b = deterministic(100, 5_000, 2.0);
        assert_close(b.fail(), 100);
        assert_eq!(b.fail(), Duration::ZERO);
        assert_eq!(b.fail(), Duration::ZERO);

        b.reset();
        assert_close(b.fail(), 100);
    }

    #[test]
    fn reset_returns_current_to_start() {
        let mut b = deterministic(100, 5_000, 2.0);
        let _ = b.fail();
        // current_duration was advanced to 0.2s.
        b.reset();
        // After reset, current_duration is back to start and the next fail() returns
        // the start duration again — proving the progression isn't sticky.
        assert!((b.current_duration - 0.1).abs() < 1e-6);
        assert_close(b.fail(), 100);
    }

    #[test]
    fn current_duration_is_capped_at_max() {
        // With factor 10 and start 100ms, the next current_duration would be 1000ms,
        // but max is 500ms — so it should be clamped.
        let mut b = deterministic(100, 500, 10.0);
        let _ = b.fail();
        assert!((b.current_duration - 0.5).abs() < 1e-6);
    }
}

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
}

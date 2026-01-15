// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use fail::fail_point;
use kvproto::errorpb;

use super::time::{Duration, Instant};

#[derive(Debug, Copy, Clone)]
pub struct DeadlineError;

impl std::error::Error for DeadlineError {
    fn description(&self) -> &str {
        "deadline has elapsed"
    }
}

impl std::fmt::Display for DeadlineError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "deadline has elapsed")
    }
}

/// A handy structure for checking deadline.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Deadline {
    deadline: Instant,
}

impl Deadline {
    /// Creates a new `Deadline` by the given `deadline`.
    pub fn new(deadline: Instant) -> Self {
        Self { deadline }
    }

    /// Creates a new `Deadline` that will reach after specified amount of time
    /// in future.
    pub fn from_now(after_duration: Duration) -> Self {
        let deadline = Instant::now_coarse() + after_duration;
        Self { deadline }
    }

    pub fn inner(&self) -> Instant {
        self.deadline
    }

    /// Returns error if the deadline is exceeded.
    pub fn check(&self) -> std::result::Result<(), DeadlineError> {
        fail_point!("deadline_check_fail", |_| Err(DeadlineError));

        let now = Instant::now_coarse();
        if self.deadline <= now {
            return Err(DeadlineError);
        }
        Ok(())
    }

    // Returns the deadline instant of the std library.
    pub fn to_std_instant(&self) -> std::time::Instant {
        std::time::Instant::now() + self.deadline.duration_since(Instant::now_coarse())
    }

    /// Returns the remaining duration of the deadline.
    pub fn remaining_duration(&self) -> Duration {
        self.deadline
            .saturating_duration_since(Instant::now_coarse())
    }
}

const DEADLINE_EXCEEDED: &str = "deadline is exceeded";

pub fn set_deadline_exceeded_busy_error(e: &mut errorpb::Error) {
    let mut server_is_busy_err = errorpb::ServerIsBusy::default();
    server_is_busy_err.set_reason(DEADLINE_EXCEEDED.to_owned());
    e.set_server_is_busy(server_is_busy_err);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remaining_duration_with_future_deadline() {
        // Test case: deadline in the future should return positive remaining duration
        let deadline = Deadline::from_now(Duration::from_millis(100));
        let remaining = deadline.remaining_duration();

        // The remaining duration should be approximately 100ms (within some tolerance)
        // Due to coarse time precision, we allow a small tolerance
        assert!(
            remaining >= Duration::from_millis(95) && remaining <= Duration::from_millis(105),
            "remaining duration should be around 100ms, got {:?}",
            remaining
        );
    }

    #[test]
    fn test_remaining_duration_with_expired_deadline() {
        // Test case: deadline in the past should return zero duration
        let past_deadline = Instant::now_coarse() - Duration::from_millis(100);
        let deadline = Deadline::new(past_deadline);
        let remaining = deadline.remaining_duration();

        // Using saturating_duration_since, expired deadline should return zero
        assert!(
            remaining.is_zero(),
            "expired deadline should return zero duration, got {:?}",
            remaining
        );
    }

    #[test]
    fn test_remaining_duration_with_current_time() {
        // Test case: deadline at current time should return zero or very small duration
        let now = Instant::now_coarse();
        let deadline = Deadline::new(now);
        let remaining = deadline.remaining_duration();

        // Due to coarse time precision and the time between creating now and calling
        // remaining_duration, it might be zero or a very small positive value
        assert!(
            remaining <= Duration::from_millis(10),
            "deadline at current time should return zero or very small duration, got {:?}",
            remaining
        );
    }

    #[test]
    fn test_remaining_duration_with_large_duration() {
        // Test case: deadline far in the future
        let deadline = Deadline::from_now(Duration::from_secs(60));
        let remaining = deadline.remaining_duration();

        // The remaining duration should be approximately 60 seconds
        assert!(
            remaining >= Duration::from_secs(59) && remaining <= Duration::from_secs(61),
            "remaining duration should be around 60s, got {:?}",
            remaining
        );
    }

    #[test]
    fn test_remaining_duration_consistency() {
        // Test case: remaining_duration should be consistent with check() method
        let deadline = Deadline::from_now(Duration::from_millis(50));
        let remaining = deadline.remaining_duration();

        // If remaining is zero, check should fail
        // If remaining is positive, check should pass
        match deadline.check() {
            Ok(_) => {
                assert!(
                    !remaining.is_zero(),
                    "if check() passes, remaining_duration should not be zero, got {:?}",
                    remaining
                );
            }
            Err(_) => {
                assert!(
                    remaining.is_zero(),
                    "if check() fails, remaining_duration should be zero, got {:?}",
                    remaining
                );
            }
        }
    }
}

// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use fail::fail_point;

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
#[derive(Debug, Copy, Clone)]
pub struct Deadline {
    deadline: Instant,
}

impl Deadline {
    /// Creates a new `Deadline` by the given `deadline`.
    pub fn new(deadline: Instant) -> Self {
        Self { deadline }
    }

    /// Creates a new `Deadline` that will reach after specified amount of time in future.
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
}

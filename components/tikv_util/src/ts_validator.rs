// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Wrapper of an atomic value that represents the current safe point.
/// A safe point is a timestamp in MVCC context. A transactional operation is valid only when its
/// timestamp is after safe point.
pub struct SafePoint(Arc<AtomicU64>);

/// Used to check whether any timestamp is expired, which means earlier than the current safe point.
/// `TsValidator` can be created from `SafePoint`. The value of safe point can't be updated via
/// TsValidator.
#[derive(Clone)]
pub struct TsValidator(Arc<AtomicU64>);

impl SafePoint {
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    pub fn get_ts_validator(&self) -> TsValidator {
        TsValidator(Arc::clone(&self.0))
    }

    #[inline]
    pub fn store(&self, safe_point: u64) {
        self.0.store(safe_point, Ordering::Release);
    }

    #[inline]
    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

impl TsValidator {
    /// For a given TS, check if the ts is expired, which means, earlier than the current safe point.
    #[inline]
    pub fn is_ts_expired(&self, ts: u64) -> bool {
        let safe_point = self.0.load(Ordering::Acquire);
        ts <= safe_point
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ts_validator() {
        let safe_point = SafePoint::new();
        let validator = safe_point.get_ts_validator();
        assert!(validator.is_ts_expired(0));
        assert!(!validator.is_ts_expired(1));
        assert!(!validator.is_ts_expired(u64::max_value()));

        safe_point.store(100);
        assert!(validator.is_ts_expired(99));
        assert!(validator.is_ts_expired(100));
        assert!(!validator.is_ts_expired(101));
        assert!(!validator.is_ts_expired(u64::max_value()));
    }
}

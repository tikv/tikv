// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

/// This structure represents a specific summary statistical item. It records various
/// statistics (such as the number of keys scanned) within a particular scope.
#[derive(Debug, Default)]
pub struct SummaryRecord {
    /// Number of keys that have been read.
    pub r_count: AtomicU64,

    /// Number of keys that have been written.
    pub w_count: AtomicU64,
}

impl Clone for SummaryRecord {
    fn clone(&self) -> Self {
        Self {
            r_count: AtomicU64::new(self.r_count.load(Relaxed)),
            w_count: AtomicU64::new(self.w_count.load(Relaxed)),
        }
    }
}

impl SummaryRecord {
    /// Reset all data to zero.
    pub fn reset(&self) {
        self.r_count.store(0, Relaxed);
        self.w_count.store(0, Relaxed);
    }

    /// Add two items.
    pub fn merge(&self, other: &Self) {
        self.r_count.fetch_add(other.r_count.load(Relaxed), Relaxed);
        self.w_count.fetch_add(other.w_count.load(Relaxed), Relaxed);
    }

    /// Gets the value and writes it to zero.
    pub fn take_and_reset(&self) -> Self {
        Self {
            r_count: AtomicU64::new(self.r_count.swap(0, Relaxed)),
            w_count: AtomicU64::new(self.w_count.swap(0, Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn test_summary_record() {
        let record = SummaryRecord {
            r_count: AtomicU64::new(1),
            w_count: AtomicU64::new(2),
        };
        assert_eq!(record.r_count.load(Relaxed), 1);
        assert_eq!(record.w_count.load(Relaxed), 2);
        let record2 = record.clone();
        assert_eq!(record2.r_count.load(Relaxed), 1);
        assert_eq!(record2.w_count.load(Relaxed), 2);
        record.merge(&SummaryRecord {
            r_count: AtomicU64::new(3),
            w_count: AtomicU64::new(4),
        });
        assert_eq!(record.r_count.load(Relaxed), 4);
        assert_eq!(record.w_count.load(Relaxed), 6);
        let record2 = record.take_and_reset();
        assert_eq!(record.r_count.load(Relaxed), 0);
        assert_eq!(record.w_count.load(Relaxed), 0);
        assert_eq!(record2.r_count.load(Relaxed), 4);
        assert_eq!(record2.w_count.load(Relaxed), 6);
        record2.reset();
        assert_eq!(record2.r_count.load(Relaxed), 0);
        assert_eq!(record2.w_count.load(Relaxed), 0);
    }
}

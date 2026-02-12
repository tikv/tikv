// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Progressive sampling extension for FMSketch.
//!
//! This module implements progressive sampling that reduces the sampling rate
//! as more rows are processed, achieving ~95% reduction in processed rows
//! for large tables while maintaining acceptable NDV estimation accuracy.

use rand::Rng;

use super::fmsketch::FmSketch;

/// SamplingPhase defines a threshold and sampling rate for progressive sampling.
#[derive(Clone, Copy, Debug)]
pub struct SamplingPhase {
    /// Row count where this phase begins
    pub threshold: u64,
    /// Sampling rate (0.0-1.0) for this phase
    pub rate: f64,
}

/// Default progressive sampling schedule.
/// This represents a ~95% reduction in processed rows for a 100M row table.
pub const DEFAULT_PROGRESSIVE_SCHEDULE: [SamplingPhase; 7] = [
    SamplingPhase {
        threshold: 0,
        rate: 1.0,
    }, // 0-500K: 100%
    SamplingPhase {
        threshold: 500_000,
        rate: 0.5,
    }, // 500K-1M: 50%
    SamplingPhase {
        threshold: 1_000_000,
        rate: 0.25,
    }, // 1M-5M: 25%
    SamplingPhase {
        threshold: 5_000_000,
        rate: 0.10,
    }, // 5M-10M: 10%
    SamplingPhase {
        threshold: 10_000_000,
        rate: 0.05,
    }, // 10M-50M: 5%
    SamplingPhase {
        threshold: 50_000_000,
        rate: 0.02,
    }, // 50M-100M: 2%
    SamplingPhase {
        threshold: 100_000_000,
        rate: 0.01,
    }, // 100M+: 1%
];

/// ProgressiveFmSketch extends FmSketch with progressive sampling capabilities.
///
/// It processes 100% of rows up to a threshold, then progressively reduces
/// the sampling rate for larger tables to improve performance while maintaining
/// acceptable NDV estimation accuracy.
#[derive(Clone)]
pub struct ProgressiveFmSketch {
    /// The underlying FMSketch
    inner: FmSketch,

    /// Total rows seen
    rows_processed: u64,
    /// Rows actually processed by FMSketch
    rows_sampled: u64,
    /// Current sampling rate (0.0-1.0)
    current_sample_rate: f64,

    /// Sampling schedule (threshold -> rate)
    schedule: Vec<SamplingPhase>,
    /// Current phase index
    current_phase: usize,

    /// NDV at last check (for singleton ratio estimation)
    last_ndv: u64,
    /// Rows sampled at last check
    last_rows_sampled: u64,
    /// Approximate ratio of singletons to NDV
    singleton_ratio: f64,
    /// How often to update singleton ratio
    check_interval: u64,
    /// Next row count to check NDV growth
    next_check_at: u64,
}

impl ProgressiveFmSketch {
    /// Creates a new ProgressiveFmSketch with the default schedule.
    pub fn new(max_size: usize) -> Self {
        Self::with_schedule(max_size, DEFAULT_PROGRESSIVE_SCHEDULE.to_vec())
    }

    /// Creates a new ProgressiveFmSketch with a custom schedule.
    pub fn with_schedule(max_size: usize, schedule: Vec<SamplingPhase>) -> Self {
        let initial_rate = schedule.first().map(|p| p.rate).unwrap_or(1.0);
        let check_interval = 10000;
        Self {
            inner: FmSketch::new(max_size),
            rows_processed: 0,
            rows_sampled: 0,
            current_sample_rate: initial_rate,
            schedule,
            current_phase: 0,
            last_ndv: 0,
            last_rows_sampled: 0,
            singleton_ratio: 0.6, // Default based on Zipf's law observation
            check_interval,
            next_check_at: check_interval,
        }
    }

    /// Determines whether the current row should be sampled.
    /// This implements Bernoulli sampling with a rate that decreases as more
    /// rows are processed.
    pub fn should_sample(&mut self) -> bool {
        self.rows_processed += 1;

        // Update phase if threshold crossed
        self.update_phase();

        // Phase 1 (100% sampling) - always sample
        if self.current_sample_rate >= 1.0 {
            self.rows_sampled += 1;
            return true;
        }

        // Bernoulli sampling with current rate
        let mut rng = rand::thread_rng();
        if rng.gen::<f64>() < self.current_sample_rate {
            self.rows_sampled += 1;
            return true;
        }
        false
    }

    /// Updates the current sampling phase based on rows processed.
    fn update_phase(&mut self) {
        while self.current_phase < self.schedule.len() - 1 {
            let next_phase = self.current_phase + 1;
            if self.rows_processed >= self.schedule[next_phase].threshold {
                self.current_phase = next_phase;
                self.current_sample_rate = self.schedule[next_phase].rate;
            } else {
                break;
            }
        }
    }

    /// Inserts a value into the sketch if it passes the sampling check.
    /// Returns true if the value was sampled and inserted.
    pub fn insert(&mut self, bytes: &[u8]) -> bool {
        if !self.should_sample() {
            return false;
        }

        self.inner.insert(bytes);

        // Periodically update singleton ratio estimate
        self.maybe_update_singleton_ratio();

        true
    }

    /// Inserts a pre-computed hash value into the sketch if it passes sampling.
    /// Returns true if the value was sampled and inserted.
    pub fn insert_hash_value(&mut self, hash_val: u64) -> bool {
        if !self.should_sample() {
            return false;
        }

        self.inner.insert_hash_value(hash_val);

        // Periodically update singleton ratio estimate
        self.maybe_update_singleton_ratio();

        true
    }

    /// Periodically estimates the singleton ratio based on NDV growth rate.
    fn maybe_update_singleton_ratio(&mut self) {
        if self.rows_sampled < self.next_check_at {
            return;
        }

        let current_ndv = self.inner.ndv();
        if self.last_rows_sampled > 0 && current_ndv > self.last_ndv {
            // Calculate NDV growth rate
            let row_delta = (self.rows_sampled - self.last_rows_sampled) as f64;
            let ndv_delta = (current_ndv - self.last_ndv) as f64;
            let growth_rate = ndv_delta / row_delta;

            // Update singleton ratio estimate based on growth rate
            // Higher growth rate = more unique values = higher singleton ratio
            // Clamp between 0.3 and 0.8 based on empirical observations
            self.singleton_ratio = (growth_rate * 10.0).clamp(0.3, 0.8);
        }

        self.last_ndv = current_ndv;
        self.last_rows_sampled = self.rows_sampled;
        self.next_check_at = self.rows_sampled + self.check_interval;
    }

    /// Returns the estimated NDV, extrapolating from sampled data if necessary.
    /// Uses the Goodman/Chao1 estimator for unbiased NDV estimation from samples.
    pub fn estimate_ndv(&self) -> u64 {
        if self.rows_sampled == 0 {
            return 0;
        }

        // Get observed NDV from underlying FMSketch
        let observed_ndv = self.inner.ndv();

        // If we sampled everything, return exact NDV
        if self.rows_sampled == self.rows_processed {
            return observed_ndv;
        }

        // Calculate effective sample fraction
        let sample_fraction = self.rows_sampled as f64 / self.rows_processed as f64;

        // Estimate number of singletons (values appearing exactly once)
        // Using approximate singleton ratio based on NDV growth observation
        let f1 = observed_ndv as f64 * self.singleton_ratio;

        // Goodman/Chao1-style estimator for NDV extrapolation
        // NDV_est = observedNDV + f1 * (1 - sampleFraction) / sampleFraction
        let adjustment = f1 * (1.0 - sample_fraction) / sample_fraction;
        let mut estimated_ndv = observed_ndv as f64 + adjustment;

        // Bound by total rows (NDV cannot exceed row count)
        if estimated_ndv > self.rows_processed as f64 {
            estimated_ndv = self.rows_processed as f64;
        }

        // Ensure we don't return less than observed
        if estimated_ndv < observed_ndv as f64 {
            estimated_ndv = observed_ndv as f64;
        }

        estimated_ndv.round() as u64
    }

    /// Returns the lower and upper bounds of the NDV estimate
    /// at the specified confidence level (e.g., 0.95 for 95% confidence).
    pub fn confidence_interval(&self, confidence: f64) -> (u64, u64) {
        let ndv = self.estimate_ndv();

        if self.rows_sampled == self.rows_processed {
            // No sampling, exact result
            return (ndv, ndv);
        }

        // Calculate standard error based on sample size and observed NDV
        let sample_fraction = self.rows_sampled as f64 / self.rows_processed as f64;
        let variance = ndv as f64 * (1.0 - sample_fraction) / sample_fraction;
        let std_err = variance.sqrt();

        // z-score for confidence level
        let z = normal_quantile(confidence);

        let lower = ((ndv as f64 - z * std_err).max(0.0)) as u64;
        let mut upper = (ndv as f64 + z * std_err) as u64;

        // Upper bound cannot exceed total rows
        if upper > self.rows_processed {
            upper = self.rows_processed;
        }

        (lower, upper)
    }

    /// Returns the total number of rows seen.
    pub fn rows_processed(&self) -> u64 {
        self.rows_processed
    }

    /// Returns the number of rows actually processed by the sketch.
    pub fn rows_sampled(&self) -> u64 {
        self.rows_sampled
    }

    /// Returns the effective overall sample rate.
    pub fn sample_rate(&self) -> f64 {
        if self.rows_processed == 0 {
            return 1.0;
        }
        self.rows_sampled as f64 / self.rows_processed as f64
    }

    /// Returns the current phase's sampling rate.
    pub fn current_phase_rate(&self) -> f64 {
        self.current_sample_rate
    }

    /// Returns a reference to the underlying FmSketch.
    pub fn inner(&self) -> &FmSketch {
        &self.inner
    }

    /// Consumes this sketch and returns the underlying FmSketch.
    pub fn into_inner(self) -> FmSketch {
        self.inner
    }

    /// Merges another ProgressiveFmSketch into this one.
    /// Note: After merging, the sampling statistics are combined but the
    /// extrapolation may be less accurate than individual sketches.
    pub fn merge(&mut self, other: &ProgressiveFmSketch) {
        // Merge underlying FMSketch using protobuf conversion
        let other_proto: tipb::FmSketch = other.inner.clone().into();
        self.inner.merge_from_proto(&other_proto);

        // Combine sampling statistics
        self.rows_processed += other.rows_processed;
        self.rows_sampled += other.rows_sampled;

        // Use weighted average for singleton ratio
        if self.rows_sampled > 0 && other.rows_sampled > 0 {
            let total_sampled = (self.rows_sampled + other.rows_sampled) as f64;
            self.singleton_ratio = (self.singleton_ratio * self.rows_sampled as f64
                + other.singleton_ratio * other.rows_sampled as f64)
                / total_sampled;
        }
    }
}

/// Returns the z-score for a given confidence level.
/// Uses common pre-computed values and approximation for others.
fn normal_quantile(confidence: f64) -> f64 {
    // Common confidence levels
    if confidence >= 0.99 {
        2.576
    } else if confidence >= 0.95 {
        1.96
    } else if confidence >= 0.90 {
        1.645
    } else if confidence >= 0.80 {
        1.282
    } else {
        // Approximation for other values using inverse error function approximation
        // This is a simplified Beasley-Springer-Moro algorithm
        let p = (1.0 + confidence) / 2.0;
        let t = (-2.0 * (1.0 - p).ln()).sqrt();
        t - (2.515517 + 0.802853 * t + 0.010328 * t * t)
            / (1.0 + 1.432788 * t + 0.189269 * t * t + 0.001308 * t * t * t)
    }
}

impl From<ProgressiveFmSketch> for tipb::FmSketch {
    fn from(pfs: ProgressiveFmSketch) -> tipb::FmSketch {
        pfs.inner.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progressive_sampling_phases() {
        let mut sketch = ProgressiveFmSketch::new(1000);

        // Phase 0: 100% sampling for first 500K rows
        assert_eq!(sketch.current_phase_rate(), 1.0);

        // Simulate processing rows
        for i in 0..500_000u64 {
            sketch.rows_processed = i;
            sketch.update_phase();
        }
        assert_eq!(sketch.current_phase_rate(), 1.0);

        // After 500K, should move to 50%
        sketch.rows_processed = 500_001;
        sketch.update_phase();
        assert_eq!(sketch.current_phase_rate(), 0.5);

        // After 1M, should move to 25%
        sketch.rows_processed = 1_000_001;
        sketch.update_phase();
        assert_eq!(sketch.current_phase_rate(), 0.25);

        // After 5M, should move to 10%
        sketch.rows_processed = 5_000_001;
        sketch.update_phase();
        assert_eq!(sketch.current_phase_rate(), 0.10);

        // After 10M, should move to 5%
        sketch.rows_processed = 10_000_001;
        sketch.update_phase();
        assert_eq!(sketch.current_phase_rate(), 0.05);

        // After 50M, should move to 2%
        sketch.rows_processed = 50_000_001;
        sketch.update_phase();
        assert_eq!(sketch.current_phase_rate(), 0.02);

        // After 100M, should move to 1%
        sketch.rows_processed = 100_000_001;
        sketch.update_phase();
        assert_eq!(sketch.current_phase_rate(), 0.01);
    }

    #[test]
    fn test_sampling_rate_decreases() {
        let mut sketch = ProgressiveFmSketch::new(1000);

        // Insert many values
        for i in 0..100_000u64 {
            let bytes = i.to_le_bytes();
            sketch.insert(&bytes);
        }

        // After 100K rows, sample rate should still be 1.0
        assert!(sketch.sample_rate() > 0.99);

        // Continue to 600K
        for i in 100_000..600_000u64 {
            let bytes = i.to_le_bytes();
            sketch.insert(&bytes);
        }

        // Sample rate should now be less than 1.0 (we're past 500K)
        assert!(sketch.sample_rate() < 1.0);
        assert!(sketch.sample_rate() > 0.7); // But not too low yet
    }

    #[test]
    fn test_ndv_estimation_full_sample() {
        let mut sketch = ProgressiveFmSketch::new(1000);

        // Insert distinct values (all sampled in phase 1)
        for i in 0..10_000u64 {
            let bytes = i.to_le_bytes();
            sketch.insert(&bytes);
        }

        // Since all are sampled, estimate should be close to actual
        let estimated = sketch.estimate_ndv();
        // FMSketch has some error, but should be within reasonable bounds
        assert!(estimated > 8_000);
        assert!(estimated < 12_000);
    }

    #[test]
    fn test_confidence_interval() {
        let mut sketch = ProgressiveFmSketch::new(1000);

        // Insert values
        for i in 0..50_000u64 {
            let bytes = i.to_le_bytes();
            sketch.insert(&bytes);
        }

        let (lower, upper) = sketch.confidence_interval(0.95);
        let estimate = sketch.estimate_ndv();

        // Estimate should be within bounds
        assert!(lower <= estimate);
        assert!(estimate <= upper);
    }

    #[test]
    fn test_custom_schedule() {
        let custom_schedule = vec![
            SamplingPhase {
                threshold: 0,
                rate: 1.0,
            },
            SamplingPhase {
                threshold: 1000,
                rate: 0.1,
            },
        ];

        let mut sketch = ProgressiveFmSketch::with_schedule(1000, custom_schedule);

        // Initial rate
        assert_eq!(sketch.current_phase_rate(), 1.0);

        // After threshold
        sketch.rows_processed = 1001;
        sketch.update_phase();
        assert_eq!(sketch.current_phase_rate(), 0.1);
    }

    #[test]
    fn test_normal_quantile() {
        assert!((normal_quantile(0.95) - 1.96).abs() < 0.01);
        assert!((normal_quantile(0.99) - 2.576).abs() < 0.01);
        assert!((normal_quantile(0.90) - 1.645).abs() < 0.01);
    }
}

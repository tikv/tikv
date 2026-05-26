// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! A minimal HyperLogLog used to estimate per-region distinct/singleton counts
//! for ANALYZE NDV sub-sampling. Unlike the FM sketch it is a fixed-size
//! register array (`1 << precision` bytes), so retaining one per region on the
//! TiDB side stays bounded. It merges by register-wise max, which is exactly
//! what the per-region leave-one-out (global singleton estimate) needs.
//!
//! The register layout and estimator must stay byte-for-byte identical to the
//! Go implementation in TiDB (pkg/statistics): TiKV builds these sketches and
//! TiDB reads the raw register bytes, unions them, and estimates cardinality.

/// Default HyperLogLog precision. m = 1<<14 = 16384 one-byte registers (16 KiB),
/// ~0.8% standard error. The singleton estimate is a difference of close
/// cardinalities, so the precision is deliberately not lower.
pub const DEFAULT_HLL_PRECISION: u8 = 14;

#[derive(Clone)]
pub struct Hll {
    precision: u8,
    /// One byte per register; `registers.len() == 1 << precision`. Each register
    /// holds the maximum observed rank (leftmost-set-bit position) for hashes
    /// routed to it.
    registers: Vec<u8>,
}

impl Hll {
    pub fn new(precision: u8) -> Hll {
        Hll {
            precision,
            registers: vec![0u8; 1usize << precision],
        }
    }

    /// Routes `hash` to a register by its top `precision` bits and records the
    /// rank (1 + leading zeros) of the remaining bits.
    pub fn insert_hash_value(&mut self, hash: u64) {
        let p = u32::from(self.precision);
        let idx = (hash >> (64 - p)) as usize;
        // Move the remaining 64-p bits to the top; the low p bits become zero.
        let w = hash << p;
        // rank in [1, 64-p+1]; the cap is hit only when every remaining bit is 0.
        let rank = if w == 0 {
            (64 - p + 1) as u8
        } else {
            (w.leading_zeros() + 1) as u8
        };
        if rank > self.registers[idx] {
            self.registers[idx] = rank;
        }
    }

    /// Register-wise max. `other` must have the same precision.
    pub fn merge(&mut self, other: &Hll) {
        debug_assert_eq!(self.precision, other.precision);
        for (r, &o) in self.registers.iter_mut().zip(other.registers.iter()) {
            if o > *r {
                *r = o;
            }
        }
    }

    /// Estimated distinct count. Standard HyperLogLog with the small-range
    /// (linear counting) correction; the large-range correction is unnecessary
    /// for 64-bit hashes.
    pub fn count(&self) -> u64 {
        let m = self.registers.len() as f64;
        let mut sum = 0.0f64;
        let mut zeros = 0usize;
        for &r in &self.registers {
            sum += 2.0f64.powi(-(i32::from(r)));
            if r == 0 {
                zeros += 1;
            }
        }
        let estimate = alpha(self.registers.len()) * m * m / sum;
        if estimate <= 2.5 * m && zeros > 0 {
            // Linear counting is more accurate when many registers are still empty.
            return (m * (m / zeros as f64).ln()).round() as u64;
        }
        estimate.round() as u64
    }
}

/// The HyperLogLog bias-correction constant alpha_m.
fn alpha(m: usize) -> f64 {
    match m {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / m as f64),
    }
}

impl From<Hll> for tipb::HllSketch {
    fn from(h: Hll) -> tipb::HllSketch {
        let mut proto = tipb::HllSketch::default();
        proto.set_precision(u32::from(h.precision));
        proto.set_registers(h.registers); // move the register bytes, no clone
        proto
    }
}

impl From<&tipb::HllSketch> for Hll {
    fn from(proto: &tipb::HllSketch) -> Hll {
        Hll {
            precision: proto.get_precision() as u8,
            registers: proto.get_registers().to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // splitmix64 is a high-quality bijective 64-bit mixer; HLL needs a uniformly
    // distributed hash (a plain multiplier skews the leading-zero rank).
    fn splitmix64(x: u64) -> u64 {
        let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn build(precision: u8, n: u64) -> Hll {
        let mut h = Hll::new(precision);
        for i in 0..n {
            h.insert_hash_value(splitmix64(i));
        }
        h
    }

    #[test]
    fn test_count_within_error() {
        // p=14 has ~0.8% standard error; allow a generous 5% band.
        for &n in &[0u64, 1, 100, 10_000, 200_000] {
            let est = build(DEFAULT_HLL_PRECISION, n).count() as f64;
            let err = if n == 0 { est } else { (est - n as f64).abs() / n as f64 };
            assert!(
                (n == 0 && est == 0.0) || err < 0.05,
                "n={n} est={est} err={err}"
            );
        }
    }

    #[test]
    fn test_merge_is_union() {
        // Disjoint halves merged must estimate the union (~2n).
        let mut a = Hll::new(DEFAULT_HLL_PRECISION);
        let mut b = Hll::new(DEFAULT_HLL_PRECISION);
        for i in 0..100_000u64 {
            a.insert_hash_value(splitmix64(i));
            b.insert_hash_value(splitmix64(i + 100_000));
        }
        a.merge(&b);
        let est = a.count() as f64;
        assert!((est - 200_000.0).abs() / 200_000.0 < 0.05, "est={est}");
    }

    #[test]
    fn test_merge_idempotent_and_proto_roundtrip() {
        let h = build(DEFAULT_HLL_PRECISION, 5000);
        let before = h.count();
        // Merging a sketch with itself changes nothing (max of equal registers).
        let mut same = h.clone();
        same.merge(&h);
        assert_eq!(before, same.count());
        // Proto carries the raw registers and precision.
        let proto: tipb::HllSketch = h.into();
        assert_eq!(proto.get_precision(), u32::from(DEFAULT_HLL_PRECISION));
        assert_eq!(proto.get_registers().len(), 1usize << DEFAULT_HLL_PRECISION);
    }
}

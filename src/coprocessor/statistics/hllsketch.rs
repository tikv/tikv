// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use mur3::murmurhash3_x64_128;

const HLL_BUCKET_BITS: u32 = 4;
const HLL_BUCKET_COUNT: usize = 1 << HLL_BUCKET_BITS;
#[cfg(test)]
const HLL_ALPHA_M: f64 = 0.673;

/// HllSketch is a fixed-precision HyperLogLog sketch with 16 buckets.
#[derive(Clone)]
pub struct HllSketch {
    registers: [u8; HLL_BUCKET_COUNT],
}

impl HllSketch {
    pub fn new() -> HllSketch {
        HllSketch {
            registers: [0; HLL_BUCKET_COUNT],
        }
    }

    pub fn insert(&mut self, bytes: &[u8]) {
        let hash = murmurhash3_x64_128(bytes, 0).0;
        self.insert_hash_value(hash);
    }

    pub fn insert_hash_value(&mut self, hash: u64) {
        let bucket = (hash as usize) & (HLL_BUCKET_COUNT - 1);
        let w = hash >> HLL_BUCKET_BITS;
        let rank = if w == 0 {
            (64 - HLL_BUCKET_BITS + 1) as u8
        } else {
            // `w` is right-aligned after removing the bucket bits, so exclude the
            // bucket width from the full-width leading-zero count.
            (w.leading_zeros() + 1 - HLL_BUCKET_BITS) as u8
        };
        if rank > self.registers[bucket] {
            self.registers[bucket] = rank;
        }
    }

    pub fn merge(&mut self, other: &HllSketch) {
        for i in 0..HLL_BUCKET_COUNT {
            if other.registers[i] > self.registers[i] {
                self.registers[i] = other.registers[i];
            }
        }
    }

    #[cfg(test)]
    fn ndv(&self) -> u64 {
        self.ndv_estimate().round() as u64
    }

    #[cfg(test)]
    fn ndv_estimate(&self) -> f64 {
        let m = HLL_BUCKET_COUNT as f64;
        let mut harmonic_sum = 0.0_f64;
        let mut zeros = 0_u32;
        for register in &self.registers {
            harmonic_sum += 2_f64.powi(-(*register as i32));
            if *register == 0 {
                zeros += 1;
            }
        }
        let raw_estimate = HLL_ALPHA_M * m * m / harmonic_sum;
        if raw_estimate <= 2.5 * m && zeros > 0 {
            m * (m / zeros as f64).ln()
        } else {
            raw_estimate
        }
    }
}

impl From<HllSketch> for tipb::HllSketch {
    fn from(hll: HllSketch) -> tipb::HllSketch {
        let mut proto = tipb::HllSketch::default();
        proto.set_bucket_bits(HLL_BUCKET_BITS);
        let registers = hll.registers.into_iter().map(u32::from).collect();
        proto.set_registers(registers);
        proto
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hll_merge() {
        let mut left = HllSketch::new();
        let mut right = HllSketch::new();
        for i in 0..500_u64 {
            left.insert(&i.to_le_bytes());
        }
        for i in 500..1000_u64 {
            right.insert(&i.to_le_bytes());
        }
        left.merge(&right);
        let estimate = left.ndv();
        assert!(estimate > 350);
        assert!(estimate < 1800);
    }

    #[test]
    fn test_hll_rank_ignores_bucket_bits() {
        let mut sketch = HllSketch::new();

        // The first non-bucket bit is 1, so the rank should be 1 rather than 1 + p.
        let hash = (1_u64 << 63) | 0x7;
        sketch.insert_hash_value(hash);
        assert_eq!(sketch.registers[0x7], 1);

        // Six leading zeros in the non-bucket bits should produce rank 7.
        let mut sketch = HllSketch::new();
        let hash = (1_u64 << (63 - 6)) | 0x3;
        sketch.insert_hash_value(hash);
        assert_eq!(sketch.registers[0x3], 7);
    }
}

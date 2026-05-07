// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashSet;
use mur3::murmurhash3_x64_128;

/// FMSketch (Flajolet-Martin Sketch) is a probabilistic data structure that
/// estimates the count of unique elements in a stream. It employs a hash
/// function to convert each element into a binary number and then counts the
/// trailing zeroes in each hashed value. **This variant of the FM sketch uses a
/// set to store unique hashed values and a binary mask to track the maximum
/// number of trailing zeroes.** The estimated count of distinct values is
/// calculated as 2^r * count, where 'r' is the maximum number of trailing
/// zeroes observed and 'count' is the number of unique hashed values. The
/// fundamental idea is that our hash function maps the input domain onto a
/// logarithmic scale. This is achieved by hashing the input value and counting
/// the number of trailing zeroes in the binary representation of the hash
/// value. Each distinct value is mapped to 'i' with a probability of 2^-(i+1).
/// For example, a value is mapped to 0 with a probability of 1/2, to 1 with a
/// probability of 1/4, to 2 with a probability of 1/8, and so on. This is
/// achieved by hashing the input value and counting the trailing zeroes in the
/// hash value. If we have a set of 'n' distinct values, the count of distinct
/// values with 'r' trailing zeroes is n / 2^r. Therefore, the estimated count
/// of distinct values is 2^r * count = n. The level-by-level approach increases
/// the accuracy of the estimation by ensuring a minimum count of distinct
/// values at each level. This way, the final estimation is less likely to be
/// skewed by outliers. For more details, refer to the following papers:
///  1. https://www.vldb.org/conf/2001/P541.pdf
///  2. https://algo.inria.fr/flajolet/Publications/FlMa85.pdf
#[derive(Clone)]
pub struct FmSketch {
    /// A binary mask used to track the maximum number of trailing zeroes in the
    /// hashed values. Also used to track the level of the sketch.
    /// Every time the size of the hashset exceeds the maximum size, the mask
    /// will be moved to the next level.
    mask: u64,
    /// The maximum size of the hashset. If the size exceeds this value, the
    /// mask will be moved to the next level. And the hashset will only keep
    /// the hashed values with trailing zeroes greater than or equal to the
    /// new mask.
    max_size: usize,
    /// A set to store unique hashed values.
    hash_set: HashSet<u64>,
}

impl FmSketch {
    /// Creates a new FmSketch with the given maximum size.
    pub fn new(max_size: usize) -> FmSketch {
        FmSketch {
            mask: 0,
            max_size,
            hash_set: HashSet::with_capacity_and_hasher(max_size + 1, Default::default()),
        }
    }

    pub fn insert(&mut self, bytes: &[u8]) {
        let hash = murmurhash3_x64_128(bytes, 0).0;
        self.insert_hash_value(hash);
    }

    pub fn insert_hash_value(&mut self, hash_val: u64) {
        // If the hashed value is already covered by the mask, we can skip it.
        // This is because the number of trailing zeroes in the hashed value is less
        // than the mask.
        if (hash_val & self.mask) != 0 {
            return;
        }
        // Put the hashed value into the hashset.
        self.hash_set.insert(hash_val);
        // We track the unique hashed values level by level to ensure a minimum count of
        // distinct values at each level. This way, the final estimation is less
        // likely to be skewed by outliers.
        if self.hash_set.len() > self.max_size {
            // If the size of the hashset exceeds the maximum size, move the mask to the
            // next level.
            let mask = (self.mask << 1) | 1;
            // Clean up the hashset by removing the hashed values with trailing zeroes less
            // than the new mask.
            self.hash_set.retain(|&x| x & mask == 0);
            self.mask = mask;
        }
    }
}

impl From<FmSketch> for tipb::FmSketch {
    fn from(fm: FmSketch) -> tipb::FmSketch {
        let mut proto = tipb::FmSketch::default();
        proto.set_mask(fm.mask);
        let hash = fm.hash_set.into_iter().collect();
        proto.set_hashset(hash);
        proto
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::repeat, slice::from_ref};

    use tidb_query_datatype::{
        codec::{datum, datum::Datum, Result},
        expr::EvalContext,
    };

    use super::*;

    struct TestData {
        samples: Vec<Datum>,
        rc: Vec<Datum>,
        pk: Vec<Datum>,
    }

    fn generate_samples(count: usize) -> Vec<Datum> {
        let start = 1000;
        let mut samples: Vec<usize> = (0..1)
            .chain(repeat(2).take(start - 1))
            .chain(1000..count)
            .collect();
        let mut id = start;
        while id < count {
            samples[id] += 1;
            id += 3;
        }

        id = start;
        while id < count {
            samples[id] += 2;
            id += 5;
        }
        samples.into_iter().map(|v| Datum::I64(v as i64)).collect()
    }

    impl Default for TestData {
        fn default() -> TestData {
            let samples = generate_samples(10000);
            let count = 100000;
            let rc = generate_samples(count);
            TestData {
                samples,
                rc,
                pk: (0..count as i64).map(Datum::I64).collect(),
            }
        }
    }

    fn build_fmsketch(values: &[Datum], max_size: usize) -> Result<FmSketch> {
        let mut s = FmSketch::new(max_size);
        for value in values {
            let bytes = datum::encode_value(&mut EvalContext::default(), from_ref(value))?;
            s.insert(&bytes);
        }
        Ok(s)
    }

    impl FmSketch {
        // ndv returns the approximate number of distinct elements
        pub fn ndv(&self) -> u64 {
            // The estimated count of distinct values is 2^r * count, where 'r' is the
            // maximum number of trailing zeroes observed and 'count' is the number of
            // unique hashed values. The fundamental idea is that the hash
            // function maps the input domain onto a logarithmic scale.
            // This is achieved by hashing the input value and counting the number of
            // trailing zeroes in the binary representation of the hash value.
            // So the count of distinct values with 'r' trailing zeroes is n / 2^r, where
            // 'n' is the number of distinct values. Therefore, the estimated
            // count of distinct values is 2^r * count = n.
            (self.mask + 1) * (self.hash_set.len() as u64)
        }
    }

    // This test was ported from tidb.
    #[test]
    fn test_sketch() {
        let max_size = 1000;
        let data = TestData::default();
        let sample = build_fmsketch(&data.samples, max_size).unwrap();
        assert_eq!(sample.ndv(), 6232);
        let rc = build_fmsketch(&data.rc, max_size).unwrap();
        assert_eq!(rc.ndv(), 73344);
        let pk = build_fmsketch(&data.pk, max_size).unwrap();
        assert_eq!(pk.ndv(), 100480);

        let max_size = 2;
        let mut sketch = FmSketch::new(max_size);
        sketch.insert_hash_value(1);
        sketch.insert_hash_value(2);
        assert_eq!(sketch.hash_set.len(), max_size);
        sketch.insert_hash_value(4);
        assert_eq!(sketch.hash_set.len(), max_size);
    }
}

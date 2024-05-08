// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashSet;
use mur3::murmurhash3_x64_128;

/// FMSketch (Flajolet–Martin Sketch) is a probabilistic data structure used for
/// estimating the number of distinct elements in a stream. It uses a hash
/// function to map each element to a binary number and counts the number of
/// trailing zeroes in each hashed value. The maximum number of trailing zeroes
/// observed gives an estimate of the logarithm of the number of distinct
/// elements. This approach allows the FM sketch to handle large streams of data
/// in a memory-efficient way.
///
/// See https://en.wikipedia.org/wiki/Flajolet%E2%80%93Martin_algorithm
#[derive(Clone)]
pub struct FmSketch {
    /// A binary mask used to track the maximum number of trailing zeroes in the
    /// hashed values.
    mask: u64,
    /// The maximum size of the hashset. If the size exceeds this value, the
    /// mask size will be doubled and some hashed values will be removed
    /// from the hashset.
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
        // If the hashed value is already in the sketch (determined by bitwise AND with
        // the mask), return without inserting. This is because the number of
        // trailing zeroes in the hashed value is less than or equal to the mask value.
        if (hash_val & self.mask) != 0 {
            return;
        }
        // Put the hashed value into the hashset.
        self.hash_set.insert(hash_val);
        // If the count of unique hashed values exceeds the maximum size,
        // double the mask size and remove any hashed values from the hashset that are
        // now within the mask. This is to ensure that the mask value is always
        // a power of two minus one (i.e., a binary number of the form 111...),
        // which allows us to quickly check the number of trailing zeroes in a hashed
        // value by performing a bitwise AND operation with the mask.
        if self.hash_set.len() > self.max_size {
            let mask = (self.mask << 1) | 1;
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
            // The size of the mask (incremented by one) is 2^r, where r is the maximum
            // number of trailing zeroes observed in the hashed values.
            // The count of unique hashed values is the number of unique elements in the
            // hashset. This estimation method is based on the Flajolet-Martin
            // algorithm for estimating the number of distinct elements in a stream.
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

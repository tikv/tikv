// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashSet;
use murmur3::murmur3_x64_128;

/// `FmSketch` is used to count the approximate number of distinct
/// elements in multiset.
/// Refer:[Flajolet-Martin](https://en.wikipedia.org/wiki/Flajolet%E2%80%93Martin_algorithm)
#[derive(Clone)]
pub struct FmSketch {
    mask: u64,
    max_size: usize,
    hash_set: HashSet<u64>,
}

impl FmSketch {
    pub fn new(max_size: usize) -> FmSketch {
        FmSketch {
            mask: 0,
            max_size,
            hash_set: HashSet::with_capacity_and_hasher(max_size + 1, Default::default()),
        }
    }

    pub fn insert(&mut self, mut bytes: &[u8]) {
        let hash = {
            let out = murmur3_x64_128(&mut bytes, 0).unwrap();
            out as u64
        };
        self.insert_hash_value(hash);
    }

    pub fn into_proto(self) -> tipb::FmSketch {
        let mut proto = tipb::FmSketch::default();
        proto.set_mask(self.mask);
        let hash = self.hash_set.into_iter().collect();
        proto.set_hashset(hash);
        proto
    }

    fn insert_hash_value(&mut self, hash_val: u64) {
        if (hash_val & self.mask) != 0 {
            return;
        }
        self.hash_set.insert(hash_val);
        if self.hash_set.len() > self.max_size {
            let mask = (self.mask << 1) | 1;
            self.hash_set.retain(|&x| x & mask == 0);
            self.mask = mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::iter::repeat;
    use std::slice::from_ref;

    use tidb_query_datatype::codec::datum;
    use tidb_query_datatype::codec::datum::Datum;
    use tidb_query_datatype::codec::Result;
    use tidb_query_datatype::expr::EvalContext;

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

    pub fn build_fmsketch(values: &[Datum], max_size: usize) -> Result<FmSketch> {
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

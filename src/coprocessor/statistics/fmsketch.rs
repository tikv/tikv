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
    /// Every time the retained hashes exceed the maximum size, the mask
    /// will be moved to the next level.
    mask: u64,
    /// The maximum number of retained hashes, counting both sets in sampled
    /// mode. If the count exceeds this value, the mask will be moved to the
    /// next level. And only the hashed values with trailing zeroes greater
    /// than or equal to the new mask are kept.
    max_size: usize,
    /// All hashes in full-input mode; singleton hashes in sampled mode.
    hash_set: HashSet<u64>,
    /// The hashes seen more than once. `None` keeps full-input mode free of
    /// duplicate tracking. In sampled mode the two sets are disjoint and
    /// share one mask and capacity bound.
    multi_hash_set: Option<HashSet<u64>>,
}

impl FmSketch {
    /// Creates a new FmSketch with the given maximum size.
    pub fn new(max_size: usize) -> FmSketch {
        FmSketch {
            mask: 0,
            max_size,
            hash_set: HashSet::with_capacity_and_hasher(max_size + 1, Default::default()),
            multi_hash_set: None,
        }
    }

    pub fn track_duplicates(&mut self) {
        // A hash that is already in the sketch would count as seen one time.
        debug_assert!(self.hash_set.is_empty());
        self.multi_hash_set = Some(HashSet::default());
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
        if let Some(multi_hash_set) = &mut self.multi_hash_set {
            if multi_hash_set.contains(&hash_val) {
                return;
            }
            if self.hash_set.remove(&hash_val) {
                multi_hash_set.insert(hash_val);
                return;
            }
        }
        // Put the hashed value into the hashset.
        self.hash_set.insert(hash_val);
        // We track the unique hashed values level by level to ensure a minimum count of
        // distinct values at each level. This way, the final estimation is less
        // likely to be skewed by outliers.
        if self.hash_set.len() + self.multi_hash_set.as_ref().map_or(0, HashSet::len)
            > self.max_size
        {
            // If the size of the hashset exceeds the maximum size, move the mask to the
            // next level.
            self.mask = (self.mask << 1) | 1;
            // Clean up the hashset by removing the hashed values with trailing zeroes less
            // than the new mask.
            self.filter();
        }
    }

    /// Records a hash that was seen more than once.
    fn insert_repeated_hash(&mut self, hash_val: u64) {
        // The first insert of a new hash puts it in `hash_set`, and the next
        // insert moves it to `multi_hash_set`. Thus two inserts are necessary
        // when the hash is new here, and they do no harm in the other cases.
        self.insert_hash_value(hash_val);
        self.insert_hash_value(hash_val);
    }

    fn filter(&mut self) {
        self.hash_set.retain(|&x| x & self.mask == 0);
        if let Some(multi_hash_set) = &mut self.multi_hash_set {
            multi_hash_set.retain(|&x| x & self.mask == 0);
        }
    }

    pub fn merge(&mut self, other: &FmSketch) {
        if self.mask < other.mask {
            self.mask = other.mask;
            self.filter();
        }
        for hash in &other.hash_set {
            self.insert_hash_value(*hash);
        }
        if let Some(other_multi_hash_set) = &other.multi_hash_set {
            for &hash in other_multi_hash_set {
                self.insert_repeated_hash(hash);
            }
        }
    }
}

impl From<FmSketch> for tipb::FmSketch {
    fn from(fm: FmSketch) -> tipb::FmSketch {
        let mut proto = tipb::FmSketch::default();
        proto.set_mask(fm.mask);
        let hash = fm.hash_set.into_iter().collect();
        proto.set_hashset(hash);
        if let Some(multi_hash_set) = fm.multi_hash_set {
            proto.set_multi_hashset(multi_hash_set.into_iter().collect());
        }
        proto
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::repeat_n, slice::from_ref};

    use tidb_query_datatype::{
        codec::{Result, datum, datum::Datum},
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
            .chain(repeat_n(2, start - 1))
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

    #[test]
    fn test_merge() {
        // Sketches over halves of the data merge into the same sketch as one
        // built over all of it, whether the mask grows on the left (small
        // max_size forces levels) or the right side.
        let data = TestData::default();
        let max_size = 100;
        let whole = build_fmsketch(&data.samples, max_size).unwrap();
        let (left, right) = data.samples.split_at(data.samples.len() / 2);
        let mut merged = build_fmsketch(left, max_size).unwrap();
        merged.merge(&build_fmsketch(right, max_size).unwrap());
        assert_eq!(merged.mask, whole.mask);
        assert_eq!(merged.ndv(), whole.ndv());

        // Merging a leveled-up sketch (bigger mask) prunes the receiver.
        let small = build_fmsketch(&data.samples, 2).unwrap();
        let mut receiver = FmSketch::new(2);
        receiver.insert_hash_value(1);
        assert_eq!(receiver.mask, 0);
        receiver.merge(&small);
        assert!(receiver.mask >= small.mask);
        assert!(receiver.hash_set.iter().all(|h| h & receiver.mask == 0));

        // `multi_hash_set` must survive both cross-input duplicates and a
        // higher mask, regardless of which input is merged first.
        //
        // `whole` comes from the same insert code as the merge inputs, so a
        // comparison with `whole` alone cannot detect a wrong rule that both
        // sides share, for example a capacity that counts only one of the two
        // sets. The literal values detect it. With a capacity of 2 the mask
        // reaches 3, and only 0 and 64 pass that mask.
        for (capacity, mask, singletons, repeated) in [
            (2, 3, vec![], vec![0, 64]),
            (16, 0, vec![2, 3], vec![0, 1, 64]),
        ] {
            let build = |values: &[u64]| {
                let mut sketch = FmSketch::new(capacity);
                sketch.track_duplicates();
                for &value in values {
                    sketch.insert_hash_value(value);
                }
                sketch
            };
            let whole = build(&[0, 1, 1, 2, 64, 0, 3, 64, 64]);
            assert_eq!(whole.mask, mask);
            assert_eq!(
                whole.hash_set,
                singletons.into_iter().collect::<HashSet<_>>()
            );
            assert_eq!(whole.multi_hash_set, Some(repeated.into_iter().collect()));
            let left = build(&[0, 1, 1, 2, 64]);
            let right = build(&[0, 3, 64, 64]);
            for (mut first, second) in [(left.clone(), right.clone()), (right, left)] {
                first.merge(&second);
                assert_eq!(first.mask, whole.mask);
                assert_eq!(first.hash_set, whole.hash_set);
                assert_eq!(first.multi_hash_set, whole.multi_hash_set);
                assert!(
                    first
                        .hash_set
                        .is_disjoint(first.multi_hash_set.as_ref().unwrap())
                );
            }
        }
    }
}

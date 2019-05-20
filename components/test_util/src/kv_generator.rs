// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use rand::prelude::*;
use rand_isaac::isaac::IsaacRng;

/// A random generator of kv.
///
/// Every iteration should be taken in µs.
#[derive(Clone, Debug)]
pub struct KvGenerator {
    key_len: usize,
    value_len: usize,
    rng: IsaacRng,
}

impl KvGenerator {
    pub fn new(key_len: usize, value_len: usize) -> KvGenerator {
        KvGenerator {
            key_len,
            value_len,
            rng: FromEntropy::from_entropy(),
        }
    }

    pub fn with_seed(key_len: usize, value_len: usize, seed: u64) -> KvGenerator {
        KvGenerator {
            key_len,
            value_len,
            rng: IsaacRng::seed_from_u64(seed),
        }
    }

    /// Generate n pair of KVs.
    ///
    /// This function consumes current generator.
    pub fn generate(self, n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.take(n).collect()
    }
}

impl Iterator for KvGenerator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let mut k = vec![0; self.key_len];
        self.rng.fill_bytes(&mut k);
        let mut v = vec![0; self.value_len];
        self.rng.fill_bytes(&mut v);

        Some((k, v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[bench]
    fn bench_kv_generator(b: &mut Bencher) {
        let mut g = KvGenerator::new(100, 1000);
        b.iter(|| g.next());
    }
}

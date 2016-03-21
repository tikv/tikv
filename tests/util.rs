use rand::{self, Rng, ThreadRng};

/// A random generator of kv.
/// Every iter should be taken in µs. See also benches::bench_kv_iter.
pub struct KVGenerator {
    key_len: usize,
    value_len: usize,
    rng: ThreadRng,
}

impl KVGenerator {
    pub fn new(key_len: usize, value_len: usize) -> KVGenerator {
        KVGenerator {
            key_len: key_len,
            value_len: value_len,
            rng: rand::thread_rng(),
        }
    }
}

impl Iterator for KVGenerator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let mut k = vec![0; self.key_len];
        self.rng.fill_bytes(&mut k);
        let mut v = vec![0; self.value_len];
        self.rng.fill_bytes(&mut v);

        Some((k, v))
    }
}

/// Generate n pair of kvs.
pub fn generate_random_kvs(n: usize, key_len: usize, value_len: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let kv_generator = KVGenerator::new(key_len, value_len);
    kv_generator.take(n).collect()
}

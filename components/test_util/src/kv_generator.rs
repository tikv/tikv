// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use rand::{self, Rng, ThreadRng};

/// A random generator of kv.
/// Every iter should be taken in µs. See also `benches::bench_kv_iter`.
pub struct KvGenerator {
    key_len: usize,
    value_len: usize,
    rng: ThreadRng,
}

impl KvGenerator {
    pub fn new(key_len: usize, value_len: usize) -> KvGenerator {
        KvGenerator {
            key_len,
            value_len,
            rng: rand::thread_rng(),
        }
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

/// Generate n random pair of kvs.
pub fn generate_random_kvs(n: usize, key_len: usize, value_len: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let kv_generator = KvGenerator::new(key_len, value_len);
    kv_generator.take(n).collect()
}

/// Generate n deliberate pair of kvs.
pub fn generate_deliberate_kvs(
    n: usize,
    key_len: usize,
    value_len: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut ret = Vec::with_capacity(n);
    for i in 0..n {
        let k = generator_vec_from_seed(key_len, i);
        let v = generator_vec_from_seed(value_len, i);
        ret.push((k, v));
    }
    ret
}

/// generate vector.
/// # Examples
///
/// Basic usage:
///
/// ```
///    let v1 = generator_vec_from_seed(3, 1);
///    assert_eq!("001".as_bytes().to_vec(), v1);
///
///    let v005 = generator_vec_from_seed(3, 5);
///    assert_eq!("005".as_bytes().to_vec(), v005);
///
///    let v125 = generator_vec_from_seed(2, 125);
///    assert_eq!("25".as_bytes().to_vec(), v125);
///
///    let v = generator_vec_from_seed(0, 125);
///    assert_eq!("".as_bytes().to_vec(), v);
///```
fn generator_vec_from_seed(len: usize, seed: usize) -> Vec<u8> {
    use std::iter::repeat;
    let mut s = format!("{}", seed).into_bytes();
    if s.len() != len {
        if s.len() < len {
            let mut zeros: Vec<u8> = repeat('0' as u8).take(len - s.len()).collect();
            zeros.append(&mut s);
            s = zeros;
        } else {
            let ri = s.len() - len;
            s = s.split_off(ri);
        }
    }
    s
}

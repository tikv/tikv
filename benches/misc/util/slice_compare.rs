// Copyright 2019 PingCAP, Inc.
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

use rand::{thread_rng, Rng};
use test::Bencher;

#[inline]
fn gen_rand_str(len: usize) -> Vec<u8> {
    let mut rand_str = vec![0; len];
    thread_rng().fill_bytes(&mut rand_str);
    rand_str
}

fn bench_slice_compare_less(b: &mut Bencher, n: usize) {
    let (s1, s2) = (gen_rand_str(n), gen_rand_str(n));
    b.iter(|| s1 < s2);
}

fn bench_slice_compare_greater(b: &mut Bencher, n: usize) {
    let (s1, s2) = (gen_rand_str(n), gen_rand_str(n));
    b.iter(|| s1 > s2);
}

#[bench]
fn bench_slice_compare(b: &mut Bencher) {
    for n in &[16, 32, 64, 128, 256, 512, 1024] {
        bench_slice_compare_less(b, *n as usize);
        bench_slice_compare_greater(b, *n as usize);
    }
}

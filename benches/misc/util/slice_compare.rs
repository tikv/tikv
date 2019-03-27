// Copyright 2019 TiKV Project Authors.
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
fn bench_slice_compare_less_32(b: &mut Bencher) {
    bench_slice_compare_less(b, 32)
}

#[bench]
fn bench_slice_compare_less_64(b: &mut Bencher) {
    bench_slice_compare_less(b, 64)
}

#[bench]
fn bench_slice_compare_less_128(b: &mut Bencher) {
    bench_slice_compare_less(b, 128)
}

#[bench]
fn bench_slice_compare_greater_32(b: &mut Bencher) {
    bench_slice_compare_greater(b, 32)
}

#[bench]
fn bench_slice_compare_greater_64(b: &mut Bencher) {
    bench_slice_compare_greater(b, 64)
}

#[bench]
fn bench_slice_compare_greater_128(b: &mut Bencher) {
    bench_slice_compare_greater(b, 128)
}

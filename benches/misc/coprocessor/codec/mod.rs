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

mod chunk;
mod mysql;

use byteorder::{BigEndian, ByteOrder, LittleEndian};
use test::black_box;
use test::Bencher;

use tikv::coprocessor::codec::table::*;

#[bench]
fn bench_table_prefix_start_with(b: &mut Bencher) {
    let key: &[u8] = b"tabc";
    b.iter(|| {
        let n = black_box(1000);
        (0..n).all(|_| black_box(key.starts_with(TABLE_PREFIX)))
    });
}

#[bench]
fn bench_table_prefix_check(b: &mut Bencher) {
    let key: &[u8] = b"tabc";
    b.iter(|| {
        let n = black_box(1000);
        (0..n).all(|_| black_box(key.len() > 1 && key[0] == TABLE_PREFIX[0]))
    });
}

#[bench]
fn bench_record_prefix_start_with(b: &mut Bencher) {
    let key: &[u8] = b"_rabc";
    b.iter(|| {
        let n = black_box(1000);
        (0..n).all(|_| black_box(key.starts_with(RECORD_PREFIX_SEP)))
    });
}

#[bench]
fn bench_record_prefix_equal_check(b: &mut Bencher) {
    let key: &[u8] = b"_rabc";
    b.iter(|| {
        let n = black_box(1000);
        (0..n).all(|_| {
            black_box(
                key.len() > 2 && key[0] == RECORD_PREFIX_SEP[0] && key[1] == RECORD_PREFIX_SEP[1],
            )
        })
    });
}

#[bench]
fn bench_record_prefix_bigendian_check(b: &mut Bencher) {
    let key: &[u8] = b"_rabc";
    let prefix: u16 = BigEndian::read_u16(RECORD_PREFIX_SEP);
    b.iter(|| {
        let n = black_box(1000);
        (0..n).all(|_| black_box(key.len() > 2 && BigEndian::read_u16(key) == prefix))
    });
}

#[bench]
fn bench_record_prefix_littleendian_check(b: &mut Bencher) {
    let key: &[u8] = b"_rabc";
    let prefix: u16 = LittleEndian::read_u16(RECORD_PREFIX_SEP);
    b.iter(|| {
        let n = black_box(1000);
        (0..n).all(|_| black_box(key.len() > 2 && LittleEndian::read_u16(key) == prefix))
    });
}

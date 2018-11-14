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

use super::{engine_get_bench, engine_put_bench, engine_snapshot_bench, ITERATIONS};
use test::Bencher;
use tikv::storage::engine::BTreeEngine;

#[bench]
fn bench_engine_get_btree_1000_times_from_000_000_size(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_get_bench(&engine, ITERATIONS, 0, false, bencher);
}

#[bench]
fn bench_engine_get_btree_1000_times_from_001_000_size(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_get_bench(&engine, ITERATIONS, 1000, false, bencher);
}

#[bench]
fn bench_engine_get_btree_1000_times_from_010_000_size(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_get_bench(&engine, ITERATIONS, 10_000, false, bencher);
}

#[bench]
fn bench_engine_get_btree_1000_times_from_100_000_size(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_get_bench(&engine, ITERATIONS, 100_000, false, bencher);
}

#[bench]
fn bench_engine_get_btree_1000_times_from_1_000_000_size(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_get_bench(&engine, ITERATIONS, 1_000_000, false, bencher);
}

#[bench]
fn bench_engine_put_btree_001_000_items(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_put_bench(&engine, ITERATIONS, bencher);
}

#[bench]
fn bench_engine_put_btree_010_000_items(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_put_bench(&engine, 10_000, bencher);
}

#[bench]
fn bench_engine_put_btree_100_000_items(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_put_bench(&engine, 100_000, bencher);
}

#[bench]
fn bench_engine_snapshot_btree_1000_times_from_000_000_size(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_snapshot_bench(&engine, ITERATIONS, 0, bencher);
}

#[bench]
fn bench_engine_snapshot_btree_1000_times_from_001_000_size(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_snapshot_bench(&engine, ITERATIONS, 1000, bencher);
}

#[bench]
fn bench_engine_snapshot_btree_1000_times_from_010_000_size(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_snapshot_bench(&engine, ITERATIONS, 10_000, bencher);
}

#[bench]
fn bench_engine_snapshot_btree_1000_times_from_100_000_size(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_snapshot_bench(&engine, ITERATIONS, 100_000, bencher);
}

#[bench]
fn bench_engine_snapshot_btree_1000_times_from_1_000_000_size(bencher: &mut Bencher) {
    let engine = BTreeEngine::default();
    engine_snapshot_bench(&engine, ITERATIONS, 1_000_000, bencher);
}

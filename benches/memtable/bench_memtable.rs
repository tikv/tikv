// Copyright 2017 PingCAP, Inc.
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

use test::Bencher;
use tempdir::TempDir;
use rocksdb::{DB, Writable, Options as RocksdbOptions, BlockBasedOptions};
use tikv::util::rocksdb;
use rand::{Rng, thread_rng};

fn random_str(len: usize) -> String {
    thread_rng().gen_ascii_chars().take(len).collect::<String>()
}

fn fill_memtable(db: &DB, target_size: usize, key_len: usize) {
    let mut cur_size = 0;
    let v = b"this is the content, this is the content";
    loop {
        if cur_size >= target_size {
            break;
        }

        let k = random_str(key_len);
        db.put(k.as_bytes(), v).unwrap();
        cur_size += k.len();
        cur_size += v.len();
    }
}

fn bench_memtable_get(b: &mut Bencher, memtable_size: usize, existed: bool) {
    let path = TempDir::new("/tmp/rocksdb_memtable_get").unwrap();
    let mut db_opts = RocksdbOptions::new();
    db_opts.create_if_missing(true);

    let mut opts = RocksdbOptions::new();
    opts.set_write_buffer_size(128 * 1024 * 1024);

    let db = rocksdb::new_engine_opt(path.path().to_str().unwrap(), db_opts, vec![rocksdb::CFOptions::new("default", opts)]).unwrap();
    fill_memtable(&db, memtable_size, 30);

    let k = random_str(31);
    if existed {
        db.put(k.as_bytes(), b"this is content").unwrap();
    }

    b.iter(|| {
        db.get(k.as_bytes()).unwrap();
    });
}

fn bench_memtable_get_with_bloom(b: &mut Bencher, memtable_size: usize, existed: bool) {
    let path = TempDir::new("/tmp/rocksdb_memtable_get_with_bloom").unwrap();
    let mut db_opts = RocksdbOptions::new();
    db_opts.create_if_missing(true);

    let mut block_based_opts = BlockBasedOptions::new();
    block_based_opts.set_block_size(16 * 1024);
    block_based_opts.set_bloom_filter(10, false);
    block_based_opts.set_whole_key_filtering(false);

    let mut opts = RocksdbOptions::new();
    opts.set_block_based_table_factory(&block_based_opts);
    opts.set_write_buffer_size(128 * 1024 * 1024);

    opts.set_prefix_extractor("NoopSliceTransform",
                             Box::new(rocksdb::NoopSliceTransform{}))
    .unwrap_or_else(|err| panic!("{:?}", err));
    opts.set_memtable_prefix_bloom_size_ratio(0.2 as f64);

    let db = rocksdb::new_engine_opt(path.path().to_str().unwrap(), RocksdbOptions::new(), vec![rocksdb::CFOptions::new("default", opts)]).unwrap();
    fill_memtable(&db, memtable_size, 30);

    let k = random_str(31);
    if existed {
        db.put(k.as_bytes(), b"this is content").unwrap();
    }

    b.iter(|| {
        db.get(k.as_bytes()).unwrap();
    });
}

#[bench]
fn bench_memtable_get_not_exist_64mb(b: &mut Bencher) {
    bench_memtable_get(b, 64 * 1024 * 1024, false);
}

#[bench]
fn bench_memtable_get_not_exist_with_bloom_64mb(b: &mut Bencher) {
    bench_memtable_get_with_bloom(b, 64 * 1024 * 1024, false);
}

#[bench]
fn bench_memtable_get_not_exist_32mb(b: &mut Bencher) {
    bench_memtable_get(b, 32 * 1024 * 1024, false);
}

#[bench]
fn bench_memtable_get_not_exist_with_bloom_32mb(b: &mut Bencher) {
    bench_memtable_get_with_bloom(b, 32 * 1024 * 1024, false);
}

#[bench]
fn bench_memtable_get_exist_64mb(b: &mut Bencher) {
    bench_memtable_get(b, 64 * 1024 * 1024, true);
}

#[bench]
fn bench_memtable_get_exist_with_bloom_64mb(b: &mut Bencher) {
    bench_memtable_get_with_bloom(b, 64 * 1024 * 1024, true);
}

#[bench]
fn bench_memtable_get_exist_32mb(b: &mut Bencher) {
    bench_memtable_get(b, 32 * 1024 * 1024, true);
}

#[bench]
fn bench_memtable_get_exist_with_bloom_32mb(b: &mut Bencher) {
    bench_memtable_get_with_bloom(b, 32 * 1024 * 1024, true);
}
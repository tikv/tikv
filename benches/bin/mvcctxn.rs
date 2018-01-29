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

extern crate rand;

use std::sync::Arc;

use tikv::storage::{Key, Modify, Mutation, Options, Snapshot, SnapshotStore, Statistics};
use tikv::storage::mvcc::MvccTxn;
use tikv::raftstore::store::engine::SyncSnapshot;
use tikv::config::DbConfig;
use tikv::util::config::ReadableSize;
use tikv::util::rocksdb::{get_cf_handle, new_engine_opt};
use kvproto::kvrpcpb::IsolationLevel;
use rocksdb::{Writable, WriteBatch, DB};
use rocksdb::rocksdb_options::DBOptions;

use rand::Rng;
use tempdir::TempDir;

use utils::*;

#[inline]
fn do_write(db: &DB, modifies: Vec<Modify>) {
    let wb = WriteBatch::new();
    for rev in modifies {
        match rev {
            Modify::Delete(cf, k) => {
                let handle = get_cf_handle(db, cf).unwrap();
                wb.delete_cf(handle, k.encoded()).unwrap();
            }
            Modify::Put(cf, k, v) => {
                let handle = get_cf_handle(db, cf).unwrap();
                wb.put_cf(handle, k.encoded(), &v).unwrap();
            }
            Modify::DeleteRange(cf, start_key, end_key) => {
                let handle = get_cf_handle(db, cf).unwrap();
                wb.delete_range_cf(handle, start_key.encoded(), end_key.encoded())
                    .unwrap();
            }
        }
    }
    db.write_without_wal(wb).unwrap();
}

#[inline]
fn get_snapshot(db: Arc<DB>) -> Box<Snapshot> {
    box SyncSnapshot::new(db) as Box<Snapshot>
}

#[inline]
fn prewrite(db: Arc<DB>, mutations: &[Mutation], primary: &[u8], start_ts: u64) {
    let snapshot = get_snapshot(Arc::clone(&db));
    let mut txn = MvccTxn::new(snapshot, start_ts, None, IsolationLevel::SI, false);
    for m in mutations {
        txn.prewrite(m.clone(), primary, &Options::default())
            .unwrap();
    }
    do_write(&*db, txn.into_modifies());
}

#[inline]
fn commit(db: Arc<DB>, keys: &[Key], start_ts: u64, commit_ts: u64) {
    let snapshot = get_snapshot(Arc::clone(&db));
    let mut txn = MvccTxn::new(snapshot, start_ts, None, IsolationLevel::SI, false);
    for key in keys {
        txn.commit(key, commit_ts).unwrap();
    }
    do_write(&*db, txn.into_modifies());
}

fn prepare_test_db(versions: usize, value_len: usize, keys: &[Vec<u8>], path: &str) -> Arc<DB> {
    let mut config = DbConfig::default();
    // Use a huge write_buffer_size to avoid flushing data to disk.
    config.defaultcf.write_buffer_size = ReadableSize::gb(1);
    config.writecf.write_buffer_size = ReadableSize::gb(1);
    config.lockcf.write_buffer_size = ReadableSize::gb(1);

    let cf_ops = config.build_cf_opts();

    let db = Arc::new(new_engine_opt(path, DBOptions::new(), cf_ops).unwrap());

    for _ in 0..versions {
        for key in keys {
            let value = vec![0u8; value_len];
            let start_ts = next_ts();
            let commit_ts = next_ts();

            prewrite(
                Arc::clone(&db),
                &[Mutation::Put((Key::from_raw(key), value))],
                key,
                start_ts,
            );
            commit(Arc::clone(&db), &[Key::from_raw(key)], start_ts, commit_ts);
        }
    }
    db
}

#[inline]
fn get(db: Arc<DB>, key: &Key, statistics: &mut Statistics) -> Option<Vec<u8>> {
    let snapshot = get_snapshot(Arc::clone(&db));
    let start_ts = next_ts();
    let snapstore = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
    snapstore.get(key, statistics).unwrap()
}

fn bench_get(db: Arc<DB>, keys: &[Vec<u8>]) -> f64 {
    let mut fake_statistics = Statistics::default();
    let mut rng = rand::thread_rng();
    do_bench(
        || {
            let key = rng.choose(keys).unwrap();
            let key = Key::from_raw(key);

            get(Arc::clone(&db), &key, &mut fake_statistics).unwrap()
        },
        500000,
    )
}

fn bench_set(db: Arc<DB>, keys: &[Vec<u8>], value_len: usize) -> f64 {
    let mut rng = rand::thread_rng();
    do_bench(
        || {
            let start_ts = next_ts();
            let commit_ts = next_ts();
            let value = vec![0u8; value_len];

            let key = rng.choose(keys).unwrap();

            prewrite(
                Arc::clone(&db),
                &[Mutation::Put((Key::from_raw(key), value))],
                key,
                start_ts,
            );
            commit(Arc::clone(&db), &[Key::from_raw(key)], start_ts, commit_ts)
        },
        500000,
    )
}

fn bench_delete(db: Arc<DB>, keys: &[Vec<u8>]) -> f64 {
    let mut rng = rand::thread_rng();
    do_bench(
        || {
            let start_ts = next_ts();
            let commit_ts = next_ts();

            let key = rng.choose(keys).unwrap();
            prewrite(
                Arc::clone(&db),
                &[Mutation::Delete(Key::from_raw(key))],
                key,
                start_ts,
            );
            commit(Arc::clone(&db), &[Key::from_raw(key)], start_ts, commit_ts)
        },
        500000,
    )
}

fn bench_batch_set_impl(
    db: Arc<DB>,
    keys: &mut [Vec<u8>],
    value_len: usize,
    batch_size: usize,
) -> f64 {
    let mut rng = rand::thread_rng();

    rng.shuffle(keys);

    let all_batch_count = keys.len() / batch_size;
    let value = vec![0u8; value_len];

    do_bench(
        || {
            let start_ts = next_ts();
            let commit_ts = next_ts();

            let index = rng.gen_range(0, all_batch_count);
            let keys_to_write: Vec<_> = keys[index..index + batch_size]
                .iter()
                .map(|key| Key::from_raw(key))
                .collect();
            let mutations: Vec<_> = keys_to_write
                .iter()
                .map(|key| Mutation::Put((key.clone(), value.clone())))
                .collect();

            let primary = &keys[index];
            prewrite(Arc::clone(&db), &mutations, primary, start_ts);
            commit(Arc::clone(&db), &keys_to_write, start_ts, commit_ts)
        },
        640000 / (batch_size as u32),
    )
}

enum BenchType {
    Row,
    UniqueIndex,
}

// Run all bench with specified parameters
fn bench_single_row(
    table_size: usize,
    version_count: usize,
    data_len: usize,
    bench_type: &BenchType,
) {
    let (mut keys, value_len, log_name) = match *bench_type {
        BenchType::Row => (generate_row_keys(1, 0, table_size), data_len, "row"),
        BenchType::UniqueIndex => (
            generate_unique_index_keys(1, 1, data_len, table_size),
            8,
            "unique index",
        ),
    };

    let mut rng = rand::thread_rng();
    rng.shuffle(&mut keys);

    let dir = TempDir::new("bench-mvcctxn").unwrap();
    let db = prepare_test_db(
        version_count,
        value_len,
        &keys,
        dir.path().to_str().unwrap(),
    );

    println!(
        "benching mvcctxn {} get\trows:{} versions:{} data len:{}\t...",
        log_name, table_size, version_count, data_len
    );
    let ns = bench_get(Arc::clone(&db), &keys) as u64;
    println!("\t{:>11} ns per op  {:>11} ops", ns, 1_000_000_000 / ns);

    println!(
        "benching mvcctxn {} set\trows:{} versions:{} data len:{}\t...",
        log_name, table_size, version_count, data_len
    );
    let ns = bench_set(Arc::clone(&db), &keys, value_len) as u64;
    println!("\t{:>11} ns per op  {:>11} ops", ns, 1_000_000_000 / ns);

    // Generate new db to bench delete, for the size of content was increased when benching set
    let dir = TempDir::new("bench-mvcctxn").unwrap();
    let db = prepare_test_db(
        version_count,
        value_len,
        &keys,
        dir.path().to_str().unwrap(),
    );

    println!(
        "benching mvcctxn {} delete\trows:{} versions:{} data len:{}\t...",
        log_name, table_size, version_count, data_len
    );
    let ns = bench_delete(Arc::clone(&db), &keys) as u64;
    println!("\t{:>11} ns per op  {:>11} ops", ns, 1_000_000_000 / ns);
}

fn bench_batch_set(
    table_size: usize,
    batch_size: usize,
    version_count: usize,
    data_len: usize,
    bench_type: &BenchType,
) {
    let (mut keys, value_len, log_name) = match *bench_type {
        BenchType::Row => (generate_row_keys(1, 0, table_size), data_len, "row"),
        BenchType::UniqueIndex => (
            generate_unique_index_keys(1, 1, data_len, table_size),
            8,
            "unique index",
        ),
    };

    let mut rng = rand::thread_rng();
    rng.shuffle(&mut keys);

    let dir = TempDir::new("bench-mvcctxn").unwrap();
    let db = prepare_test_db(
        version_count,
        value_len,
        &keys,
        dir.path().to_str().unwrap(),
    );

    println!(
        "benching mvcctxn {} batch write\trows:{} versions:{} data len:{} batch:{}\t...",
        log_name, table_size, version_count, data_len, batch_size,
    );
    let ns = bench_batch_set_impl(Arc::clone(&db), &mut keys, value_len, batch_size);
    println!(
        "\t{:>11} ns per op  {:>11} ops  {:>11} ns per key  {:>11} key per sec",
        ns as u64,
        (1_000_000_000_f64 / ns) as u64,
        (ns / (batch_size as f64)) as u64,
        (1_000_000_000_f64 * (batch_size as f64) / ns) as u64
    );
}

pub fn bench_mvcctxn() {
    for bench_type in &[BenchType::Row, BenchType::UniqueIndex] {
        for version_count in &[1, 16, 64] {
            bench_single_row(10_000, *version_count, 128, bench_type);
        }

        for value_len in &[32, 128, 1024] {
            bench_single_row(10_000, 5, *value_len, bench_type);
        }

        for batch_size in &[8, 64, 128, 256] {
            bench_batch_set(10_000, *batch_size, 5, 128, bench_type);
        }
    }
}

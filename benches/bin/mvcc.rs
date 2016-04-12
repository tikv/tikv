// Copyright 2016 PingCAP, Inc.
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

use std::sync::Arc;
use test::BenchSamples;
use tempdir::TempDir;

use rocksdb;
use rocksdb::ffi::DBCompactionStyle;

use test_util::*;
use tikv::storage::{self, Dsn, Mutation, Key};
use tikv::storage::txn::TxnStore;
use kvproto::kvrpcpb::Context;

use super::print_result;

fn rocksdb_option() -> rocksdb::Options {
    let mut opts = rocksdb::Options::new();

    let mut block_base_opts = rocksdb::BlockBasedOptions::new();
    block_base_opts.set_block_size(64 * 1024);

    opts.set_block_based_table_factory(&block_base_opts);
    opts.increase_parallelism(8);
    opts.create_if_missing(true);
    opts.set_max_open_files(4096);
    opts.set_use_fsync(false);
    opts.set_bytes_per_sync(8 * 1024 * 1024);
    opts.set_disable_data_sync(false);
    opts.set_block_cache_size_mb(4 * 1024);
    opts.set_table_cache_num_shard_bits(6);
    // parallelism options
    opts.set_max_background_compactions(6);
    opts.set_max_background_flushes(2);
    // flush options
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_max_write_buffer_number(4);
    opts.set_min_write_buffer_number_to_merge(1);
    // level style compaction
    opts.set_target_file_size_base(64 * 1024 * 1024);
    // write stalls
    opts.set_level_zero_slowdown_writes_trigger(16);
    opts.set_level_zero_stop_writes_trigger(64);

    opts.set_compaction_style(DBCompactionStyle::DBUniversal);
    opts.set_filter_deletes(false);
    opts.set_disable_auto_compactions(false);

    opts
}

/// In mvcc kv is not actually deleted, which may cause performance issue
/// when doing scan.
fn bench_tombstone_scan(dsn: Dsn, option: Option<rocksdb::Options>) -> BenchSamples {
    let engine = match dsn {
        Dsn::RocksDBPath(path) => {
            if let Some(opts) = option {
                storage::engine::rocksdb::EngineRocksdb::new_with_option(path, &opts)
                    .map(|engine| -> Box<storage::Engine> { Box::new(engine) })
                    .unwrap()
            } else {
                storage::new_engine(dsn).unwrap()
            }
        }
        _ => storage::new_engine(dsn).unwrap(),
    };

    let store = TxnStore::new(Arc::new(engine));
    let mut ts_generator = 1..;

    let mut kvs = KvGenerator::new(100, 1000);

    for (k, v) in kvs.take(100000) {
        let mut ts = ts_generator.next().unwrap();
        store.prewrite(Context::new(),
                       vec![Mutation::Put((Key::from_raw(k.clone()), v))],
                       k.clone(),
                       ts)
             .expect("");
        store.commit(Context::new(),
                     vec![Key::from_raw(k.clone())],
                     ts,
                     ts_generator.next().unwrap())
             .expect("");

        ts = ts_generator.next().unwrap();
        store.prewrite(Context::new(),
                       vec![Mutation::Delete(Key::from_raw(k.clone()))],
                       k.clone(),
                       ts)
             .expect("");
        store.commit(Context::new(),
                     vec![Key::from_raw(k.clone())],
                     ts,
                     ts_generator.next().unwrap())
             .expect("");
    }

    kvs = KvGenerator::new(100, 1000);
    bench!{
        let (k, _) = kvs.next().unwrap();
        assert!(store.scan(Context::new(),
                                    Key::from_raw(k.clone()),
                                    1,
            ts_generator.next().unwrap())
            .unwrap()
            .is_empty())
    }
}

pub fn bench_engine() {
    let path = TempDir::new("bench-mvcc").unwrap();
    let dsn = Dsn::RocksDBPath(path.path().to_str().unwrap());
    printf!("benching tombstone scan with rocksdb turning options\t...\t");
    print_result(bench_tombstone_scan(dsn, Some(rocksdb_option())));
    printf!("benching tombstone scan with rocksdb\t...\t");
    print_result(bench_tombstone_scan(dsn, None));
    printf!("benching tombstone scan with memory\t...\t");
    print_result(bench_tombstone_scan(Dsn::Memory, None));
}

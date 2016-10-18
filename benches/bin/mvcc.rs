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

use test::BenchSamples;

#[allow(dead_code)]
#[path="../../tests/storage/sync_storage.rs"]
mod sync_storage;

use test_util::*;
use tikv::storage::{Mutation, Key};
use tikv::server::transport::MockRaftStoreRouter;
use kvproto::kvrpcpb::Context;
use self::sync_storage::SyncStorage;

use super::print_result;

/// In mvcc kv is not actually deleted, which may cause performance issue
/// when doing scan.
fn bench_tombstone_scan() -> BenchSamples {
    let store = SyncStorage::new(&Default::default(), MockRaftStoreRouter);
    let mut ts_generator = 1..;

    let mut kvs = KvGenerator::new(100, 1000);

    for (k, v) in kvs.take(100000) {
        let mut ts = ts_generator.next().unwrap();
        store.prewrite(Context::new(),
                      vec![Mutation::Put((Key::from_raw(&k), v))],
                      k.clone(),
                      ts)
            .expect("");
        store.commit(Context::new(),
                    vec![Key::from_raw(&k)],
                    ts,
                    ts_generator.next().unwrap())
            .expect("");

        ts = ts_generator.next().unwrap();
        store.prewrite(Context::new(),
                      vec![Mutation::Delete(Key::from_raw(&k))],
                      k.clone(),
                      ts)
            .expect("");
        store.commit(Context::new(),
                    vec![Key::from_raw(&k)],
                    ts,
                    ts_generator.next().unwrap())
            .expect("");
    }

    kvs = KvGenerator::new(100, 1000);
    bench!{
        let (k, _) = kvs.next().unwrap();
        assert!(store.scan(Context::new(),
                           Key::from_raw(&k),
                           1,
                           ts_generator.next().unwrap())
                     .unwrap()
                     .is_empty())
    }
}

pub fn bench_engine() {
    printf!("benching tombstone scan with rocksdb\t...\t");
    print_result(bench_tombstone_scan());
}

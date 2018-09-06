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

use test::Bencher;

use kvproto::kvrpcpb::Context;

use test_storage::SyncStorage;
use test_util::*;
use tikv::server::readpool::{self, ReadPool};
use tikv::storage::{self, Key, Mutation};
use tikv::util::worker::FutureWorker;

/// In mvcc kv is not actually deleted, which may cause performance issue
/// when doing scan.
#[ignore]
#[bench]
fn bench_tombstone_scan(b: &mut Bencher) {
    let pd_worker = FutureWorker::new("test-pd-worker");
    let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
        || storage::ReadPoolContext::new(pd_worker.scheduler())
    });
    let store = SyncStorage::new(&Default::default(), read_pool);
    let mut ts_generator = 1..;

    let mut kvs = KvGenerator::new(100, 1000);

    for (k, v) in kvs.take(100000) {
        let mut ts = ts_generator.next().unwrap();
        store
            .prewrite(
                Context::new(),
                vec![Mutation::Put((Key::from_raw(&k), v))],
                k.clone(),
                ts,
            )
            .expect("");
        store
            .commit(
                Context::new(),
                vec![Key::from_raw(&k)],
                ts,
                ts_generator.next().unwrap(),
            )
            .expect("");

        ts = ts_generator.next().unwrap();
        store
            .prewrite(
                Context::new(),
                vec![Mutation::Delete(Key::from_raw(&k))],
                k.clone(),
                ts,
            )
            .expect("");
        store
            .commit(
                Context::new(),
                vec![Key::from_raw(&k)],
                ts,
                ts_generator.next().unwrap(),
            )
            .expect("");
    }

    kvs = KvGenerator::new(100, 1000);
    b.iter(|| {
        let (k, _) = kvs.next().unwrap();
        assert!(
            store
                .scan(
                    Context::new(),
                    Key::from_raw(&k),
                    1,
                    false,
                    ts_generator.next().unwrap()
                )
                .unwrap()
                .is_empty()
        )
    })
}

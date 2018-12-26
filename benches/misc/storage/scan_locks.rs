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

use bench_util::*;
use test_storage::*;
use test_util::KvGenerator;
use tikv::storage::{Key, Mutation};

#[bench]
fn bench_scan_locks(b: &mut Bencher) {
    let store = SyncTestStorageBuilder::new().build().unwrap();

    let kvs = KvGenerator::new(100, 5);
    let mut ts_generator = 1..;
    for (k, v) in kvs.take(1000) {
        store
            .prewrite(
                Context::new(),
                vec![Mutation::Put((Key::from_raw(&k), v))],
                k.clone(),
                ts_generator.next().unwrap(),
            )
            .unwrap();
    }

    let ts = ts_generator.next().unwrap();
    b.iter(|| {
        store
            .scan_locks(new_no_cache_context(), ts, vec![], 500)
            .unwrap();
    });
}

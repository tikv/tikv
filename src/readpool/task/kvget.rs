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

use futures::Future;

use storage;
use kvproto::kvrpcpb;

use super::*;

pub struct KvGetTask {
    request_context: kvrpcpb::Context,
    key: Vec<u8>,
    start_ts: u64,
}

impl Task for KvGetTask {
    fn build(&mut self, context: &WorkerThreadContext) -> BoxedFuture {
        let key = storage::Key::from_raw(self.key.as_slice());
        let start_ts = self.start_ts;
        let isolation_level = self.request_context.get_isolation_level();
        let not_fill_cache = self.request_context.get_not_fill_cache();
        box context
            .engine
            .future_snapshot(&self.request_context)
            // map storage::engine::Error -> storage::Error
            .map_err(storage::Error::from)
            .and_then(move |snapshot: Box<storage::Snapshot>| {
                let mut statistics = storage::Statistics::default();
                let snap_store = storage::SnapshotStore::new(
                    snapshot,
                    start_ts,
                    isolation_level,
                    not_fill_cache,
                );
                snap_store
                    .get(&key, &mut statistics)
                    // map storage::txn::Error -> storage::Error
                    .map_err(storage::Error::from)
            })
            // map storage::Error -> Error
            .map_err(Error::from)
            .map(Value::StorageValue)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;
    use futures::Future;

    use storage;
    use kvproto::kvrpcpb;

    use super::super::tests::*;

    #[test]
    fn test_get() {
        let storage_config = storage::Config::default();
        let mut storage = storage::Storage::new(&storage_config).unwrap();
        storage.start(&storage_config).unwrap();

        let (tx, rx) = channel();

        let task_context = WorkerThreadContext {
            engine: storage.get_engine(),
        };

        KvGetTask {
            request_context: kvrpcpb::Context::new(),
            key: b"x".to_vec(),
            start_ts: 100,
        }.build(&task_context)
            .then(expect_get_none(tx.clone(), 0))
            .wait()
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 0);

        storage
            .async_prewrite(
                kvrpcpb::Context::new(),
                vec![
                    storage::Mutation::Put((storage::make_key(b"x"), b"100".to_vec())),
                ],
                b"x".to_vec(),
                100,
                storage::Options::default(),
                {
                    let tx = tx.clone();
                    box move |_| tx.send(1).unwrap()
                },
            )
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 1);

        storage
            .async_commit(
                kvrpcpb::Context::new(),
                vec![storage::make_key(b"x")],
                100,
                101,
                {
                    let tx = tx.clone();
                    box move |_| tx.send(2).unwrap()
                },
            )
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 2);

        KvGetTask {
            request_context: kvrpcpb::Context::new(),
            key: b"x".to_vec(),
            start_ts: 100,
        }.build(&task_context)
            .then(expect_get_none(tx.clone(), 3))
            .wait()
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 3);

        KvGetTask {
            request_context: kvrpcpb::Context::new(),
            key: b"x".to_vec(),
            start_ts: 101,
        }.build(&task_context)
            .then(expect_get_val(tx.clone(), b"100".to_vec(), 4))
            .wait()
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 4);

        storage.stop().unwrap();
    }

    // TODO: Test Snapshot Error

    // TODO: Test snap_store.get Error

}

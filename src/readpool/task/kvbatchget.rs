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

pub struct KvBatchGetTask {
    request_context: kvrpcpb::Context,
    keys: Vec<Vec<u8>>,
    start_ts: u64,
}

impl KvBatchGetTask {
    pub fn new(
        request_context: kvrpcpb::Context,
        keys: Vec<Vec<u8>>,
        start_ts: u64,
    ) -> KvBatchGetTask {
        KvBatchGetTask {
            request_context,
            keys,
            start_ts,
        }
    }
}

impl Task for KvBatchGetTask {
    fn build(&mut self, context: &WorkerThreadContext) -> BoxedFuture {
        let keys: Vec<storage::Key> = self.keys
            .iter()
            .map(|key| storage::Key::from_raw(key.as_slice()))
            .collect();
        let start_ts = self.start_ts;
        let isolation_level = self.request_context.get_isolation_level();
        let not_fill_cache = self.request_context.get_not_fill_cache();
        box context
            .engine
            .future_snapshot(&self.request_context)
            // map storage::engine::Error -> storage::Error
            .map_err(storage::Error::from)
            .and_then(move |snapshot: Box<storage::Snapshot>| {
                let keys = keys;
                let mut statistics = storage::Statistics::default();
                let snap_store = storage::SnapshotStore::new(
                    snapshot,
                    start_ts,
                    isolation_level,
                    not_fill_cache,
                );
                snap_store
                    .batch_get(keys.as_slice(), &mut statistics)
                    .map(|results| results
                        .into_iter()
                        .zip(keys)
                        .filter(|&(ref v, ref _k)|
                            !(v.is_ok() && v.as_ref().unwrap().is_none())
                        )
                        .map(|(v, k)| match v {
                            Ok(Some(x)) => Ok((k.raw().unwrap(), x)),
                            Err(e) => Err(storage::Error::from(e)),
                            _ => unreachable!(),
                        })
                        .collect()
                    )
                    // map storage::txn::Error -> storage::Error
                    .map_err(storage::Error::from)
            })
            // map storage::Error -> Error
            .map_err(Error::from)
            .map(Value::StorageMultiKvpairs)
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
    fn test_batch_get() {
        let storage_config = storage::Config::default();
        let mut storage = storage::Storage::new(&storage_config).unwrap();
        storage.start(&storage_config).unwrap();

        let (tx, rx) = channel();

        let task_context = WorkerThreadContext {
            engine: storage.get_engine(),
        };

        // write
        storage
            .async_prewrite(
                kvrpcpb::Context::new(),
                vec![
                    storage::Mutation::Put((storage::make_key(b"a"), b"aa".to_vec())),
                    storage::Mutation::Put((storage::make_key(b"b"), b"bb".to_vec())),
                    storage::Mutation::Put((storage::make_key(b"c"), b"cc".to_vec())),
                ],
                b"a".to_vec(),
                1,
                storage::Options::default(),
                {
                    let tx = tx.clone();
                    box move |_| tx.send(0).unwrap()
                },
            )
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 0);

        storage
            .async_commit(
                kvrpcpb::Context::new(),
                vec![
                    storage::make_key(b"a"),
                    storage::make_key(b"b"),
                    storage::make_key(b"c"),
                ],
                1,
                2,
                {
                    let tx = tx.clone();
                    box move |_| tx.send(1).unwrap()
                },
            )
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 1);

        // found
        KvBatchGetTask::new(
            kvrpcpb::Context::new(),
            vec![b"c".to_vec(), b"x".to_vec(), b"a".to_vec(), b"b".to_vec()],
            5,
        ).build(&task_context)
            .then(expect_get_vals(
                tx.clone(),
                vec![
                    Some((b"c".to_vec(), b"cc".to_vec())),
                    Some((b"a".to_vec(), b"aa".to_vec())),
                    Some((b"b".to_vec(), b"bb".to_vec())),
                ],
                2,
            ))
            .wait()
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 2);
    }

}

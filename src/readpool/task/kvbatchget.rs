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
    pub request_context: kvrpcpb::Context,
    pub keys: Vec<Vec<u8>>,
    pub start_ts: u64,
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
                            v.is_ok() && v.as_ref().unwrap().is_none()
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

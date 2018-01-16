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

use kvproto::kvrpcpb;
use storage;

use super::*;
use super::util::*;

/// `KvBatchGet` Subtask 1: Get snapshot and build Subtask 2
#[derive(Debug)]
pub struct KvBatchGetSubTask {
    pub req_context: kvrpcpb::Context,
    pub keys: Vec<Vec<u8>>,
    pub start_ts: u64,
}

impl SnapshotSubTask for KvBatchGetSubTask {
    #[inline]
    fn new_next_subtask_builder(&mut self) -> Box<SnapshotNextSubTaskBuilder> {
        box KvBatchGetSubTaskSecondBuilder {
            options: Some(KvBatchGetSubTaskSecondOptions {
                isolation_level: self.req_context.get_isolation_level(),
                not_fill_cache: self.req_context.get_not_fill_cache(),
                keys: self.keys
                    .iter()
                    .map(|key| storage::Key::from_raw(key.as_slice()))
                    .collect(),
                start_ts: self.start_ts,
            }),
        }
    }
    #[inline]
    fn get_request_context(&self) -> &kvrpcpb::Context {
        &self.req_context
    }
}


/// `KvBatchGet` Subtask 2: Invoke `KvBatchGet`
#[derive(Debug)]
struct KvBatchGetSubTaskSecond {
    snapshot: Option<Box<storage::Snapshot>>,
    options: KvBatchGetSubTaskSecondOptions,
}

impl SubTask for KvBatchGetSubTaskSecond {
    fn async_work(
        mut self: Box<Self>,
        _context: &mut WorkerThreadContext,
        on_done: SubTaskCallback,
    ) {
        let mut statistics = storage::Statistics::default();
        let snap_store = storage::SnapshotStore::new(
            self.snapshot.take().unwrap(),
            self.options.start_ts,
            self.options.isolation_level,
            !self.options.not_fill_cache,
        );
        let res = snap_store.batch_get(self.options.keys.as_slice(), &mut statistics);
        on_done(SubTaskResult::Finish(match res {
            Ok(results) => {
                // TODO: Move this logic into storage
                let mut res = vec![];
                for (k, v) in self.options.keys.into_iter().zip(results) {
                    match v {
                        Ok(Some(x)) => res.push(Ok((k.raw().unwrap(), x))),
                        Ok(None) => {}
                        Err(e) => res.push(Err(storage::Error::from(e))),
                    }
                }
                Ok(Value::StorageMultiKvpairs(res))
            }
            Err(e) => Err(Error::Storage(storage::Error::from(e))),
        }));
        // TODO: handle statistics
    }
}

#[derive(Debug)]
struct KvBatchGetSubTaskSecondOptions {
    isolation_level: kvrpcpb::IsolationLevel,
    not_fill_cache: bool,
    keys: Vec<storage::Key>,
    start_ts: u64,
}

struct KvBatchGetSubTaskSecondBuilder {
    options: Option<KvBatchGetSubTaskSecondOptions>,
}

impl SnapshotNextSubTaskBuilder for KvBatchGetSubTaskSecondBuilder {
    fn build(mut self: Box<Self>, snapshot: Box<storage::Snapshot>) -> Box<SubTask> {
        box KvBatchGetSubTaskSecond {
            snapshot: Some(snapshot),
            options: self.options.take().unwrap(),
        }
    }
}

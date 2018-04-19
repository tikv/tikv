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

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use raft::Ready;
use rocksdb::rocksdb_options::WriteOptions;
use rocksdb::{WriteBatch, DB};

use raftstore::store::Msg;
use raftstore::store::peer_storage::InvokeContext;
use util::transport::{NotifyError, RetryableSendCh, Sender};
use util::worker::Runnable;

pub struct Task {
    pub kv_wb: WriteBatch,
    pub raft_wb: WriteBatch,
    pub persist: Vec<(Ready, InvokeContext)>,
    pub sync_log: bool,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "async persist, kv write batch size {}, raft write batch size {}",
            self.kv_wb.data_size(),
            self.raft_wb.data_size()
        )
    }
}

pub struct Runner<C: Sender<Msg>> {
    tag: String,
    kv_engine: Arc<DB>,
    raft_engine: Arc<DB>,
    store_ch: C,
    sync_log: bool,
}

impl<C: Sender<Msg>> Runner<C> {
    pub fn new(
        store_id: u64,
        kv_engine: Arc<DB>,
        raft_engine: Arc<DB>,
        store_ch: RetryableSendCh<Msg, C>,
        sync_log: bool,
    ) -> Runner<C> {
        let tag = format!("store {}", store_id);
        let store_ch = store_ch.into_inner();
        Runner {
            tag,
            kv_engine,
            raft_engine,
            store_ch,
            sync_log,
        }
    }

    fn handle_persist(&mut self, task: Task) {
        // apply_snapshot, peer_destroy will clear_meta, so we need write region state first.
        // otherwise, if program restart between two write, raft log will be removed,
        // but region state may not changed in disk.
        fail_point!("raft_before_save");
        if !task.kv_wb.is_empty() {
            // RegionLocalState, ApplyState
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            self.kv_engine
                .write_opt(task.kv_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save append state result: {:?}", self.tag, e);
                });
        }
        fail_point!("raft_between_save");
        if !task.raft_wb.is_empty() {
            // RaftLocalState, Raft Log Entry
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(self.sync_log || task.sync_log);
            self.raft_engine
                .write_opt(task.raft_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save raft append result: {:?}", self.tag, e);
                });
        }
        fail_point!("raft_after_save");

        let mut appended = Some(Msg::Persistence {
            append_res: task.persist,
        });
        while let Some(msg) = appended.take() {
            match self.store_ch.send(msg) {
                Ok(()) => (),
                Err(NotifyError::Full(msg)) => {
                    appended = Some(msg);
                    error!("fail to send Msg::Persistence, notify queue is full, retry...");
                }
                Err(NotifyError::Closed(_)) => {
                    warn!("store_ch is closed");
                    break;
                }
                Err(NotifyError::Io(e)) => {
                    panic!("fail to send Msg::Persistence: {:?}", e);
                }
            }
        }
    }
}

impl<C: Sender<Msg>> Runnable<Task> for Runner<C> {
    fn run(&mut self, task: Task) {
        self.handle_persist(task);
    }
}

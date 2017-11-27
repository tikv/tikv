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

use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::fmt::{self, Display, Formatter};

use rocksdb::{WriteBatch, DB};
use rocksdb::rocksdb_options::WriteOptions;
use util::worker::Runnable;
use util::transport::SendCh;
use raft::Ready;
use raftstore::store::peer_storage::InvokeContext;
use raftstore::store::Msg;

pub struct Task {
    pub kv_wb: WriteBatch,
    pub raft_wb: WriteBatch,
    pub ready_res: Vec<(Ready, InvokeContext)>,
    pub sync_log: bool,
}

pub struct TaskRes {
    pub ready_res: Vec<(Ready, InvokeContext)>,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "async append, kv write batch size {}, raft write batch size {}",
            self.kv_wb.data_size(),
            self.raft_wb.data_size()
        )
    }
}

pub struct Runner {
    tag: String,
    kv_engine: Arc<DB>,
    raft_engine: Arc<DB>,
    sender: Sender<TaskRes>,
    notifier: SendCh<Msg>,
    sync_log: bool,
}

impl Runner {
    pub fn new(
        tag: String,
        kv_engine: Arc<DB>,
        raft_engine: Arc<DB>,
        sender: Sender<TaskRes>,
        notifier: SendCh<Msg>,
        sync_log: bool,
    ) -> Runner {
        Runner {
            tag,
            kv_engine,
            raft_engine,
            sender,
            notifier,
            sync_log,
        }
    }

    fn handle_append(&mut self, task: Task) {
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

        self.sender
            .send(TaskRes {
                ready_res: task.ready_res,
            })
            .unwrap();
        self.notifier.send(Msg::AppendLogReady).unwrap();
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        self.handle_append(task);
    }
}

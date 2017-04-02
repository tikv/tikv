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

use rocksdb::{DB, WriteBatch};
use util::worker::Runnable;
use raft::Ready;
use raftstore::store::peer_storage::InvokeContext;

pub struct Task {
    pub wb: WriteBatch,
    pub ready: Ready,
    pub ctx: InvokeContext,
}

pub struct TaskRes {
    pub ready: Ready,
    pub ctx: InvokeContext,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "[region {}] async append", self.ctx.region_id)
    }
}

pub struct Runner {
    tag: String,
    db: Arc<DB>,
    notifier: Sender<TaskRes>,
}

impl Runner {
    pub fn new(tag: String, db: Arc<DB>, notifier: Sender<TaskRes>) -> Runner {
        Runner {
            tag: tag,
            db: db,
            notifier: notifier,
        }
    }

    fn handle_append(&mut self, task: Task) {
        if !task.wb.is_empty() {
            self.db.write(task.wb).unwrap_or_else(|e| {
                panic!("{} failed to save append result: {:?}", self.tag, e);
            });
        }

        self.notifier
            .send(TaskRes {
                ready: task.ready,
                ctx: task.ctx,
            })
            .unwrap();
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        self.handle_append(task);
    }
}

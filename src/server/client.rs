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

use std::fmt::{self, Formatter, Display};
use std::net::SocketAddr;

use kvproto::msgpb::Message;

use pd::Result as PdResult;
use raftstore::Error as RaftStoreError;
use raftstore::store::client::{Callback, Client};
use util::worker::{Runnable, Worker, Scheduler};
use util::client::{Client as SendClient, TikvClient};
use super::resolve::Task as ResolveTask;
use super::Result;

pub enum Task {
    Send {
        store_id: u64,
        msg: Message,
        cb: Callback,
    },
    ResolveResult {
        store_id: u64,
        addr: PdResult<SocketAddr>,
        msg: Message,
        cb: Callback,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Send { ref store_id, ref msg, .. } => {
                write!(f, "send msg {:?} to store {}", msg.get_msg_type(), store_id)
            }
            Task::ResolveResult { ref store_id, ref addr, .. } => {
                write!(f, "resolve addr {:?} for store id {}", addr, store_id)
            }
        }
    }
}

pub struct Runner {
    scheduler: Scheduler<Task>,
    resolve_scheduler: Scheduler<ResolveTask>,
}

impl Runner {
    pub fn new(scheduler: Scheduler<Task>, resolve_scheduler: Scheduler<ResolveTask>) -> Runner {
        Runner {
            scheduler: scheduler,
            resolve_scheduler: resolve_scheduler,
        }
    }

    fn send(&self, store_id: u64, msg: Message, cb: Callback) {
        // resolve address for the specified store id
        let scheduler = self.scheduler.clone();
        let resolve_cb = box move |r| {
            let task = Task::ResolveResult {
                store_id: store_id,
                addr: r,
                msg: msg,
                cb: cb,
            };
            if let Err(e) = scheduler.schedule(task) {
                error!("failed to schedule resolve result task, store_id {}, error {:?}",
                       store_id,
                       e);
            }
        };
        if let Err(e) = self.resolve_scheduler.schedule(ResolveTask::new(store_id, resolve_cb)) {
            error!("failed to schedule resolve store address task, error {:?}",
                   e);
            return;
        }
    }

    fn on_resolve_result(&self,
                         _store_id: u64,
                         addr: PdResult<SocketAddr>,
                         msg: Message,
                         cb: Callback) {
        match addr {
            Ok(address) => {
                let client = TikvClient::new(address);
                match client.send(&msg) {
                    Ok(resp) => cb.call_box((Ok(resp),)),
                    Err(e) => cb.call_box((Err(RaftStoreError::from(e)),)),
                }
            }
            Err(e) => cb.call_box((Err(RaftStoreError::from(e)),)),
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::Send { store_id, msg, cb } => self.send(store_id, msg, cb),
            Task::ResolveResult { store_id, addr, msg, cb } => {
                self.on_resolve_result(store_id, addr, msg, cb)
            }
        }
    }
}

pub struct TikvRpcWorker {
    worker: Worker<Task>,
}

impl TikvRpcWorker {
    pub fn new(resolve_scheduler: Scheduler<ResolveTask>) -> Result<TikvRpcWorker> {
        let mut r = TikvRpcWorker { worker: Worker::new("simple tikv client") };

        let runner = Runner::new(r.worker.scheduler(), resolve_scheduler);
        box_try!(r.worker.start(runner));
        Ok(r)
    }

    pub fn client(&self) -> SimpleClient {
        SimpleClient::new(self.worker.scheduler())
    }
}

impl Drop for TikvRpcWorker {
    fn drop(&mut self) {
        if let Some(Err(e)) = self.worker.stop().map(|h| h.join()) {
            error!("failed to stop tikv rpc worker thread: {:?}!!!", e)
        }
    }
}

#[derive(Clone)]
pub struct SimpleClient {
    scheduler: Scheduler<Task>,
}

impl SimpleClient {
    pub fn new(scheduler: Scheduler<Task>) -> SimpleClient {
        SimpleClient { scheduler: scheduler }
    }
}

impl Client for SimpleClient {
    fn send(&self, store_id: u64, msg: Message, cb: Callback) {
        let task = Task::Send {
            store_id: store_id,
            msg: msg,
            cb: cb,
        };
        if let Err(e) = self.scheduler.schedule(task) {
            error!("failed to schedule send message task for store id {}, error {:?}",
                   store_id,
                   e);
        }
    }
}

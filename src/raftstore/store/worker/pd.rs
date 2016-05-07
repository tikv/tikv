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

use std::sync::{Arc, RwLock};
use std::fmt::{self, Formatter, Display};

use kvproto::metapb;
use kvproto::raftpb;

use util::HandyRwLock;
use util::worker::Runnable;
use util::escape;
use pd::PdClient;

// Use an asynchronous thread to tell pd something.
pub enum Task {
    AskChangePeer {
        change_type: raftpb::ConfChangeType,
        region: metapb::Region,
        leader_store_id: u64,
    },
    AskSplit {
        region: metapb::Region,
        split_key: Vec<u8>,
        leader_store_id: u64,
    },
    Heartbeat {
        store: metapb::Store,
    },
}


impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::AskChangePeer { ref change_type, ref region, .. } => {
                write!(f, "ask {:?} for region {}", change_type, region.get_id())
            }
            Task::AskSplit { ref region, ref split_key, .. } => {
                write!(f,
                       "ask split region {} with key {}",
                       region.get_id(),
                       escape(&split_key))
            }
            Task::Heartbeat { ref store } => write!(f, "heartbeat for store {}", store.get_id()),
        }
    }
}

pub struct Runner<T: PdClient> {
    cluster_id: u64,
    pd_client: Arc<RwLock<T>>,
}

impl<T: PdClient> Runner<T> {
    pub fn new(cluster_id: u64, pd_client: Arc<RwLock<T>>) -> Runner<T> {
        Runner {
            cluster_id: cluster_id,
            pd_client: pd_client,
        }
    }
}

impl<T: PdClient> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        info!("executing task {}", task);

        let res = match task {
            Task::AskChangePeer { region, leader_store_id, .. } => {
                // TODO: We may add change_type in pd protocol later.
                self.pd_client.rl().ask_change_peer(self.cluster_id, region, leader_store_id)
            }
            Task::AskSplit { region, split_key, leader_store_id } => {
                self.pd_client.rl().ask_split(self.cluster_id, region, &split_key, leader_store_id)
            }
            Task::Heartbeat { store } => {
                // Now we use put store protocol for heartbeat.
                self.pd_client.wl().put_store(self.cluster_id, store)
            }
        };

        if let Err(e) = res {
            error!("executing pd command failed {:?}", e);
        }
    }
}

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
use raftstore::store::{SendCh, Msg};

use util::HandyRwLock;
use util::worker::Runnable;
use util::escape;
use pd::PdClient;

// Use an asynchronous thread to tell pd something.
pub enum Task {
    AskChangePeer {
        change_type: raftpb::ConfChangeType,
        region: metapb::Region,
        peer: metapb::Peer,
    },
    AskSplit {
        region: metapb::Region,
        split_key: Vec<u8>,
        peer: metapb::Peer,
    },
    Heartbeat {
        store: metapb::Store,
    },
    DeadPeerCheck {
        peer: metapb::Peer,
        region: metapb::Region,
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
            Task::DeadPeerCheck { ref peer, .. } => {
                write!(f, "dead peer check for {}", peer.get_id())
            }
        }
    }
}

pub struct Runner<T: PdClient> {
    ch: SendCh,
    pd_client: Arc<RwLock<T>>,
}

impl<T: PdClient> Runner<T> {
    pub fn new(ch: SendCh, pd_client: Arc<RwLock<T>>) -> Runner<T> {
        Runner {
            ch: ch,
            pd_client: pd_client,
        }
    }
}

impl<T: PdClient> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        info!("executing task {}", task);

        let res = match task {
            Task::AskChangePeer { region, peer, .. } => {
                // TODO: We may add change_type in pd protocol later.
                self.pd_client.rl().ask_change_peer(region, peer)
            }
            Task::AskSplit { region, split_key, peer } => {
                self.pd_client.rl().ask_split(region, &split_key, peer)
            }
            Task::Heartbeat { store } => {
                // Now we use put store protocol for heartbeat.
                self.pd_client.wl().put_store(store)
            }
            Task::DeadPeerCheck { peer, region } => {
                let result = self.pd_client.rl().get_region(region.get_start_key());
                if result.is_err() {
                    return;
                }
                let region_pd = result.unwrap();
                // if the region id of remote do not equal to local, it means region
                // has already splited and local peer don't know, this case it's also
                // a dead peer
                let mut exist = false;
                if region_pd.get_id() == region.get_id() {
                    for p in region_pd.get_peers() {
                        if *p == peer {
                            exist = true;
                            break;
                        }
                    }
                }
                if let Err(e) = self.ch
                    .send(Msg::DeadPeerCheckResult {
                        region_id: region.get_id(),
                        peer: peer.clone(),
                        exist: exist,
                    }) {
                        warn!("failed to send dead peer check result of {:?}: {}", peer, e)
                    }
                Ok(())
            }
        };

        if let Err(e) = res {
            error!("executing pd command failed {:?}", e);
        }
    }
}

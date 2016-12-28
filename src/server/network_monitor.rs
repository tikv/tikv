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

use super::{Result, Msg};
use util::worker::Runnable;
use util::transport::SendCh;

pub enum Event {
    RenewNetworkStat {
        store_id: u64,
        remote_store_ids: Vec<u64>,
    },
    Ping { from_store_id: u64 },
    Pong { from_store_id: u64 },
}

impl Display for Event {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        // TODO add messages
        match *self {
            Event::RenewNetworkStat { .. } => write!(f, ""),
            Event::Ping { .. } => write!(f, ""),
            Event::Pong { .. } => write!(f, ""),
        }
    }
}

pub struct Runner {
    ch: SendCh<Msg>,
}

impl Runner {
    pub fn new(ch: SendCh<Msg>) -> Runner {
        Runner { ch: ch }
    }
}

impl Runnable<Event> for Runner {
    fn run(&mut self, event: Event) {
        match event {
            Event::RenewNetworkStat { .. } => {
                // TODO add impl
            }
            Event::Ping { .. } => {
                // TODO add impl
            }
            Event::Pong { .. } => {
                // TODO add impl
            }
        }
    }
}

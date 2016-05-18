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

use kvproto::raft_serverpb::RaftMessage;
use tikv::raftstore::{Result, Error};
use tikv::raftstore::store::Transport;
use rand;
use std::sync::{Arc, RwLock};

use super::util::*;
use tikv::util::HandyRwLock;

pub trait Filter: Send + Sync {
    // in a SimulateTransport, if any filter's before return true, msg will be discard
    fn before(&self, msg: &RaftMessage) -> bool;
    // with after provided, one can change the return value arbitrarily
    fn after(&self, Result<()>) -> Result<()>;
}

struct FilterDropPacket {
    rate: u32,
    drop: RwLock<bool>,
}

struct FilterDelay {
    duration: u64,
}

impl Filter for FilterDropPacket {
    fn before(&self, _: &RaftMessage) -> bool {
        let drop = rand::random::<u32>() % 100u32 < self.rate;
        *self.drop.wl() = drop;
        drop
    }
    fn after(&self, x: Result<()>) -> Result<()> {
        if *self.drop.rl() {
            return Err(Error::Timeout("drop by FilterDropPacket in SimulateTransport".to_string()));
        }
        x
    }
}


impl Filter for FilterDelay {
    fn before(&self, _: &RaftMessage) -> bool {
        sleep_ms(self.duration);
        false
    }
    fn after(&self, x: Result<()>) -> Result<()> {
        x
    }
}

pub struct SimulateTransport<T: Transport> {
    filters: Vec<Box<Filter>>,
    trans: Arc<RwLock<T>>,
}

impl<T: Transport> SimulateTransport<T> {
    pub fn new(trans: Arc<RwLock<T>>) -> SimulateTransport<T> {
        SimulateTransport {
            filters: vec![],
            trans: trans,
        }
    }

    pub fn set_filters(&mut self, filters: Vec<Box<Filter>>) {
        self.filters = filters;
    }
}

impl<T: Transport> Transport for SimulateTransport<T> {
    fn send(&self, msg: RaftMessage) -> Result<()> {
        let mut discard = false;
        for filter in &self.filters {
            if filter.before(&msg) {
                discard = true;
            }
        }

        let mut res = Ok(());
        if !discard {
            res = self.trans.rl().send(msg);
        }

        for filter in self.filters.iter().rev() {
            res = filter.after(res);
        }

        res
    }
}

struct PartitionFilter {
    node_ids: Vec<u64>,
    drop: RwLock<bool>,
}

impl Filter for PartitionFilter {
    fn before(&self, msg: &RaftMessage) -> bool {
        let drop = self.node_ids.contains(&msg.get_to_peer().get_store_id());
        *self.drop.wl() = drop;
        drop
    }
    fn after(&self, r: Result<()>) -> Result<()> {
        if *self.drop.rl() {
            return Err(Error::Timeout("drop by PartitionPacket in SimulateTransport".to_string()));
        }
        r
    }
}

pub fn new_partition_filter(node_ids: Vec<u64>) -> Box<Filter> {
    let ids = node_ids.clone();
    Box::new(PartitionFilter {
        node_ids: ids,
        drop: RwLock::new(false),
    })
}

pub fn new_drop_packet_filter(rate: u32) -> Box<Filter> {
    Box::new(FilterDropPacket {
        rate: rate,
        drop: RwLock::new(false),
    })
}

pub fn new_delay_filter(duration: u64) -> Box<Filter> {
    Box::new(FilterDelay { duration: duration })
}

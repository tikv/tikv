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
use self::Strategy::*;

#[derive(Clone)]
pub enum Strategy {
    DropPacket(u32),
    Delay(u64),
}

pub trait Filter: Send + Sync {
    // in a SimulateTransport, if any filter's before return true, msg will be discard
    fn before(&mut self, msg: &RaftMessage) -> bool;
    // with after provided, one can change the return value arbitrarily
    fn after(&mut self, Result<()>) -> Result<()>;
}

struct FilterDropPacket {
    rate: u32,
    drop: bool,
}

struct FilterDelay {
    duration: u64,
}

impl Filter for FilterDropPacket {
    fn before(&mut self, _: &RaftMessage) -> bool {
        self.drop = rand::random::<u32>() % 100u32 < self.rate;
        self.drop
    }
    fn after(&mut self, x: Result<()>) -> Result<()> {
        if self.drop {
            return Err(Error::Timeout("drop by FilterDropPacket in SimulateTransport".to_string()));
        }
        x
    }
}

impl Filter for FilterDelay {
    fn before(&mut self, _: &RaftMessage) -> bool {
        sleep_ms(self.duration);
        false
    }
    fn after(&mut self, x: Result<()>) -> Result<()> {
        x
    }
}

pub struct SimulateTransport<T: Transport> {
    filters: Vec<RwLock<Box<Filter>>>,
    trans: Arc<RwLock<T>>,
}

impl<T: Transport> SimulateTransport<T> {
    pub fn new(strategy: Vec<Strategy>, trans: Arc<RwLock<T>>) -> SimulateTransport<T> {
        let mut filters: Vec<RwLock<Box<Filter>>> = vec![];
        for s in strategy {
            match s {
                DropPacket(rate) => {
                    filters.push(RwLock::new(box FilterDropPacket {
                        rate: rate,
                        drop: false,
                    }));
                }
                Delay(latency) => {
                    filters.push(RwLock::new(box FilterDelay { duration: latency }));
                }
            }
        }

        SimulateTransport {
            filters: filters,
            trans: trans,
        }
    }

    pub fn push(&mut self, filter: Box<Filter>) {
        self.filters.push(filter);
    }

    pub fn pop(&mut self) {
        self.filters.pop();
    }
}

impl<T: Transport> Transport for SimulateTransport<T> {
    fn send(&self, msg: RaftMessage) -> Result<()> {
        let mut discard = false;
        for strategy in &self.filters {
            if strategy.wl().before(&msg) {
                discard = true;
            }
        }

        let mut res = Ok(());
        if !discard {
            res = self.trans.rl().send(msg);
        }

        for strategy in self.filters.iter().rev() {
            res = strategy.wl().after(res);
        }

        res
    }
}

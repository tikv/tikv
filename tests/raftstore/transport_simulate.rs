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
use kvproto::raftpb::MessageType;
use tikv::raftstore::{Result, Error};
use tikv::raftstore::store::Transport;
use tikv::util::HandyRwLock;

use rand;
use std::sync::{Arc, RwLock};
use std::time;
use std::usize;
use std::thread;
use std::vec::Vec;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub trait Filter: Send + Sync {
    // in a SimulateTransport, if any filter's before return true, msg will be discard
    fn before(&self, msg: &RaftMessage) -> bool;
    // with after provided, one can change the return value arbitrarily
    fn after(&self, Result<()>) -> Result<()>;
}

struct FilterDropPacket {
    rate: u32,
    drop: AtomicBool,
}

struct FilterDelay {
    duration: time::Duration,
}

impl Filter for FilterDropPacket {
    fn before(&self, _: &RaftMessage) -> bool {
        let drop = rand::random::<u32>() % 100u32 < self.rate;
        self.drop.store(drop, Ordering::Relaxed);
        drop
    }

    fn after(&self, x: Result<()>) -> Result<()> {
        if self.drop.load(Ordering::Relaxed) {
            return Err(Error::Timeout("drop by FilterDropPacket in SimulateTransport".to_string()));
        }
        x
    }
}

impl Filter for FilterDelay {
    fn before(&self, _: &RaftMessage) -> bool {
        thread::sleep(self.duration);
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

    pub fn clear_filters(&mut self) {
        self.filters.clear();
    }

    pub fn add_filter(&mut self, filter: Box<Filter>) {
        self.filters.push(filter);
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

        let mut res = if !discard {
            self.trans.rl().send(msg)
        } else {
            Ok(())
        };

        for filter in self.filters.iter().rev() {
            res = filter.after(res);
        }

        res
    }
}

pub trait FilterFactory {
    fn generate(&self, node_id: u64) -> Vec<Box<Filter>>;
}

pub struct DropPacket {
    rate: u32,
}

impl DropPacket {
    pub fn new(rate: u32) -> DropPacket {
        DropPacket { rate: rate }
    }
}

impl FilterFactory for DropPacket {
    fn generate(&self, _: u64) -> Vec<Box<Filter>> {
        vec![box FilterDropPacket {
                 rate: self.rate,
                 drop: AtomicBool::new(false),
             }]
    }
}

pub struct Delay {
    duration: time::Duration,
}

impl Delay {
    pub fn new(duration: time::Duration) -> Delay {
        Delay { duration: duration }
    }
}

impl FilterFactory for Delay {
    fn generate(&self, _: u64) -> Vec<Box<Filter>> {
        vec![box FilterDelay { duration: self.duration }]
    }
}

struct PartitionFilter {
    node_ids: Vec<u64>,
    drop: AtomicBool,
}

impl Filter for PartitionFilter {
    fn before(&self, msg: &RaftMessage) -> bool {
        let drop = self.node_ids.contains(&msg.get_to_peer().get_store_id());
        self.drop.store(drop, Ordering::Relaxed);
        drop
    }
    fn after(&self, r: Result<()>) -> Result<()> {
        if self.drop.load(Ordering::Relaxed) {
            return Err(Error::Timeout("drop by PartitionPacket in SimulateTransport".to_string()));
        }
        r
    }
}

pub struct Partition {
    s1: Vec<u64>,
    s2: Vec<u64>,
}

impl Partition {
    pub fn new(s1: Vec<u64>, s2: Vec<u64>) -> Partition {
        Partition { s1: s1, s2: s2 }
    }
}

impl FilterFactory for Partition {
    fn generate(&self, node_id: u64) -> Vec<Box<Filter>> {
        if self.s1.contains(&node_id) {
            return vec![box PartitionFilter {
                            node_ids: self.s2.clone(),
                            drop: AtomicBool::new(false),
                        }];
        }
        return vec![box PartitionFilter {
                        node_ids: self.s1.clone(),
                        drop: AtomicBool::new(false),
                    }];
    }
}

pub struct Isolate {
    node_id: u64,
}

impl Isolate {
    pub fn new(node_id: u64) -> Isolate {
        Isolate { node_id: node_id }
    }
}

impl FilterFactory for Isolate {
    fn generate(&self, node_id: u64) -> Vec<Box<Filter>> {
        if node_id == self.node_id {
            return vec![box FilterDropPacket {
                            rate: 100,
                            drop: AtomicBool::new(false),
                        }];
        }
        vec![box PartitionFilter {
                 node_ids: vec![self.node_id],
                 drop: AtomicBool::new(false),
             }]
    }
}

#[derive(Clone, Copy)]
pub enum Direction {
    Recv,
    Send,
    Both,
}

impl Direction {
    pub fn is_recv(&self) -> bool {
        match *self {
            Direction::Recv | Direction::Both => true,
            Direction::Send => false,
        }
    }

    pub fn is_send(&self) -> bool {
        match *self {
            Direction::Send | Direction::Both => true,
            Direction::Recv => false,
        }
    }
}

/// Drop specified messages for the store with special region.
///
/// If `msg_type` is None, all message will be filtered.
pub struct FilterRegionPacket {
    region_id: u64,
    store_id: u64,
    drop: AtomicBool,
    direction: Direction,
    allow: AtomicUsize,
    msg_type: Option<MessageType>,
}

impl Filter for FilterRegionPacket {
    fn before(&self, m: &RaftMessage) -> bool {
        let region_id = m.get_region_id();
        let from_store_id = m.get_from_peer().get_store_id();
        let to_store_id = m.get_to_peer().get_store_id();

        let mut drop =
            self.region_id == region_id &&
            (self.direction.is_send() && self.store_id == from_store_id ||
             self.direction.is_recv() && self.store_id == to_store_id) &&
            self.msg_type.as_ref().map_or(true, |t| t == &m.get_message().get_msg_type());
        if drop && self.allow.load(Ordering::SeqCst) > 0 {
            drop = false;
            self.allow.fetch_sub(1, Ordering::SeqCst);
        }
        self.drop.store(drop, Ordering::Relaxed);
        drop
    }

    fn after(&self, r: Result<()>) -> Result<()> {
        if self.drop.load(Ordering::Relaxed) {
            return Err(Error::Timeout("drop by FilterRegionPacket in SimulateTransport"
                .to_string()));
        }
        r
    }
}

pub struct IsolateRegionStore {
    region_id: u64,
    store_id: u64,
    direction: Direction,
    allow: usize,
    msg_type: Option<MessageType>,
}

impl IsolateRegionStore {
    pub fn new(region_id: u64, store_id: u64) -> IsolateRegionStore {
        IsolateRegionStore {
            region_id: region_id,
            store_id: store_id,
            direction: Direction::Both,
            msg_type: None,
            allow: 0,
        }
    }

    pub fn direction(mut self, direction: Direction) -> IsolateRegionStore {
        self.direction = direction;
        self
    }

    pub fn msg_type(mut self, m_type: MessageType) -> IsolateRegionStore {
        self.msg_type = Some(m_type);
        self
    }

    pub fn allow(mut self, number: usize) -> IsolateRegionStore {
        self.allow = number;
        self
    }
}

impl FilterFactory for IsolateRegionStore {
    fn generate(&self, _: u64) -> Vec<Box<Filter>> {
        vec![box FilterRegionPacket {
                 region_id: self.region_id,
                 store_id: self.store_id,
                 direction: self.direction,
                 msg_type: self.msg_type.clone(),
                 allow: AtomicUsize::new(self.allow),
                 drop: AtomicBool::new(false),
             }]
    }
}

struct SnapshotFilter {
    drop: AtomicBool,
}

impl Filter for SnapshotFilter {
    fn before(&self, msg: &RaftMessage) -> bool {
        let drop = msg.get_message().get_msg_type() == MessageType::MsgSnapshot;
        self.drop.store(drop, Ordering::Relaxed);
        drop
    }

    fn after(&self, x: Result<()>) -> Result<()> {
        x
    }
}

pub struct DropSnapshot;

impl FilterFactory for DropSnapshot {
    fn generate(&self, _: u64) -> Vec<Box<Filter>> {
        vec![box SnapshotFilter { drop: AtomicBool::new(false) }]
    }
}

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
use kvproto::eraftpb::MessageType;
use tikv::raftstore::{Result, Error};
use tikv::raftstore::store::{Msg as StoreMsg, Transport};
use tikv::server::transport::*;
use tikv::raft::SnapshotStatus;
use tikv::util::HandyRwLock;

use rand;
use std::sync::{Arc, RwLock, Mutex};
use std::marker::PhantomData;
use std::time;
use std::usize;
use std::thread;
use std::vec::Vec;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub trait Channel<M>: Send + Clone {
    fn send(&self, m: M) -> Result<()>;
}

impl Channel<RaftMessage> for ServerTransport {
    fn send(&self, m: RaftMessage) -> Result<()> {
        Transport::send(self, m)
    }
}

impl Channel<StoreMsg> for ServerRaftStoreRouter {
    fn send(&self, m: StoreMsg) -> Result<()> {
        RaftStoreRouter::try_send(self, m)
    }
}

pub trait Filter<M>: Send + Sync {
    // in a SimulateTransport, if any filter's before return true, msg will be discard
    fn before(&self, _: &mut Vec<M>);
    // with after provided, one can change the return value arbitrarily
    fn after(&self, res: Result<()>) -> Result<()> {
        res
    }
}

pub type SendFilter = Box<Filter<RaftMessage>>;
pub type RecvFilter = Box<Filter<StoreMsg>>;

#[derive(Clone)]
pub struct DropPacketFilter {
    rate: u32,
}

impl DropPacketFilter {
    pub fn new(rate: u32) -> DropPacketFilter {
        DropPacketFilter { rate: rate }
    }
}

impl<M> Filter<M> for DropPacketFilter {
    fn before(&self, msgs: &mut Vec<M>) {
        msgs.retain(|_| rand::random::<u32>() % 100u32 >= self.rate)
    }
}

#[derive(Clone)]
pub struct DelayFilter {
    duration: time::Duration,
}

impl DelayFilter {
    pub fn new(duration: time::Duration) -> DelayFilter {
        DelayFilter { duration: duration }
    }
}

impl<M> Filter<M> for DelayFilter {
    fn before(&self, _: &mut Vec<M>) {
        thread::sleep(self.duration);
    }
}

pub struct SimulateTransport<M, C: Channel<M>> {
    filters: Arc<RwLock<Vec<Box<Filter<M>>>>>,
    ch: C,
}

impl<M, C: Channel<M>> SimulateTransport<M, C> {
    pub fn new(ch: C) -> SimulateTransport<M, C> {
        SimulateTransport {
            filters: Arc::new(RwLock::new(vec![])),
            ch: ch,
        }
    }

    pub fn clear_filters(&mut self) {
        self.filters.wl().clear();
    }

    pub fn add_filter(&mut self, filter: Box<Filter<M>>) {
        self.filters.wl().push(filter);
    }
}

impl<M, C: Channel<M>> Channel<M> for SimulateTransport<M, C> {
    fn send(&self, msg: M) -> Result<()> {
        let mut taken = 0;
        let mut msgs = vec![msg];
        let filters = self.filters.rl();
        for filter in filters.iter() {
            filter.before(&mut msgs);
            taken += 1;
            if msgs.is_empty() {
                break;
            }
        }
        let mut res = Ok(());
        if msgs.is_empty() {
            res = Err(Error::Timeout("drop by in SimulateTransport".to_owned()))
        } else {
            for msg in msgs {
                res = self.ch.send(msg);
                if res.is_err() {
                    break;
                }
            }
        }
        for filter in filters[..taken].iter().rev() {
            res = filter.after(res);
        }
        res
    }
}

impl<M, C: Channel<M>> Clone for SimulateTransport<M, C> {
    fn clone(&self) -> SimulateTransport<M, C> {
        SimulateTransport {
            filters: self.filters.clone(),
            ch: self.ch.clone(),
        }
    }
}

impl<C: Channel<RaftMessage>> Transport for SimulateTransport<RaftMessage, C> {
    fn send(&self, m: RaftMessage) -> Result<()> {
        Channel::send(self, m)
    }
}

impl<C: Channel<StoreMsg>> RaftStoreRouter for SimulateTransport<StoreMsg, C> {
    fn send(&self, m: StoreMsg) -> Result<()> {
        Channel::send(self, m)
    }

    fn try_send(&self, m: StoreMsg) -> Result<()> {
        Channel::send(self, m)
    }
}

pub trait FilterFactory {
    fn generate(&self, node_id: u64) -> Vec<SendFilter>;
}

#[derive(Default)]
pub struct DefaultFilterFactory<F: Filter<RaftMessage> + Default>(PhantomData<F>);

impl<F: Filter<RaftMessage> + Default + 'static> FilterFactory for DefaultFilterFactory<F> {
    fn generate(&self, _: u64) -> Vec<SendFilter> {
        vec![Box::new(F::default())]
    }
}

pub struct CloneFilterFactory<F: Filter<RaftMessage> + Clone>(pub F);

impl<F: Filter<RaftMessage> + Clone + 'static> FilterFactory for CloneFilterFactory<F> {
    fn generate(&self, _: u64) -> Vec<SendFilter> {
        vec![Box::new(self.0.clone())]
    }
}

struct PartitionFilter {
    node_ids: Vec<u64>,
}

impl Filter<RaftMessage> for PartitionFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) {
        msgs.retain(|m| !self.node_ids.contains(&m.get_to_peer().get_store_id()));
    }
}

pub struct PartitionFilterFactory {
    s1: Vec<u64>,
    s2: Vec<u64>,
}

impl PartitionFilterFactory {
    pub fn new(s1: Vec<u64>, s2: Vec<u64>) -> PartitionFilterFactory {
        PartitionFilterFactory { s1: s1, s2: s2 }
    }
}

impl FilterFactory for PartitionFilterFactory {
    fn generate(&self, node_id: u64) -> Vec<SendFilter> {
        if self.s1.contains(&node_id) {
            return vec![box PartitionFilter { node_ids: self.s2.clone() }];
        }
        return vec![box PartitionFilter { node_ids: self.s1.clone() }];
    }
}

pub struct IsolationFilterFactory {
    node_id: u64,
}

impl IsolationFilterFactory {
    pub fn new(node_id: u64) -> IsolationFilterFactory {
        IsolationFilterFactory { node_id: node_id }
    }
}

impl FilterFactory for IsolationFilterFactory {
    fn generate(&self, node_id: u64) -> Vec<SendFilter> {
        if node_id == self.node_id {
            return vec![box DropPacketFilter { rate: 100 }];
        }
        vec![box PartitionFilter { node_ids: vec![self.node_id] }]
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
#[derive(Clone)]
pub struct RegionPacketFilter {
    region_id: u64,
    store_id: u64,
    direction: Direction,
    allow: Arc<AtomicUsize>,
    msg_type: Option<MessageType>,
}

impl Filter<RaftMessage> for RegionPacketFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) {
        msgs.retain(|m| {
            let region_id = m.get_region_id();
            let from_store_id = m.get_from_peer().get_store_id();
            let to_store_id = m.get_to_peer().get_store_id();

            if self.region_id == region_id &&
               (self.direction.is_send() && self.store_id == from_store_id ||
                self.direction.is_recv() && self.store_id == to_store_id) &&
               self.msg_type.as_ref().map_or(true, |t| t == &m.get_message().get_msg_type()) {
                if self.allow.load(Ordering::Relaxed) > 0 {
                    self.allow.fetch_sub(1, Ordering::Relaxed);
                    return true;
                }
                return false;
            }
            true
        });
    }
}

impl RegionPacketFilter {
    pub fn new(region_id: u64, store_id: u64) -> RegionPacketFilter {
        RegionPacketFilter {
            region_id: region_id,
            store_id: store_id,
            direction: Direction::Both,
            msg_type: None,
            allow: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn direction(mut self, direction: Direction) -> RegionPacketFilter {
        self.direction = direction;
        self
    }

    pub fn msg_type(mut self, m_type: MessageType) -> RegionPacketFilter {
        self.msg_type = Some(m_type);
        self
    }

    pub fn allow(mut self, number: usize) -> RegionPacketFilter {
        self.allow = Arc::new(AtomicUsize::new(number));
        self
    }
}

#[derive(Default)]
pub struct SnapshotFilter {
    drop: AtomicBool,
}

impl Filter<RaftMessage> for SnapshotFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) {
        msgs.retain(|m| m.get_message().get_msg_type() != MessageType::MsgSnapshot);
        self.drop.store(msgs.is_empty(), Ordering::Relaxed);
    }

    fn after(&self, x: Result<()>) -> Result<()> {
        if self.drop.load(Ordering::Relaxed) {
            Ok(())
        } else {
            x
        }
    }
}

/// Pause Snap
pub struct PauseFirstSnapshotFilter {
    dropped: AtomicBool,
    stale: AtomicBool,
    pending_msg: Mutex<Vec<StoreMsg>>,
}

impl PauseFirstSnapshotFilter {
    pub fn new() -> PauseFirstSnapshotFilter {
        PauseFirstSnapshotFilter {
            dropped: AtomicBool::new(false),
            stale: AtomicBool::new(false),
            pending_msg: Mutex::new(vec![]),
        }
    }
}

impl Filter<StoreMsg> for PauseFirstSnapshotFilter {
    fn before(&self, msgs: &mut Vec<StoreMsg>) {
        if self.stale.load(Ordering::Relaxed) {
            return;
        }
        let mut to_send = vec![];
        let mut pending_msg = self.pending_msg.lock().unwrap();
        for m in msgs.drain(..) {
            let paused = match m {
                StoreMsg::ReportSnapshot { ref status, .. } => *status == SnapshotStatus::Finish,
                StoreMsg::RaftMessage(ref msg) => {
                    msg.get_message().get_msg_type() == MessageType::MsgSnapshot
                }
                _ => false,
            };
            if paused {
                self.dropped.compare_and_swap(false, true, Ordering::Relaxed);
                pending_msg.push(m);
            } else {
                to_send.push(m);
            }
        }
        if pending_msg.len() > 1 {
            self.dropped.compare_and_swap(true, false, Ordering::Relaxed);
            msgs.extend(pending_msg.drain(..));
            self.stale.compare_and_swap(false, true, Ordering::Relaxed);
        }
        msgs.extend(to_send);
    }

    fn after(&self, res: Result<()>) -> Result<()> {
        if res.is_err() && self.dropped.load(Ordering::Relaxed) {
            self.dropped.compare_and_swap(true, false, Ordering::Relaxed);
            Ok(())
        } else {
            res
        }
    }
}

/// Filter leading duplicated Snap.
///
/// It will pause the first snapshot and fiter out all the snapshot that
/// are same as first snapshot msg until the first different snapshot shows up.
pub struct LeadingDuplicatedSnapshotFilter {
    dropped: AtomicBool,
    stale: Arc<AtomicBool>,
    last_msg: Mutex<Option<RaftMessage>>,
}

impl LeadingDuplicatedSnapshotFilter {
    pub fn new(stale: Arc<AtomicBool>) -> LeadingDuplicatedSnapshotFilter {
        LeadingDuplicatedSnapshotFilter {
            dropped: AtomicBool::new(false),
            stale: stale,
            last_msg: Mutex::new(None),
        }
    }
}

impl Filter<StoreMsg> for LeadingDuplicatedSnapshotFilter {
    fn before(&self, msgs: &mut Vec<StoreMsg>) {
        let mut last_msg = self.last_msg.lock().unwrap();
        let mut stale = self.stale.load(Ordering::Relaxed);
        if stale {
            if last_msg.is_some() {
                msgs.push(StoreMsg::RaftMessage(last_msg.take().unwrap()));
            }
            return;
        }
        let mut to_send = vec![];
        for m in msgs.drain(..) {
            match m {
                StoreMsg::RaftMessage(msg) => {
                    if msg.get_message().get_msg_type() == MessageType::MsgSnapshot && !stale {
                        if last_msg.as_ref().map_or(false, |l| l != &msg) {
                            to_send.push(StoreMsg::RaftMessage(last_msg.take().unwrap()));
                            *last_msg = Some(msg);
                            stale = true;
                        } else {
                            self.dropped.store(true, Ordering::Relaxed);
                            *last_msg = Some(msg);
                        }
                    } else {
                        to_send.push(StoreMsg::RaftMessage(msg));
                    }
                }
                _ => {
                    to_send.push(m);
                }
            }
        }
        self.stale.store(stale, Ordering::Relaxed);
        msgs.extend(to_send);
    }

    fn after(&self, res: Result<()>) -> Result<()> {
        let dropped = self.dropped.compare_and_swap(true, false, Ordering::Relaxed);
        if res.is_err() && dropped { Ok(()) } else { res }
    }
}

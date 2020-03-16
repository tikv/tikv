// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::atomic::*;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::{mem, thread, time, usize};

use rand;

use engine_rocks::RocksEngine;
use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::RaftMessage;
use raft::eraftpb::MessageType;

use raftstore::router::RaftStoreRouter;
use raftstore::store::{Callback, CasualMessage, SignificantMsg, Transport};
use raftstore::{DiscardReason, Error, Result};
use tikv_util::collections::{HashMap, HashSet};
use tikv_util::{Either, HandyRwLock};

pub fn check_messages(msgs: &[RaftMessage]) -> Result<()> {
    if msgs.is_empty() {
        Err(Error::Transport(DiscardReason::Filtered))
    } else {
        Ok(())
    }
}

pub trait Filter: Send + Sync {
    /// `before` is run before sending the messages.
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()>;
    /// `after` is run after sending the messages,
    /// so that the returned value could be changed if necessary.
    fn after(&self, res: Result<()>) -> Result<()> {
        res
    }
}

/// Emits a notification for each given message type that it sees.
#[allow(dead_code)]
pub struct MessageTypeNotifier {
    message_type: MessageType,
    notifier: Mutex<Sender<()>>,
    pending_notify: AtomicUsize,
    ready_notify: Arc<AtomicBool>,
}

impl MessageTypeNotifier {
    pub fn new(
        message_type: MessageType,
        notifier: Sender<()>,
        ready_notify: Arc<AtomicBool>,
    ) -> Self {
        Self {
            message_type,
            notifier: Mutex::new(notifier),
            ready_notify,
            pending_notify: AtomicUsize::new(0),
        }
    }
}

impl Filter for MessageTypeNotifier {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        for msg in msgs.iter() {
            if msg.get_message().get_msg_type() == self.message_type
                && self.ready_notify.load(Ordering::SeqCst)
            {
                self.pending_notify.fetch_add(1, Ordering::SeqCst);
            }
        }

        Ok(())
    }

    fn after(&self, _: Result<()>) -> Result<()> {
        while self.pending_notify.load(Ordering::SeqCst) > 0 {
            debug!("notify {:?}", self.message_type);
            self.pending_notify.fetch_sub(1, Ordering::SeqCst);
            let _ = self.notifier.lock().unwrap().send(());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct DropPacketFilter {
    rate: u32,
}

impl DropPacketFilter {
    pub fn new(rate: u32) -> DropPacketFilter {
        DropPacketFilter { rate }
    }
}

impl Filter for DropPacketFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        msgs.retain(|_| rand::random::<u32>() % 100u32 >= self.rate);
        check_messages(msgs)
    }
}

#[derive(Clone)]
pub struct DelayFilter {
    duration: time::Duration,
}

impl DelayFilter {
    pub fn new(duration: time::Duration) -> DelayFilter {
        DelayFilter { duration }
    }
}

impl Filter for DelayFilter {
    fn before(&self, _: &mut Vec<RaftMessage>) -> Result<()> {
        thread::sleep(self.duration);
        Ok(())
    }
}

#[derive(Clone)]
pub struct SimulateTransport<C> {
    filters: Arc<RwLock<Vec<Box<dyn Filter>>>>,
    ch: C,
}

impl<C> SimulateTransport<C> {
    pub fn new(ch: C) -> SimulateTransport<C> {
        SimulateTransport {
            filters: Arc::new(RwLock::new(vec![])),
            ch,
        }
    }

    pub fn clear_filters(&mut self) {
        self.filters.wl().clear();
    }

    pub fn add_filter(&mut self, filter: Box<dyn Filter>) {
        self.filters.wl().push(filter);
    }
}

fn filter_send<H>(
    filters: &Arc<RwLock<Vec<Box<dyn Filter>>>>,
    msg: RaftMessage,
    mut h: H,
) -> Result<()>
where
    H: FnMut(RaftMessage) -> Result<()>,
{
    let mut taken = 0;
    let mut msgs = vec![msg];
    let filters = filters.rl();
    let mut res = Ok(());
    for filter in filters.iter() {
        taken += 1;
        res = filter.before(&mut msgs);
        if res.is_err() {
            break;
        }
    }
    if res.is_ok() {
        for msg in msgs {
            res = h(msg);
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

impl<C: Transport> Transport for SimulateTransport<C> {
    fn send(&mut self, m: RaftMessage) -> Result<()> {
        let ch = &mut self.ch;
        filter_send(&self.filters, m, |m| ch.send(m))
    }

    fn flush(&mut self) {
        self.ch.flush();
    }
}

impl<C: RaftStoreRouter> RaftStoreRouter for SimulateTransport<C> {
    fn send_raft_msg(&self, msg: RaftMessage) -> Result<()> {
        filter_send(&self.filters, msg, |m| self.ch.send_raft_msg(m))
    }

    fn send_command(&self, req: RaftCmdRequest, cb: Callback<RocksEngine>) -> Result<()> {
        self.ch.send_command(req, cb)
    }

    fn casual_send(&self, region_id: u64, msg: CasualMessage<RocksEngine>) -> Result<()> {
        self.ch.casual_send(region_id, msg)
    }

    fn broadcast_unreachable(&self, store_id: u64) {
        self.ch.broadcast_unreachable(store_id)
    }

    fn significant_send(&self, region_id: u64, msg: SignificantMsg) -> Result<()> {
        self.ch.significant_send(region_id, msg)
    }
}

pub trait FilterFactory {
    fn generate(&self, node_id: u64) -> Vec<Box<dyn Filter>>;
}

#[derive(Default)]
pub struct DefaultFilterFactory<F: Filter + Default>(PhantomData<F>);

impl<F: Filter + Default + 'static> FilterFactory for DefaultFilterFactory<F> {
    fn generate(&self, _: u64) -> Vec<Box<dyn Filter>> {
        vec![Box::new(F::default())]
    }
}

pub struct CloneFilterFactory<F: Filter + Clone>(pub F);

impl<F: Filter + Clone + 'static> FilterFactory for CloneFilterFactory<F> {
    fn generate(&self, _: u64) -> Vec<Box<dyn Filter>> {
        vec![Box::new(self.0.clone())]
    }
}

struct PartitionFilter {
    node_ids: Vec<u64>,
}

impl Filter for PartitionFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        msgs.retain(|m| !self.node_ids.contains(&m.get_to_peer().get_store_id()));
        check_messages(msgs)
    }
}

pub struct PartitionFilterFactory {
    s1: Vec<u64>,
    s2: Vec<u64>,
}

impl PartitionFilterFactory {
    pub fn new(s1: Vec<u64>, s2: Vec<u64>) -> PartitionFilterFactory {
        PartitionFilterFactory { s1, s2 }
    }
}

impl FilterFactory for PartitionFilterFactory {
    fn generate(&self, node_id: u64) -> Vec<Box<dyn Filter>> {
        if self.s1.contains(&node_id) {
            return vec![Box::new(PartitionFilter {
                node_ids: self.s2.clone(),
            })];
        }
        return vec![Box::new(PartitionFilter {
            node_ids: self.s1.clone(),
        })];
    }
}

pub struct IsolationFilterFactory {
    node_id: u64,
}

impl IsolationFilterFactory {
    pub fn new(node_id: u64) -> IsolationFilterFactory {
        IsolationFilterFactory { node_id }
    }
}

impl FilterFactory for IsolationFilterFactory {
    fn generate(&self, node_id: u64) -> Vec<Box<dyn Filter>> {
        if node_id == self.node_id {
            return vec![Box::new(DropPacketFilter { rate: 100 })];
        }
        vec![Box::new(PartitionFilter {
            node_ids: vec![self.node_id],
        })]
    }
}

#[derive(Clone, Copy)]
pub enum Direction {
    Recv,
    Send,
    Both,
}

impl Direction {
    pub fn is_recv(self) -> bool {
        match self {
            Direction::Recv | Direction::Both => true,
            Direction::Send => false,
        }
    }

    pub fn is_send(self) -> bool {
        match self {
            Direction::Send | Direction::Both => true,
            Direction::Recv => false,
        }
    }
}

/// Drop specified messages for the store with special region.
///
/// If `drop_type` is empty, all message will be dropped.
#[derive(Clone)]
pub struct RegionPacketFilter {
    region_id: u64,
    store_id: u64,
    direction: Direction,
    block: Either<Arc<AtomicUsize>, Arc<AtomicBool>>,
    drop_type: Vec<MessageType>,
    skip_type: Vec<MessageType>,
    dropped_messages: Option<Arc<Mutex<Vec<RaftMessage>>>>,
    msg_callback: Option<Arc<dyn Fn(&RaftMessage) + Send + Sync>>,
}

impl Filter for RegionPacketFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let retain = |m: &RaftMessage| {
            let region_id = m.get_region_id();
            let from_store_id = m.get_from_peer().get_store_id();
            let to_store_id = m.get_to_peer().get_store_id();
            let msg_type = m.get_message().get_msg_type();

            if self.region_id == region_id
                && (self.direction.is_send() && self.store_id == from_store_id
                    || self.direction.is_recv() && self.store_id == to_store_id)
                && (self.drop_type.is_empty() || self.drop_type.contains(&msg_type))
                && !self.skip_type.contains(&msg_type)
            {
                if let Some(f) = self.msg_callback.as_ref() {
                    f(m)
                }
                return match self.block {
                    Either::Left(ref count) => loop {
                        let left = count.load(Ordering::SeqCst);
                        if left == 0 {
                            return false;
                        }
                        if count.compare_and_swap(left, left - 1, Ordering::SeqCst) == left {
                            return true;
                        }
                    },
                    Either::Right(ref block) => !block.load(Ordering::SeqCst),
                };
            }
            true
        };
        let origin_msgs = mem::replace(msgs, Vec::default());
        let (retained, dropped) = origin_msgs.into_iter().partition(retain);
        *msgs = retained;
        if let Some(dropped_messages) = self.dropped_messages.as_ref() {
            dropped_messages.lock().unwrap().extend_from_slice(&dropped);
        }
        check_messages(msgs)
    }
}

impl RegionPacketFilter {
    pub fn new(region_id: u64, store_id: u64) -> RegionPacketFilter {
        RegionPacketFilter {
            region_id,
            store_id,
            direction: Direction::Both,
            drop_type: vec![],
            skip_type: vec![],
            block: Either::Right(Arc::new(AtomicBool::new(true))),
            dropped_messages: None,
            msg_callback: None,
        }
    }

    pub fn direction(mut self, direction: Direction) -> RegionPacketFilter {
        self.direction = direction;
        self
    }

    // TODO: rename it to `drop`.
    pub fn msg_type(mut self, m_type: MessageType) -> RegionPacketFilter {
        self.drop_type.push(m_type);
        self
    }

    pub fn skip(mut self, m_type: MessageType) -> RegionPacketFilter {
        self.skip_type.push(m_type);
        self
    }

    pub fn allow(mut self, number: usize) -> RegionPacketFilter {
        self.block = Either::Left(Arc::new(AtomicUsize::new(number)));
        self
    }

    pub fn when(mut self, condition: Arc<AtomicBool>) -> RegionPacketFilter {
        self.block = Either::Right(condition);
        self
    }

    pub fn reserve_dropped(mut self, dropped: Arc<Mutex<Vec<RaftMessage>>>) -> RegionPacketFilter {
        self.dropped_messages = Some(dropped);
        self
    }

    pub fn set_msg_callback(
        mut self,
        cb: Arc<dyn Fn(&RaftMessage) + Send + Sync>,
    ) -> RegionPacketFilter {
        self.msg_callback = Some(cb);
        self
    }
}

#[derive(Default)]
pub struct SnapshotFilter {
    drop: AtomicBool,
}

impl Filter for SnapshotFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        msgs.retain(|m| m.get_message().get_msg_type() != MessageType::MsgSnapshot);
        self.drop.store(msgs.is_empty(), Ordering::Relaxed);
        check_messages(msgs)
    }

    fn after(&self, x: Result<()>) -> Result<()> {
        if self.drop.load(Ordering::Relaxed) {
            Ok(())
        } else {
            x
        }
    }
}

/// `CollectSnapshotFilter` is a simulation transport filter to simulate the simultaneous delivery
/// of multiple snapshots from different peers. It collects the snapshots from different
/// peers and drop the subsequent snapshots from the same peers. Currently, if there are
/// more than 1 snapshots in this filter, all the snapshots will be dilivered at once.
pub struct CollectSnapshotFilter {
    dropped: AtomicBool,
    stale: AtomicBool,
    pending_msg: Mutex<HashMap<u64, RaftMessage>>,
    pending_count_sender: Mutex<Sender<usize>>,
}

impl CollectSnapshotFilter {
    pub fn new(sender: Sender<usize>) -> CollectSnapshotFilter {
        CollectSnapshotFilter {
            dropped: AtomicBool::new(false),
            stale: AtomicBool::new(false),
            pending_msg: Mutex::new(HashMap::default()),
            pending_count_sender: Mutex::new(sender),
        }
    }
}

impl Filter for CollectSnapshotFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        if self.stale.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut to_send = vec![];
        let mut pending_msg = self.pending_msg.lock().unwrap();
        for msg in msgs.drain(..) {
            let (is_pending, from_peer_id) = {
                if msg.get_message().get_msg_type() == MessageType::MsgSnapshot {
                    let from_peer_id = msg.get_from_peer().get_id();
                    if pending_msg.contains_key(&from_peer_id) {
                        // Drop this snapshot message directly since it's from a seen peer
                        continue;
                    } else {
                        // Pile the snapshot from unseen peer
                        (true, from_peer_id)
                    }
                } else {
                    (false, 0)
                }
            };
            if is_pending {
                self.dropped
                    .compare_and_swap(false, true, Ordering::Relaxed);
                pending_msg.insert(from_peer_id, msg);
                let sender = self.pending_count_sender.lock().unwrap();
                sender.send(pending_msg.len()).unwrap();
            } else {
                to_send.push(msg);
            }
        }
        // Deliver those pending snapshots if there are more than 1.
        if pending_msg.len() > 1 {
            self.dropped
                .compare_and_swap(true, false, Ordering::Relaxed);
            msgs.extend(pending_msg.drain().map(|(_, v)| v));
            self.stale.compare_and_swap(false, true, Ordering::Relaxed);
        }
        msgs.extend(to_send);
        check_messages(msgs)
    }

    fn after(&self, res: Result<()>) -> Result<()> {
        if res.is_err() && self.dropped.load(Ordering::Relaxed) {
            self.dropped
                .compare_and_swap(true, false, Ordering::Relaxed);
            Ok(())
        } else {
            res
        }
    }
}

pub struct DropSnapshotFilter {
    notifier: Mutex<Sender<u64>>,
}

impl DropSnapshotFilter {
    pub fn new(ch: Sender<u64>) -> DropSnapshotFilter {
        DropSnapshotFilter {
            notifier: Mutex::new(ch),
        }
    }
}

impl Filter for DropSnapshotFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let notifier = self.notifier.lock().unwrap();
        msgs.retain(|msg| {
            if msg.get_message().get_msg_type() != MessageType::MsgSnapshot {
                true
            } else {
                let idx = msg.get_message().get_snapshot().get_metadata().get_index();
                if let Err(e) = notifier.send(idx) {
                    error!("failed to notify snapshot {:?}: {:?}", msg, e);
                }
                false
            }
        });
        Ok(())
    }
}

/// Capture the first snapshot message.
pub struct RecvSnapshotFilter {
    pub notifier: Mutex<Option<Sender<RaftMessage>>>,
    pub region_id: u64,
}

impl Filter for RecvSnapshotFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        for msg in msgs {
            if msg.get_message().get_msg_type() == MessageType::MsgSnapshot
                && msg.get_region_id() == self.region_id
            {
                let tx = self.notifier.lock().unwrap().take().unwrap();
                tx.send(msg.clone()).unwrap();
            }
        }
        Ok(())
    }
}

/// Filters all `filter_type` packets until seeing the `flush_type`.
///
/// The first filtered message will be flushed too.
pub struct LeadingFilter {
    filter_type: MessageType,
    flush_type: MessageType,
    first_filtered_msg: Mutex<Option<RaftMessage>>,
}

impl LeadingFilter {
    pub fn new(filter_type: MessageType, flush_type: MessageType) -> LeadingFilter {
        LeadingFilter {
            filter_type,
            flush_type,
            first_filtered_msg: Mutex::default(),
        }
    }
}

impl Filter for LeadingFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let mut filtered_msg = self.first_filtered_msg.lock().unwrap();
        let mut to_send = vec![];
        for msg in msgs.drain(..) {
            if msg.get_message().get_msg_type() == self.filter_type {
                if filtered_msg.is_none() {
                    *filtered_msg = Some(msg);
                }
            } else if msg.get_message().get_msg_type() == self.flush_type {
                to_send.push(filtered_msg.take().unwrap());
                to_send.push(msg);
            } else {
                to_send.push(msg);
            }
        }
        msgs.extend(to_send);
        check_messages(msgs)
    }

    fn after(&self, _: Result<()>) -> Result<()> {
        Ok(())
    }
}

/// Filter leading duplicated Snap.
///
/// It will pause the first snapshot and filter out all the snapshot that
/// are same as first snapshot msg until the first different snapshot shows up.
pub struct LeadingDuplicatedSnapshotFilter {
    dropped: AtomicBool,
    stale: Arc<AtomicBool>,
    last_msg: Mutex<Option<RaftMessage>>,
    // whether the two different snapshots will send together
    together: bool,
}

impl LeadingDuplicatedSnapshotFilter {
    pub fn new(stale: Arc<AtomicBool>, together: bool) -> LeadingDuplicatedSnapshotFilter {
        LeadingDuplicatedSnapshotFilter {
            dropped: AtomicBool::new(false),
            stale,
            last_msg: Mutex::new(None),
            together,
        }
    }
}

impl Filter for LeadingDuplicatedSnapshotFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let mut last_msg = self.last_msg.lock().unwrap();
        let mut stale = self.stale.load(Ordering::Relaxed);
        if stale {
            if last_msg.is_some() {
                // To make sure the messages will not handled in one raftstore batch.
                thread::sleep(Duration::from_millis(100));
                msgs.push(last_msg.take().unwrap());
            }
            return check_messages(msgs);
        }
        let mut to_send = vec![];
        for msg in msgs.drain(..) {
            if msg.get_message().get_msg_type() == MessageType::MsgSnapshot && !stale {
                if last_msg.as_ref().map_or(false, |l| l != &msg) {
                    to_send.push(last_msg.take().unwrap());
                    if self.together {
                        to_send.push(msg);
                    } else {
                        *last_msg = Some(msg);
                    }
                    stale = true;
                } else {
                    self.dropped.store(true, Ordering::Relaxed);
                    *last_msg = Some(msg);
                }
            } else {
                to_send.push(msg);
            }
        }
        self.stale.store(stale, Ordering::Relaxed);
        msgs.extend(to_send);
        check_messages(msgs)
    }

    fn after(&self, res: Result<()>) -> Result<()> {
        let dropped = self
            .dropped
            .compare_and_swap(true, false, Ordering::Relaxed);
        if res.is_err() && dropped {
            Ok(())
        } else {
            res
        }
    }
}

/// `RandomLatencyFilter` is a transport filter to simulate randomized network latency.
/// Based on a randomized rate, `RandomLatencyFilter` will decide whether to delay
/// the sending of any message. It's could be used to simulate the message sending
/// in a network with random latency, where messages could be delayed, disordered or lost.
pub struct RandomLatencyFilter {
    delay_rate: u32,
    delayed_msgs: Mutex<Vec<RaftMessage>>,
}

impl RandomLatencyFilter {
    pub fn new(rate: u32) -> RandomLatencyFilter {
        RandomLatencyFilter {
            delay_rate: rate,
            delayed_msgs: Mutex::new(vec![]),
        }
    }

    fn will_delay(&self, _: &RaftMessage) -> bool {
        rand::random::<u32>() % 100u32 >= self.delay_rate
    }
}

impl Filter for RandomLatencyFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let mut to_send = vec![];
        let mut to_delay = vec![];
        let mut delayed_msgs = self.delayed_msgs.lock().unwrap();
        // check whether to send those messages which are delayed previouly
        // and check whether to send any newly incoming message if they are not delayed
        for m in delayed_msgs.drain(..).chain(msgs.drain(..)) {
            if self.will_delay(&m) {
                to_delay.push(m);
            } else {
                to_send.push(m);
            }
        }
        delayed_msgs.extend(to_delay);
        msgs.extend(to_send);
        Ok(())
    }
}

impl Clone for RandomLatencyFilter {
    fn clone(&self) -> RandomLatencyFilter {
        let delayed_msgs = self.delayed_msgs.lock().unwrap();
        RandomLatencyFilter {
            delay_rate: self.delay_rate,
            delayed_msgs: Mutex::new(delayed_msgs.clone()),
        }
    }
}

#[derive(Clone, Default)]
pub struct LeaseReadFilter {
    pub ctx: Arc<RwLock<HashSet<Vec<u8>>>>,
    pub take: bool,
}

impl Filter for LeaseReadFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let mut ctx = self.ctx.wl();
        for m in msgs {
            let msg = m.mut_message();
            if msg.get_msg_type() == MessageType::MsgHeartbeat && !msg.get_context().is_empty() {
                ctx.insert(msg.get_context().to_owned());
            }
            if self.take {
                msg.take_context();
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct DropMessageFilter {
    ty: MessageType,
}

impl DropMessageFilter {
    pub fn new(ty: MessageType) -> DropMessageFilter {
        DropMessageFilter { ty }
    }
}

impl Filter for DropMessageFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        msgs.retain(|m| m.get_message().get_msg_type() != self.ty);
        Ok(())
    }
}

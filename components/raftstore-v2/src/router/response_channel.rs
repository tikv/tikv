// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! Variants of channels for `Msg`.
//!  - `Read`: a channel for read only requests including `StatusRequest`,
//!    `GetRequest` and `SnapRequest`
//!  - `Write`: a channel for write only requests including `AdminRequest`
//!    `PutRequest`, `DeleteRequest` and `DeleteRangeRequest`.
//!
//! Prefer channel over callback because:
//! 1. channel can be reused, hence reduce allocations (not yet implemented).
//! 2. channel may not need dynamic dispatch.
//! 3. caller can use async fashion.
//! 4. there will be no callback leak.

use std::{
    cell::UnsafeCell,
    fmt::{self, Debug, Formatter},
    future::Future,
    mem,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use engine_traits::Snapshot;
use futures::task::AtomicWaker;
use kvproto::{kvrpcpb::ExtraOp as TxnExtraOp, raft_cmdpb::RaftCmdResponse};
use raftstore::store::{
    local_metrics::TimeTracker, msg::ErrorCallback, region_meta::RegionMeta, ReadCallback,
    RegionSnapshot, WriteCallback,
};
use smallvec::SmallVec;
use tikv_util::memory::HeapSize;
use tracker::TrackerToken;

/// A struct allows to watch and notify specific events.
///
/// There are two different events: state and payload. Obviously, state events
/// have no payload. At most 30 states can be defined. There can be only one
/// type of payload.
struct EventCore<Res> {
    /// Every event will have two bits.
    /// - 0b00 means the event is not fired and not subscribed.
    /// - 0b01 means the event is fired and not subscribed.
    /// - 0b10 means the event is not fired and subscribed.
    /// - 0b11 means the event is fired and subscribed.
    /// Event 0 and Event 31 is reserved as payload and cancel respectively.
    /// Other events should be defined within [1, 30].
    event: AtomicU64,
    res: UnsafeCell<Option<Res>>,
    // Waker can be changed, need to use `AtomicWaker` to guarantee no data race.
    waker: AtomicWaker,
}

unsafe impl<Res: Send> Send for EventCore<Res> {}

const PAYLOAD_EVENT: u64 = 0;
const CANCEL_EVENT: u64 = 31;

#[inline]
const fn subscribed_bit_of(event: u64) -> u64 {
    1 << (event * 2)
}

#[inline]
const fn fired_bit_of(event: u64) -> u64 {
    1 << (event * 2 + 1)
}

impl<Res> Default for EventCore<Res> {
    #[inline]
    fn default() -> Self {
        Self {
            event: AtomicU64::new(0),
            res: UnsafeCell::new(None),
            waker: AtomicWaker::new(),
        }
    }
}

impl<Res> EventCore<Res> {
    #[inline]
    fn notify_event(&self, event: u64) {
        let previous = self.event.fetch_or(fired_bit_of(event), Ordering::AcqRel);
        if previous & subscribed_bit_of(event) != 0 {
            self.waker.wake()
        }
    }

    /// Set the result.
    ///
    /// After this call, no events should be notified.
    #[inline]
    fn set_result(&self, result: Res) {
        unsafe {
            *self.res.get() = Some(result);
        }
        let previous = self.event.fetch_or(
            fired_bit_of(PAYLOAD_EVENT) | fired_bit_of(CANCEL_EVENT),
            Ordering::AcqRel,
        );
        if previous & subscribed_bit_of(PAYLOAD_EVENT) != 0 {
            self.waker.wake()
        }
    }

    /// Cancel all subscribers.
    ///
    /// After this call, no events should be notified and no result should be
    /// set.
    #[inline]
    fn cancel(&self) {
        let mut previous = self
            .event
            .fetch_or(fired_bit_of(CANCEL_EVENT), Ordering::AcqRel);
        let subscribed_bit = subscribed_bit_of(0);
        while previous != 0 {
            // Not notified yet.
            if previous & 0b11 == subscribed_bit {
                self.waker.wake();
                return;
            }
            previous >>= 2;
        }
    }
}

struct WaitEvent<'a, Res> {
    event: u64,
    core: &'a EventCore<Res>,
}

#[inline]
fn check_bit(e: u64, fired_bit: u64) -> Option<bool> {
    if e & fired_bit != 0 {
        return Some(true);
    }
    let cancel_bit = fired_bit_of(CANCEL_EVENT);
    if e & cancel_bit != 0 {
        return Some(false);
    }
    None
}

impl<'a, Res> Future for WaitEvent<'a, Res> {
    type Output = bool;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = &self.core.event;
        let mut e = event.load(Ordering::Relaxed);
        let fired_bit = fired_bit_of(self.event);
        if let Some(b) = check_bit(e, fired_bit) {
            return Poll::Ready(b);
        }
        self.core.waker.register(cx.waker());
        let subscribed_bit = subscribed_bit_of(self.event);
        loop {
            match event.compare_exchange_weak(
                e,
                e | subscribed_bit,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Poll::Pending,
                Err(v) => e = v,
            };
            if let Some(b) = check_bit(e, fired_bit) {
                return Poll::Ready(b);
            }
        }
    }
}

struct WaitResult<'a, Res> {
    core: &'a EventCore<Res>,
}

impl<'a, Res> Future for WaitResult<'a, Res> {
    type Output = Option<Res>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = &self.core.event;
        let fired_bit = fired_bit_of(PAYLOAD_EVENT);
        let mut e = event.load(Ordering::Relaxed);
        if check_bit(e, fired_bit).is_some() {
            unsafe {
                return Poll::Ready((*self.core.res.get()).take());
            }
        }
        let subscribed_bit = subscribed_bit_of(PAYLOAD_EVENT);
        self.core.waker.register(cx.waker());
        loop {
            match event.compare_exchange_weak(
                e,
                e | subscribed_bit,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Poll::Pending,
                Err(v) => e = v,
            };
            if check_bit(e, fired_bit).is_some() {
                unsafe {
                    return Poll::Ready((*self.core.res.get()).take());
                }
            }
        }
    }
}

/// A base subscriber that contains most common implementation of subscribers.
pub struct BaseSubscriber<Res> {
    core: Arc<EventCore<Res>>,
}

impl<Res> BaseSubscriber<Res> {
    /// Wait for the result.
    #[inline]
    pub async fn result(mut self) -> Option<Res> {
        WaitResult { core: &self.core }.await
    }
}

unsafe impl<Res: Send> Send for BaseSubscriber<Res> {}
unsafe impl<Res: Send> Sync for BaseSubscriber<Res> {}

/// A base channel that contains most common implementation of channels.
pub struct BaseChannel<Res> {
    core: Arc<EventCore<Res>>,
}

impl<Res> BaseChannel<Res> {
    /// Creates a pair of channel and subscriber.
    #[inline]
    pub fn pair() -> (Self, BaseSubscriber<Res>) {
        let core: Arc<EventCore<Res>> = Arc::default();
        (Self { core: core.clone() }, BaseSubscriber { core })
    }

    /// Sets the final result.
    #[inline]
    pub fn set_result(mut self, res: Res) {
        self.core.set_result(res);
    }
}

impl<Res> Drop for BaseChannel<Res> {
    #[inline]
    fn drop(&mut self) {
        self.core.cancel();
    }
}

unsafe impl<Res: Send> Send for BaseChannel<Res> {}
unsafe impl<Res: Send> Sync for BaseChannel<Res> {}

pub type CmdResSubscriber = BaseSubscriber<RaftCmdResponse>;

impl CmdResSubscriber {
    pub async fn wait_proposed(&mut self) -> bool {
        WaitEvent {
            event: CmdResChannel::PROPOSED_EVENT,
            core: &self.core,
        }
        .await
    }

    pub async fn wait_committed(&mut self) -> bool {
        WaitEvent {
            event: CmdResChannel::COMMITTED_EVENT,
            core: &self.core,
        }
        .await
    }
}

pub type CmdResChannel = BaseChannel<RaftCmdResponse>;

impl Debug for CmdResChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "CmdResChannel")
    }
}

impl CmdResChannel {
    // Valid range is [1, 30]
    const PROPOSED_EVENT: u64 = 1;
    const COMMITTED_EVENT: u64 = 2;
}

impl ErrorCallback for CmdResChannel {
    fn report_error(self, err: RaftCmdResponse) {
        self.set_result(err);
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl WriteCallback for CmdResChannel {
    type Response = RaftCmdResponse;

    /// Called after a request is proposed to the raft group successfully. It's
    /// used to notify the caller to move on early because it's very likely the
    /// request will be applied to the raftstore.
    #[inline]
    fn notify_proposed(&mut self) {
        self.core.notify_event(Self::PROPOSED_EVENT);
    }

    /// Called after a request is committed and before it's being applied, and
    /// it's guaranteed that the request will be successfully applied soon.
    #[inline]
    fn notify_committed(&mut self) {
        self.core.notify_event(Self::COMMITTED_EVENT);
    }

    fn write_trackers(&self) -> Option<&SmallVec<[TimeTracker; 4]>> {
        None
    }

    fn write_trackers_mut(&mut self) -> Option<&mut SmallVec<[TimeTracker; 4]>> {
        None
    }

    // TODO: support executing hooks inside setting result.
    #[inline]
    fn set_result(mut self, res: RaftCmdResponse) {
        self.set_result(res);
    }
}

/// Response for Read.
///
/// Unlike v1, snapshot are always taken in LocalReader, hence snapshot doesn't
/// need to be a field of the struct.
#[derive(Clone, PartialEq, Debug)]
pub struct ReadResponse {
    pub txn_extra_op: TxnExtraOp,
}

/// Possible result of a raft query.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryResult {
    /// If it's a read like get or snapshot, `ReadResponse` is returned on
    /// success.
    Read(ReadResponse),
    /// If it's a status query, `RaftCmdResponse` is returned. If it's a read
    /// like query, `RaftCmdResponse` is returned on error.
    Response(RaftCmdResponse),
}

impl QueryResult {
    pub fn read(&self) -> Option<&ReadResponse> {
        match self {
            QueryResult::Read(r) => Some(r),
            _ => None,
        }
    }

    pub fn response(&self) -> Option<&RaftCmdResponse> {
        match self {
            QueryResult::Response(r) => Some(r),
            _ => None,
        }
    }
}

pub type QueryResChannel = BaseChannel<QueryResult>;

impl ErrorCallback for QueryResChannel {
    #[inline]
    fn report_error(self, err: RaftCmdResponse) {
        self.set_result(QueryResult::Response(err));
    }

    #[inline]
    fn is_none(&self) -> bool {
        false
    }
}

impl ReadCallback for QueryResChannel {
    type Response = QueryResult;

    #[inline]
    fn set_result(mut self, res: QueryResult) {
        self.set_result(res);
    }

    fn read_tracker(&self) -> Option<&TrackerToken> {
        None
    }
}

pub type QueryResSubscriber = BaseSubscriber<QueryResult>;

pub type DebugInfoChannel = BaseChannel<RegionMeta>;
pub type DebugInfoSubscriber = BaseSubscriber<RegionMeta>;

#[cfg(test)]
mod tests {
    use engine_test::kv::KvTestSnapshot;
    use futures::executor::block_on;

    use super::*;

    #[test]
    fn test_cancel() {
        let (mut chan, mut sub) = CmdResChannel::pair();
        drop(chan);
        assert!(!block_on(sub.wait_proposed()));
        assert!(!block_on(sub.wait_committed()));
        assert!(block_on(sub.result()).is_none());

        let (mut chan, mut sub) = CmdResChannel::pair();
        chan.notify_proposed();
        let mut result = RaftCmdResponse::default();
        result.mut_header().set_current_term(4);
        chan.set_result(result.clone());
        assert!(block_on(sub.wait_proposed()));
        assert!(!block_on(sub.wait_committed()));
        assert_eq!(block_on(sub.result()), Some(result));

        let (mut chan, mut sub) = QueryResChannel::pair();
        drop(chan);
        assert!(block_on(sub.result()).is_none());
    }

    #[test]
    fn test_channel() {
        let (mut chan, mut sub) = CmdResChannel::pair();
        chan.notify_proposed();
        chan.notify_committed();
        let mut result = RaftCmdResponse::default();
        result.mut_header().set_current_term(2);
        chan.set_result(result.clone());
        assert!(block_on(sub.wait_proposed()));
        assert!(block_on(sub.wait_committed()));
        assert_eq!(block_on(sub.result()), Some(result.clone()));

        let (mut chan, mut sub) = QueryResChannel::pair();
        let resp = QueryResult::Response(result.clone());
        chan.set_result(resp.clone());
        assert_eq!(block_on(sub.result()).unwrap(), resp);

        let (mut chan, mut sub) = QueryResChannel::pair();
        let read = QueryResult::Read(ReadResponse {
            txn_extra_op: TxnExtraOp::ReadOldValue,
        });
        chan.set_result(read.clone());
        assert_eq!(block_on(sub.result()).unwrap(), read);
    }
}

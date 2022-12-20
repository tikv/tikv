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
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use futures::{task::AtomicWaker, FutureExt, Stream};
use kvproto::{kvrpcpb::ExtraOp as TxnExtraOp, raft_cmdpb::RaftCmdResponse};
use raftstore::store::{
    local_metrics::TimeTracker, msg::ErrorCallback, region_meta::RegionMeta, ReadCallback,
    WriteCallback,
};
use smallvec::SmallVec;
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
    /// Even a channel supports multiple events, it's not necessary to trigger
    /// all of them. `event_mask` is used to filter unnecessary events.
    event_mask: u32,
    res: UnsafeCell<Option<Res>>,
    before_set: UnsafeCell<Option<Box<dyn FnOnce(&mut Res) + Send>>>,
    // Waker can be changed, need to use `AtomicWaker` to guarantee no data race.
    waker: AtomicWaker,
}

unsafe impl<Res: Send> Send for EventCore<Res> {}

const PAYLOAD_EVENT: u64 = 0;
const CANCEL_EVENT: u64 = 31;

const fn event_mask_bit_of(event: u64) -> u32 {
    1 << event
}

#[inline]
const fn subscribed_bit_of(event: u64) -> u64 {
    1 << (event * 2)
}

#[inline]
const fn fired_bit_of(event: u64) -> u64 {
    1 << (event * 2 + 1)
}

impl<Res> EventCore<Res> {
    #[inline]
    fn notify_event(&self, event: u64) {
        if self.event_mask & event_mask_bit_of(event) != 0 {
            let previous = self.event.fetch_or(fired_bit_of(event), Ordering::AcqRel);
            if previous & subscribed_bit_of(event) != 0 {
                self.waker.wake()
            }
        }
    }

    /// Set the result.
    ///
    /// After this call, no events should be notified.
    #[inline]
    fn set_result(&self, mut result: Res) {
        unsafe {
            if let Some(cb) = (*self.before_set.get()).take() {
                cb(&mut result);
            }
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
    sub: &'a BaseSubscriber<Res>,
}

impl<'a, Res> Future for WaitResult<'a, Res> {
    type Output = Option<Res>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = &self.sub.core.event;
        let fired_bit = fired_bit_of(PAYLOAD_EVENT);
        let mut e = event.load(Ordering::Relaxed);
        if check_bit(e, fired_bit).is_some() {
            unsafe {
                return Poll::Ready((*self.sub.core.res.get()).take());
            }
        }
        let subscribed_bit = subscribed_bit_of(PAYLOAD_EVENT);
        self.sub.core.waker.register(cx.waker());
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
                    return Poll::Ready((*self.sub.core.res.get()).take());
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
    pub async fn result(self) -> Option<Res> {
        WaitResult { sub: &self }.await
    }

    /// Test if the result is ready without any polling.
    #[inline]
    pub fn has_result(&self) -> bool {
        let e = self.core.event.load(Ordering::Relaxed);
        check_bit(e, fired_bit_of(PAYLOAD_EVENT)).is_some()
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
        Self::with_mask(u32::MAX)
    }

    fn with_mask(mask: u32) -> (Self, BaseSubscriber<Res>) {
        let core: Arc<EventCore<Res>> = Arc::new(EventCore {
            event: AtomicU64::new(0),
            res: UnsafeCell::new(None),
            event_mask: mask,
            before_set: UnsafeCell::new(None),
            waker: AtomicWaker::new(),
        });
        (Self { core: core.clone() }, BaseSubscriber { core })
    }

    /// Sets the final result.
    #[inline]
    pub fn set_result(self, res: Res) {
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

#[derive(Clone, Copy, Debug)]
enum CmdResPollStage {
    ExpectProposed,
    ExpectCommitted,
    ExpectResult,
    Drained,
}

impl CmdResPollStage {
    #[inline]
    fn init(event_mask: u32) -> CmdResPollStage {
        if event_mask & event_mask_bit_of(CmdResChannel::PROPOSED_EVENT) != 0 {
            CmdResPollStage::ExpectProposed
        } else if event_mask & event_mask_bit_of(CmdResChannel::COMMITTED_EVENT) != 0 {
            CmdResPollStage::ExpectCommitted
        } else {
            CmdResPollStage::ExpectResult
        }
    }

    #[inline]
    fn next(&mut self, event_mask: u32) {
        *self = match self {
            CmdResPollStage::ExpectProposed => {
                if event_mask & event_mask_bit_of(CmdResChannel::COMMITTED_EVENT) == 0 {
                    CmdResPollStage::ExpectResult
                } else {
                    CmdResPollStage::ExpectCommitted
                }
            }
            CmdResPollStage::ExpectCommitted => CmdResPollStage::ExpectResult,
            CmdResPollStage::ExpectResult => CmdResPollStage::Drained,
            CmdResPollStage::Drained => CmdResPollStage::Drained,
        }
    }
}

#[derive(Debug)]
pub enum CmdResEvent {
    Proposed,
    Committed,
    Finished(RaftCmdResponse),
}

pub struct CmdResStream {
    sub: CmdResSubscriber,
    stage: CmdResPollStage,
}

impl CmdResStream {
    #[inline]
    pub fn new(sub: CmdResSubscriber) -> Self {
        Self {
            stage: CmdResPollStage::init(sub.core.event_mask),
            sub,
        }
    }
}

impl Stream for CmdResStream {
    type Item = CmdResEvent;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        loop {
            match stream.stage {
                CmdResPollStage::ExpectProposed => {
                    match (WaitEvent {
                        event: CmdResChannel::PROPOSED_EVENT,
                        core: &stream.sub.core,
                    })
                    .poll_unpin(cx)
                    {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(b) => {
                            stream.stage.next(stream.sub.core.event_mask);
                            if b {
                                return Poll::Ready(Some(CmdResEvent::Proposed));
                            }
                        }
                    }
                }
                CmdResPollStage::ExpectCommitted => {
                    match (WaitEvent {
                        event: CmdResChannel::COMMITTED_EVENT,
                        core: &stream.sub.core,
                    })
                    .poll_unpin(cx)
                    {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(b) => {
                            stream.stage.next(stream.sub.core.event_mask);
                            if b {
                                return Poll::Ready(Some(CmdResEvent::Committed));
                            }
                        }
                    }
                }
                CmdResPollStage::ExpectResult => {
                    match (WaitResult { sub: &stream.sub }).poll_unpin(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(res) => {
                            stream.stage.next(stream.sub.core.event_mask);
                            if let Some(res) = res {
                                return Poll::Ready(Some(CmdResEvent::Finished(res)));
                            }
                        }
                    }
                }
                CmdResPollStage::Drained => return Poll::Ready(None),
            }
        }
    }
}

pub type CmdResChannel = BaseChannel<RaftCmdResponse>;

impl Debug for CmdResChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "CmdResChannel")
    }
}

#[derive(Default)]
pub struct CmdResChannelBuilder {
    event_mask: u32,
    before_set: Option<Box<dyn FnOnce(&mut RaftCmdResponse) + Send>>,
}

impl CmdResChannelBuilder {
    #[inline]
    pub fn subscribe_proposed(&mut self) -> &mut Self {
        self.event_mask |= event_mask_bit_of(CmdResChannel::PROPOSED_EVENT);
        self
    }

    #[inline]
    pub fn subscribe_committed(&mut self) -> &mut Self {
        self.event_mask |= event_mask_bit_of(CmdResChannel::COMMITTED_EVENT);
        self
    }

    #[inline]
    pub fn before_set(
        &mut self,
        f: impl FnOnce(&mut RaftCmdResponse) + Send + 'static,
    ) -> &mut Self {
        self.before_set = Some(Box::new(f));
        self
    }

    #[inline]
    pub fn build(self) -> (CmdResChannel, CmdResSubscriber) {
        let (c, s) = CmdResChannel::with_mask(self.event_mask);
        if let Some(f) = self.before_set {
            unsafe {
                *c.core.before_set.get() = Some(f);
            }
        }
        (c, s)
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
    fn set_result(self, res: RaftCmdResponse) {
        self.set_result(res);
    }
}

/// Response for Read.
///
/// Unlike v1, snapshot are always taken in LocalReader, hence snapshot doesn't
/// need to be a field of the struct.
#[derive(Clone, PartialEq, Debug)]
pub struct ReadResponse {
    pub read_index: u64,
    pub txn_extra_op: TxnExtraOp,
}

impl ReadResponse {
    pub fn new(read_index: u64) -> Self {
        ReadResponse {
            read_index,
            txn_extra_op: TxnExtraOp::Noop,
        }
    }
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
    fn set_result(self, res: QueryResult) {
        self.set_result(res);
    }

    fn read_tracker(&self) -> Option<&TrackerToken> {
        None
    }
}

pub type QueryResSubscriber = BaseSubscriber<QueryResult>;

impl fmt::Debug for QueryResChannel {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "QueryResChannel")
    }
}

pub type DebugInfoChannel = BaseChannel<RegionMeta>;
pub type DebugInfoSubscriber = BaseSubscriber<RegionMeta>;

impl Debug for DebugInfoChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DebugInfoChannel")
    }
}

#[cfg(feature = "testexport")]
pub type FlushChannel = BaseChannel<()>;
#[cfg(feature = "testexport")]
pub type FlushSubscriber = BaseSubscriber<()>;

#[cfg(feature = "testexport")]
impl Debug for FlushChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FlushChannel")
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use futures::{executor::block_on, StreamExt};

    use super::*;

    #[test]
    fn test_cancel() {
        let (chan, mut sub) = CmdResChannel::pair();
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

        let (chan, sub) = QueryResChannel::pair();
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

        let (chan, sub) = QueryResChannel::pair();
        let resp = QueryResult::Response(result.clone());
        chan.set_result(resp.clone());
        assert_eq!(block_on(sub.result()).unwrap(), resp);

        let (chan, sub) = QueryResChannel::pair();
        let read = QueryResult::Read(ReadResponse {
            read_index: 0,
            txn_extra_op: TxnExtraOp::ReadOldValue,
        });
        chan.set_result(read.clone());
        assert_eq!(block_on(sub.result()).unwrap(), read);
    }

    #[test]
    fn test_cmd_res_stream() {
        let mut builder = CmdResChannelBuilder::default();
        builder.before_set(|res| {
            res.mut_header().set_current_term(6);
        });
        let (chan, sub) = builder.build();
        let mut stream = CmdResStream::new(sub);
        chan.set_result(RaftCmdResponse::default());
        assert_matches!(block_on(stream.next()), Some(CmdResEvent::Finished(res)) if res.get_header().get_current_term() == 6);

        // When using builder, no event is subscribed by default.
        let (mut chan, sub) = CmdResChannelBuilder::default().build();
        let mut stream = CmdResStream::new(sub);
        chan.notify_proposed();
        chan.notify_committed();
        drop(chan);
        assert_matches!(block_on(stream.next()), None);

        let mut builder = CmdResChannelBuilder::default();
        builder.subscribe_proposed();
        let (mut chan, sub) = builder.build();
        let mut stream = CmdResStream::new(sub);
        chan.notify_proposed();
        chan.notify_committed();
        assert_matches!(block_on(stream.next()), Some(CmdResEvent::Proposed));
        drop(chan);
        assert_matches!(block_on(stream.next()), None);

        let mut builder = CmdResChannelBuilder::default();
        builder.subscribe_committed();
        let (mut chan, sub) = builder.build();
        let mut stream = CmdResStream::new(sub);
        chan.notify_proposed();
        chan.notify_committed();
        assert_matches!(block_on(stream.next()), Some(CmdResEvent::Committed));
        drop(chan);
        assert_matches!(block_on(stream.next()), None);
    }
}

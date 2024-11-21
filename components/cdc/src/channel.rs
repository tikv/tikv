// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::{
    channel::mpsc::{
        channel as bounded, unbounded, Receiver, SendError as FuturesSendError, Sender,
        TrySendError, UnboundedReceiver, UnboundedSender,
    },
    executor::block_on,
    stream, SinkExt, Stream, StreamExt,
};
use grpcio::WriteFlags;
use kvproto::cdcpb::{ChangeDataEvent, Event, ResolvedTs};
use protobuf::Message;
use tikv_util::{
    future::block_on_timeout,
    impl_display_as_debug, info,
    memory::{MemoryQuota, MemoryQuotaExceeded},
    time::Instant,
    warn,
};

use crate::{metrics::*, service::ConnId};

/// The maximum bytes of events can be batched into one `CdcEvent::Event`, 32KB.
pub const CDC_EVENT_MAX_BYTES: usize = 32 * 1024;

/// The maximum count of `CdcEvent::Event`s can be batched into
/// one ChangeDataEvent, 64.
const CDC_EVENT_MAX_COUNT: usize = 64;

/// The default `channel` capacity for sending incremental scan events.
///
/// The maximum bytes of in-memory incremental scan events is about 6MB
/// per-connection (EventFeed RPC).
///
/// 6MB = (CDC_CHANNLE_CAPACITY + CDC_EVENT_MAX_COUNT) * CDC_EVENT_MAX_BYTES.
pub const CDC_CHANNLE_CAPACITY: usize = 128;

/// The maximum bytes of ChangeDataEvent, 6MB.
const CDC_RESP_MAX_BYTES: u32 = 6 * 1024 * 1024;

/// Assume the average size of batched `CdcEvent::Event`s is 32KB and
/// the average count of batched `CdcEvent::Event`s is 64.
/// ```text
/// 2 = (CDC_EVENT_MAX_BYTES * CDC_EVENT_MAX_COUNT / CDC_MAX_RESP_SIZE).ceil() + 1 /* reserve for ResolvedTs */;
/// ```
const CDC_RESP_MAX_BATCH_COUNT: usize = 2;

pub enum CdcEvent {
    ResolvedTs(ResolvedTs),
    Event(Event),
    Barrier(Option<Box<dyn FnOnce(()) + Send>>),
}

impl CdcEvent {
    pub fn size(&self) -> usize {
        fail::fail_point!("cdc_event_size", |size| size
            .map(|s| s.parse::<usize>().unwrap())
            .unwrap_or(0));
        match self {
            CdcEvent::ResolvedTs(ref r) => {
                // For region id, it is unlikely to exceed 100,000,000 which is
                // encoded into 4 bytes.
                // For TSO, it is likely to be encoded into 9 bytes,
                // e.g., 426624231625982140.
                //
                // See https://play.golang.org/p/GFA9S-z_kUt
                let approximate_region_id_bytes = 4;
                let approximate_tso_bytes = 9;
                // Protobuf encoding adds a tag to every Uvarint.
                // protobuf::rt::tag_size(1 /* or 2, field number*/) yields 1.
                let tag_bytes = 1;

                // Bytes of an array of region id.
                r.regions.len() * (tag_bytes + approximate_region_id_bytes)
                // Bytes of a TSO.
                + (tag_bytes + approximate_tso_bytes)
            }
            CdcEvent::Event(ref e) => e.compute_size() as _,
            CdcEvent::Barrier(_) => 0,
        }
    }

    pub fn event(&self) -> &Event {
        match self {
            CdcEvent::ResolvedTs(_) | CdcEvent::Barrier(_) => unreachable!(),
            CdcEvent::Event(ref e) => e,
        }
    }

    pub fn resolved_ts(&self) -> &ResolvedTs {
        match self {
            CdcEvent::ResolvedTs(ref r) => r,
            CdcEvent::Event(_) | CdcEvent::Barrier(_) => unreachable!(),
        }
    }
}

impl fmt::Debug for CdcEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CdcEvent::Barrier(_) => {
                let mut d = f.debug_tuple("Barrier");
                d.finish()
            }
            CdcEvent::ResolvedTs(ref r) => {
                let mut d = f.debug_struct("ResolvedTs");
                d.field("resolved ts", &r.ts);
                d.field("region count", &r.regions.len());
                d.finish()
            }
            CdcEvent::Event(e) => {
                let mut d = f.debug_struct("Event");
                d.field("region_id", &e.region_id);
                d.field("request_id", &e.request_id);
                if e.has_entries() {
                    d.field("entries count", &e.get_entries().get_entries().len());
                }
                d.finish()
            }
        }
    }
}

pub struct EventBatcher {
    buffer: Vec<ChangeDataEvent>,
    last_size: u32,

    // statistics
    total_event_bytes: usize,
    total_resolved_ts_bytes: usize,
}

impl EventBatcher {
    pub fn with_capacity(cap: usize) -> EventBatcher {
        EventBatcher {
            buffer: Vec::with_capacity(cap),
            last_size: 0,

            total_event_bytes: 0,
            total_resolved_ts_bytes: 0,
        }
    }

    // The size of the response should not exceed CDC_MAX_RESP_SIZE.
    // Split the events into multiple responses by CDC_MAX_RESP_SIZE here.
    pub fn push(&mut self, event: CdcEvent) {
        let size = event.size();
        if size >= CDC_RESP_MAX_BYTES {
            warn!("cdc event too large"; "size" => size, "event" => ?event);
        }
        match event {
            CdcEvent::Event(e) => {
                if self.buffer.is_empty() || self.last_size + size >= CDC_RESP_MAX_BYTES {
                    self.last_size = 0;
                    self.buffer.push(ChangeDataEvent::default());
                }
                self.last_size += size;
                self.buffer.last_mut().unwrap().mut_events().push(e);
                self.total_event_bytes += size as usize;
            }
            CdcEvent::ResolvedTs(r) => {
                let mut change_data_event = ChangeDataEvent::default();
                change_data_event.set_resolved_ts(r);
                self.buffer.push(change_data_event);

                // Make sure the next message is not batched with ResolvedTs.
                self.last_size = CDC_RESP_MAX_BYTES;
                self.total_resolved_ts_bytes += size as usize;
            }
            CdcEvent::Barrier(_) => {
                // Barrier requires events must be batched across the barrier.
                self.last_size = CDC_RESP_MAX_BYTES;
            }
        }
    }

    pub fn build(self) -> Vec<ChangeDataEvent> {
        self.buffer
    }

    // Return the total bytes of event and resolved ts.
    pub fn statistics(&self) -> (usize, usize) {
        (self.total_event_bytes, self.total_resolved_ts_bytes)
    }
}

pub fn channel(conn_id: ConnId, buffer: usize, memory_quota: Arc<MemoryQuota>) -> (Sink, Drain) {
    let (unbounded_sender, unbounded_receiver) = unbounded();
    let (bounded_sender, bounded_receiver) = bounded(buffer);
    (
        Sink {
            unbounded_sender,
            bounded_sender,
            memory_quota: memory_quota.clone(),
        },
        Drain {
            unbounded_receiver,
            bounded_receiver,
            memory_quota,
            conn_id,
        },
    )
}

#[derive(Clone, Debug, PartialEq)]
pub enum SendError {
    // Full is returned by the sender if the channel is full, this should only happen to the
    // bounded sender.
    Full,
    // Disconnected is returned by the sender if the channel is disconnected.
    Disconnected,
    // Congested is returned if memory quota exceeded.
    Congested,
}

impl std::error::Error for SendError {}

impl_display_as_debug!(SendError);

macro_rules! impl_from_future_send_error {
    ($($f:ty,)+) => {
        $(
            impl From<$f> for SendError {
                fn from(e: $f) -> Self {
                    if e.is_disconnected() {
                        SendError::Disconnected
                    } else if e.is_full() {
                        Self::Full
                    } else {
                        unreachable!()
                    }
                }
            }
        )+
    };
}

impl_from_future_send_error! {
    FuturesSendError,
    TrySendError<ObservedEvent>,
    TrySendError<ScanedEvent>,
}

impl From<MemoryQuotaExceeded> for SendError {
    fn from(_: MemoryQuotaExceeded) -> Self {
        SendError::Congested
    }
}

pub struct ObservedEvent {
    pub created: Instant,
    pub event: CdcEvent,
    pub size: usize,
}

pub struct ScanedEvent {
    pub created: Instant,
    pub event: CdcEvent,
    pub size: usize,
    // Incremental scan can be canceled by region errors. We must check it when draing
    // an event instead of emit it to `Sink`.
    pub truncated: Arc<AtomicBool>,
}

impl ObservedEvent {
    fn new(created: Instant, event: CdcEvent, size: usize) -> Self {
        ObservedEvent {
            created,
            event,
            size,
        }
    }
}

impl ScanedEvent {
    fn new(created: Instant, event: CdcEvent, size: usize, truncated: Arc<AtomicBool>) -> Self {
        ScanedEvent {
            created,
            event,
            size,
            truncated,
        }
    }
}

#[derive(Clone)]
pub struct Sink {
    tx: UnboundedSender<ObservedEvent>,
    memory_quota: Arc<MemoryQuota>,
}

impl Sink {
    /// Only observed events can be sent by `unbounded_send`.
    pub fn send(&self, observed_event: CdcEvent, force: bool) -> Result<(), SendError> {
        let bytes = observed_event.size();
        if force {
            self.memory_quota.alloc_force(bytes);
        } else {
            self.memory_quota.alloc(bytes)?;
        }


        // Try it's best to send error events.
        let bytes = if !force {
            observed_event.size() as usize
        } else {
            0
        };
        if bytes != 0 {
        }
        let ob_event = ObservedEvent::new(Instant::now_coarse(), observed_event, bytes);
        match self.unbounded_sender.unbounded_send(ob_event) {
            Ok(_) => Ok(()),
            Err(e) => {
                // Free quota if send fails.
                self.memory_quota.free(bytes);
                Err(SendError::from(e))
            }
        }
    }

    /// Only scaned events can be sent by `send_all`.
    pub async fn send_all(
        &mut self,
        scaned_events: Vec<CdcEvent>,
        truncated: Arc<AtomicBool>,
    ) -> Result<(), SendError> {
        // Allocate quota in advance.
        let mut total_bytes = 0;
        for event in &scaned_events {
            let bytes = event.size();
            total_bytes += bytes;
        }
        self.memory_quota.alloc(total_bytes as _)?;

        let now = Instant::now_coarse();
        for event in scaned_events {
            let bytes = event.size() as usize;
            let sc_event = ScanedEvent::new(now, event, bytes, truncated.clone());
            if let Err(e) = self.bounded_sender.feed(sc_event).await {
                // Free quota if send fails.
                self.memory_quota.free(total_bytes as _);
                return Err(SendError::from(e));
            }
        }
        if let Err(e) = self.bounded_sender.flush().await {
            // Free quota if send fails.
            self.memory_quota.free(total_bytes as _);
            return Err(SendError::from(e));
        }
        Ok(())
    }
}

pub struct Drain {
    unbounded_receiver: UnboundedReceiver<ObservedEvent>,
    bounded_receiver: Receiver<ScanedEvent>,
    memory_quota: Arc<MemoryQuota>,
    conn_id: ConnId,
}

impl<'a> Drain {
    pub fn drain(&'a mut self) -> impl Stream<Item = (CdcEvent, usize)> + 'a {
        let observed = (&mut self.unbounded_receiver).map(|x| (x.created, x.event, x.size));
        let scaned = (&mut self.bounded_receiver).filter_map(|x| {
            if x.truncated.load(Ordering::Acquire) {
                self.memory_quota.free(x.size as _);
                return futures::future::ready(None);
            }
            futures::future::ready(Some((x.created, x.event, x.size)))
        });

        stream::select(scaned, observed).map(|(start, mut event, size)| {
            CDC_EVENTS_PENDING_DURATION.observe(start.saturating_elapsed_secs() * 1000.0);
            if let CdcEvent::Barrier(ref mut barrier) = event {
                if let Some(barrier) = barrier.take() {
                    // Unset barrier when it is received.
                    barrier(());
                }
            }
            (event, size)
        })
    }

    // Forwards contents to the sink, simulates StreamExt::forward.
    pub async fn forward<S, E>(&'a mut self, sink: &mut S) -> Result<(), E>
    where
        S: futures::Sink<(ChangeDataEvent, WriteFlags), Error = E> + Unpin,
    {
        let total_event_bytes = CDC_GRPC_ACCUMULATE_MESSAGE_BYTES.with_label_values(&["event"]);
        let total_resolved_ts_bytes =
            CDC_GRPC_ACCUMULATE_MESSAGE_BYTES.with_label_values(&["resolved_ts"]);

        let memory_quota = self.memory_quota.clone();
        let mut chunks = self.drain().ready_chunks(CDC_EVENT_MAX_COUNT);
        while let Some(events) = chunks.next().await {
            let mut bytes = 0;
            let mut batcher = EventBatcher::with_capacity(CDC_RESP_MAX_BATCH_COUNT);
            events.into_iter().for_each(|(e, size)| {
                bytes += size;
                batcher.push(e);
            });
            let (event_bytes, resolved_ts_bytes) = batcher.statistics();
            let resps = batcher.build();
            let resps_len = resps.len();
            // Events are about to be sent, free pending events memory counter.
            memory_quota.free(bytes as _);
            for (i, e) in resps.into_iter().enumerate() {
                // Buffer messages and flush them at once.
                let write_flags = WriteFlags::default().buffer_hint(i + 1 != resps_len);
                sink.feed((e, write_flags)).await?;
            }
            sink.flush().await?;
            total_event_bytes.inc_by(event_bytes as u64);
            total_resolved_ts_bytes.inc_by(resolved_ts_bytes as u64);
        }
        Ok(())
    }
}

impl Drop for Drain {
    fn drop(&mut self) {
        self.bounded_receiver.close();
        self.unbounded_receiver.close();
        let start = Instant::now();
        let mut total_bytes = 0;
        let mut drain = Box::pin(async move {
            let conn_id = self.conn_id;
            let memory_quota = self.memory_quota.clone();
            let mut drain = self.drain();
            while let Some((_, bytes)) = drain.next().await {
                total_bytes += bytes;
            }
            memory_quota.free(total_bytes);
            info!("drop Drain finished, free memory"; "conn_id" => ?conn_id,
                "freed_bytes" => total_bytes, "inuse_bytes" => memory_quota.in_use());
        });
        block_on(&mut drain);
        let takes = start.saturating_elapsed();
        if takes >= Duration::from_millis(200) {
            warn!("drop Drain too slow"; "takes" => ?takes);
        }
    }
}

#[allow(clippy::result_unit_err)]
pub fn recv_timeout<S, I>(s: &mut S, dur: std::time::Duration) -> Result<Option<I>, ()>
where
    S: Stream<Item = I> + Unpin,
{
    block_on_timeout(s.next(), dur)
}

#[cfg(test)]
mod tests {
    use std::{
        assert_matches::assert_matches,
        sync::{mpsc, Arc},
        time::Duration,
    };

    use futures::executor::block_on;
    use kvproto::cdcpb::{
        ChangeDataEvent, Event, EventEntries, EventRow, Event_oneof_event, ResolvedTs,
    };

    use super::*;

    type Send = Box<dyn FnMut(CdcEvent) -> Result<(), SendError>>;
    fn new_test_channel(buffer: usize, capacity: usize, force_send: bool) -> (Send, Drain) {
        let memory_quota = Arc::new(MemoryQuota::new(capacity));
        let (mut tx, rx) = channel(ConnId::default(), buffer, memory_quota);
        let mut flag = true;
        let send = move |event| {
            flag = !flag;
            if flag {
                tx.unbounded_send(event, force_send)
            } else {
                block_on(tx.send_all(vec![event], Arc::new(Default::default())))
            }
        };
        (Box::new(send), rx)
    }

    #[test]
    fn test_scanned_event() {
        let mut e = Event::default();
        e.region_id = 233;
        {
            let memory_quota = Arc::new(MemoryQuota::new(1024));
            let (mut tx, mut rx) = channel(ConnId::default(), 10, memory_quota);

            let truncated = Arc::new(AtomicBool::new(false));
            let event = CdcEvent::Event(e.clone());
            let size = event.size() as usize;
            let _ = block_on(tx.send_all(vec![event], truncated));

            let memory_quota = rx.memory_quota.clone();
            let mut drain = rx.drain();
            assert_matches!(block_on(drain.next()), Some((CdcEvent::Event(_), _)));
            assert_eq!(memory_quota.in_use(), size);
        }
        {
            let memory_quota = Arc::new(MemoryQuota::new(1024));
            let (mut tx, mut rx) = channel(ConnId::default(), 10, memory_quota);

            let truncated = Arc::new(AtomicBool::new(true));
            let _ = block_on(tx.send_all(vec![CdcEvent::Event(e)], truncated));

            let memory_quota = rx.memory_quota.clone();
            let mut drain = rx.drain();
            recv_timeout(&mut drain, Duration::from_millis(100)).unwrap_err();
            assert_eq!(memory_quota.in_use(), 0);
        }
    }

    #[test]
    fn test_barrier() {
        let force_send = false;
        let (mut send, mut rx) = new_test_channel(10, usize::MAX, force_send);
        send(CdcEvent::Event(Default::default())).unwrap();
        let (btx1, brx1) = mpsc::channel();
        send(CdcEvent::Barrier(Some(Box::new(move |()| {
            btx1.send(()).unwrap();
        }))))
        .unwrap();
        send(CdcEvent::ResolvedTs(Default::default())).unwrap();
        let (btx2, brx2) = mpsc::channel();
        send(CdcEvent::Barrier(Some(Box::new(move |()| {
            btx2.send(()).unwrap();
        }))))
        .unwrap();

        let mut drain = rx.drain();
        brx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(drain.next()), Some((CdcEvent::Event(_), _)));
        brx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(drain.next()), Some((CdcEvent::Barrier(_), _)));
        brx1.recv_timeout(Duration::from_millis(100)).unwrap();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(drain.next()), Some((CdcEvent::ResolvedTs(_), _)));
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(drain.next()), Some((CdcEvent::Barrier(_), _)));
        brx2.recv_timeout(Duration::from_millis(100)).unwrap();
        brx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
    }

    #[test]
    fn test_nonblocking_batch() {
        let force_send = false;
        for count in 1..CDC_RESP_MAX_BATCH_COUNT + CDC_RESP_MAX_BATCH_COUNT / 2 {
            let (mut send, mut drain) =
                new_test_channel(CDC_EVENT_MAX_COUNT * 2, usize::MAX, force_send);
            for _ in 0..count {
                send(CdcEvent::Event(Default::default())).unwrap();
            }
            drop(send);

            // Forward `drain` after `send` is dropped so that all items should be batched.
            let (mut tx, mut rx) = unbounded();
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.spawn(async move {
                drain.forward(&mut tx).await.unwrap();
            });
            let timeout = Duration::from_millis(100);
            assert!(recv_timeout(&mut rx, timeout).unwrap().is_some());
            assert!(recv_timeout(&mut rx, timeout).unwrap().is_none());
        }
    }

    #[test]
    fn test_congest() {
        let mut e = Event::default();
        e.region_id = 1;
        let event = CdcEvent::Event(e.clone());
        assert_ne!(event.size(), 0);
        // 1KB
        let max_pending_bytes = 1024;
        let buffer = max_pending_bytes / event.size();
        let force_send = false;
        let (mut send, _rx) = new_test_channel(buffer as _, max_pending_bytes as _, force_send);
        for _ in 0..buffer {
            send(CdcEvent::Event(e.clone())).unwrap();
        }
        assert_matches!(send(CdcEvent::Event(e)).unwrap_err(), SendError::Congested);
    }

    #[test]
    fn test_set_capacity() {
        let mut e = Event::default();
        e.region_id = 1;
        let event = CdcEvent::Event(e.clone());
        assert_ne!(event.size(), 0);
        // 1KB
        let max_pending_bytes = 1024;
        let buffer = max_pending_bytes / event.size();
        let force_send = false;

        // Make sure we can increase the memory quota capacity.
        {
            let (mut send, rx) = new_test_channel(buffer as _, max_pending_bytes as _, force_send);
            for _ in 0..buffer {
                send(CdcEvent::Event(e.clone())).unwrap();
            }

            assert_matches!(
                send(CdcEvent::Event(e.clone())).unwrap_err(),
                SendError::Congested
            );

            let memory_quota = rx.memory_quota.clone();
            assert_eq!(memory_quota.capacity(), 1024);

            let new_capacity = 1024 + event.size();
            memory_quota.set_capacity(new_capacity as usize);
            send(CdcEvent::Event(e.clone())).unwrap();
            assert_eq!(memory_quota.capacity(), new_capacity as usize);
        }

        // Make sure we can reduce the memory quota capacity.
        {
            let (mut send, rx) = new_test_channel(buffer as _, max_pending_bytes as _, force_send);
            // Send one less event.
            let count = buffer - 1;
            for _ in 0..count {
                send(CdcEvent::Event(e.clone())).unwrap();
            }

            let memory_quota = rx.memory_quota.clone();
            assert_eq!(memory_quota.capacity(), 1024);

            let new_capacity = 1024 - event.size();
            memory_quota.set_capacity(new_capacity as usize);
            assert_matches!(send(CdcEvent::Event(e)).unwrap_err(), SendError::Congested);
            assert_eq!(memory_quota.capacity(), new_capacity as usize);
        }
    }

    #[test]
    fn test_force_send() {
        let mut e = Event::default();
        e.region_id = 1;
        let event = CdcEvent::Event(e.clone());
        assert_ne!(event.size(), 0);
        // 1KB
        let max_pending_bytes = 1024;
        let buffer = max_pending_bytes / event.size();
        let memory_quota = Arc::new(MemoryQuota::new(max_pending_bytes as _));
        let (tx, _rx) = channel(ConnId::default(), buffer as _, memory_quota);
        for _ in 0..buffer {
            tx.unbounded_send(CdcEvent::Event(e.clone()), false)
                .unwrap();
        }
        assert_matches!(
            tx.unbounded_send(CdcEvent::Event(e.clone()), false)
                .unwrap_err(),
            SendError::Congested
        );
        tx.unbounded_send(CdcEvent::Event(e), true).unwrap();
    }

    #[test]
    fn test_channel_memory_leak() {
        let mut e = Event::default();
        e.region_id = 1;
        let event = CdcEvent::Event(e.clone());
        assert_ne!(event.size(), 0);
        // 1KB
        let max_pending_bytes = 1024;
        let buffer = max_pending_bytes / event.size() + 1;
        let force_send = false;
        // Make sure memory quota is freed when rx is dropped before tx.
        {
            let (mut send, rx) = new_test_channel(buffer as _, max_pending_bytes as _, force_send);
            loop {
                match send(CdcEvent::Event(e.clone())) {
                    Ok(_) => (),
                    Err(e) => {
                        assert_matches!(e, SendError::Congested);
                        break;
                    }
                }
            }
            let memory_quota = rx.memory_quota.clone();
            memory_quota.alloc(event.size() as _).unwrap_err();
            drop(rx);
            memory_quota.alloc(1024).unwrap();
        }
        // Make sure memory quota is freed when tx is dropped before rx.
        {
            let (mut send, rx) = new_test_channel(buffer as _, max_pending_bytes as _, force_send);
            loop {
                match send(CdcEvent::Event(e.clone())) {
                    Ok(_) => (),
                    Err(e) => {
                        assert_matches!(e, SendError::Congested);
                        break;
                    }
                }
            }
            let memory_quota = rx.memory_quota.clone();
            memory_quota.alloc(event.size() as _).unwrap_err();
            drop(send);
            drop(rx);
            memory_quota.alloc(1024).unwrap();
        }
        // Make sure sending message to a closed channel does not leak memory quota.
        {
            let (mut send, rx) = new_test_channel(buffer as _, max_pending_bytes as _, force_send);
            let memory_quota = rx.memory_quota.clone();
            assert_eq!(memory_quota.in_use(), 0);
            drop(rx);
            for _ in 0..max_pending_bytes {
                send(CdcEvent::Event(e.clone())).unwrap_err();
            }
            assert_eq!(memory_quota.in_use(), 0);
            memory_quota.alloc(1024).unwrap();

            // Freeing bytes should not cause overflow.
            memory_quota.free(1024);
            assert_eq!(memory_quota.in_use(), 0);
            memory_quota.free(1024);
            assert_eq!(memory_quota.in_use(), 0);
        }
    }

    #[test]
    fn test_event_batcher() {
        let check_events = |result: Vec<ChangeDataEvent>, expected: Vec<Vec<CdcEvent>>| {
            assert_eq!(result.len(), expected.len());

            for i in 0..expected.len() {
                if !result[i].has_resolved_ts() {
                    assert_eq!(result[i].events.len(), expected[i].len());
                    for j in 0..expected[i].len() {
                        assert_eq!(&result[i].events[j], expected[i][j].event());
                    }
                } else {
                    assert_eq!(expected[i].len(), 1);
                    assert_eq!(result[i].get_resolved_ts(), expected[i][0].resolved_ts());
                }
            }
        };

        let row_small = EventRow::default();
        let event_entries = EventEntries {
            entries: vec![row_small].into(),
            ..Default::default()
        };
        let event_small = Event {
            event: Some(Event_oneof_event::Entries(event_entries)),
            ..Default::default()
        };

        let mut row_big = EventRow::default();
        row_big.set_key(vec![0_u8; CDC_RESP_MAX_BYTES as usize]);
        let event_entries = EventEntries {
            entries: vec![row_big].into(),
            ..Default::default()
        };
        let event_big = Event {
            event: Some(Event_oneof_event::Entries(event_entries)),
            ..Default::default()
        };

        let mut resolved_ts = ResolvedTs::default();
        resolved_ts.set_ts(1);

        // None empty event should not return a zero size.
        assert_ne!(CdcEvent::ResolvedTs(resolved_ts.clone()).size(), 0);
        assert_ne!(CdcEvent::Event(event_big.clone()).size(), 0);
        assert_ne!(CdcEvent::Event(event_small.clone()).size(), 0);

        // An ReslovedTs event follows a small event, they should not be batched
        // in one message.
        let mut batcher = EventBatcher::with_capacity(CDC_RESP_MAX_BATCH_COUNT);
        batcher.push(CdcEvent::ResolvedTs(resolved_ts.clone()));
        batcher.push(CdcEvent::Event(event_small.clone()));

        check_events(
            batcher.build(),
            vec![
                vec![CdcEvent::ResolvedTs(resolved_ts.clone())],
                vec![CdcEvent::Event(event_small.clone())],
            ],
        );

        // A more complex case.
        let mut batcher = EventBatcher::with_capacity(1024);
        batcher.push(CdcEvent::Event(event_small.clone()));
        batcher.push(CdcEvent::ResolvedTs(resolved_ts.clone()));
        batcher.push(CdcEvent::ResolvedTs(resolved_ts.clone()));
        batcher.push(CdcEvent::Event(event_big.clone()));
        batcher.push(CdcEvent::Event(event_small.clone()));
        batcher.push(CdcEvent::Event(event_small.clone()));
        batcher.push(CdcEvent::Event(event_big.clone()));

        check_events(
            batcher.build(),
            vec![
                vec![CdcEvent::Event(event_small.clone())],
                vec![CdcEvent::ResolvedTs(resolved_ts.clone())],
                vec![CdcEvent::ResolvedTs(resolved_ts)],
                vec![CdcEvent::Event(event_big.clone())],
                vec![
                    CdcEvent::Event(event_small.clone()),
                    CdcEvent::Event(event_small),
                ],
                vec![CdcEvent::Event(event_big)],
            ],
        );
    }

    #[test]
    fn test_event_batcher_statistics() {
        let mut event_small = Event::default();
        let row_small = EventRow::default();
        let mut event_entries = EventEntries::default();
        event_entries.entries = vec![row_small].into();
        event_small.event = Some(Event_oneof_event::Entries(event_entries));

        let mut resolved_ts = ResolvedTs::default();
        resolved_ts.set_ts(1);

        let mut batcher = EventBatcher::with_capacity(1024);
        batcher.push(CdcEvent::Event(event_small.clone()));
        assert_eq!(
            batcher.statistics(),
            (CdcEvent::Event(event_small.clone()).size() as usize, 0)
        );

        batcher.push(CdcEvent::ResolvedTs(resolved_ts.clone()));
        assert_eq!(
            batcher.statistics(),
            (
                CdcEvent::Event(event_small.clone()).size() as usize,
                CdcEvent::ResolvedTs(resolved_ts.clone()).size() as usize
            )
        );

        batcher.push(CdcEvent::Event(event_small.clone()));
        assert_eq!(
            batcher.statistics(),
            (
                CdcEvent::Event(event_small.clone()).size() as usize * 2,
                CdcEvent::ResolvedTs(resolved_ts.clone()).size() as usize
            )
        );

        batcher.push(CdcEvent::ResolvedTs(resolved_ts.clone()));
        assert_eq!(
            batcher.statistics(),
            (
                CdcEvent::Event(event_small).size() as usize * 2,
                CdcEvent::ResolvedTs(resolved_ts).size() as usize * 2
            )
        );
    }

    #[test]
    fn test_cdc_event_resolved_ts_size() {
        // A typical region id.
        let region_id = 4194304;
        // A typical ts.
        let ts = 426624231625982140;
        for i in 0..17 {
            let mut resolved_ts = ResolvedTs::default();
            resolved_ts.ts = ts;
            resolved_ts.regions = vec![region_id; 2usize.pow(i)];
            assert_eq!(
                resolved_ts.compute_size(),
                CdcEvent::ResolvedTs(resolved_ts).size()
            );
        }
    }
}

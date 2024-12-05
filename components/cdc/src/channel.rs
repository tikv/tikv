// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt, pin::Pin, result::Result as StdResult, sync::Arc, task::Context, time::Duration};

use futures::{
    channel::mpsc::{
        unbounded, SendError as FuturesSendError, TrySendError, UnboundedReceiver, UnboundedSender,
    },
    executor::block_on,
    lock::Mutex,
    ready,
    task::Poll,
    SinkExt, Stream, StreamExt,
};
use kvproto::cdcpb::{
    ChangeDataEvent, Error as ErrorEvent, Event, EventEntries, EventRow, Event_oneof_event,
    ResolvedTs,
};
use protobuf::Message;
use tikv_util::{
    debug,
    future::block_on_timeout,
    impl_display_as_debug, info,
    memory::{MemoryQuota, MemoryQuotaExceeded},
    time::Instant,
    warn,
};

use crate::{
    delegate::DownstreamId,
    metrics::*,
    service::{ConnId, RequestId},
    Error, Result,
};

/// The maximum bytes of events can be batched into one `CdcEvent::Event`, 32KB.
pub const CDC_EVENT_MAX_BYTES: usize = 32 * 1024;

/// The maximum bytes of ChangeDataEvent, 6MB.
const CDC_RESP_MAX_BYTES: usize = 6 * 1024 * 1024;

pub enum CdcEvent {
    ResolvedTs(ResolvedTs),
    Event(Event),
    Barrier(Box<dyn FnOnce(()) + Send>),
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

pub fn channel(conn_id: ConnId, memory_quota: Arc<MemoryQuota>) -> (Sink, Drain) {
    let (tx, rx) = unbounded();
    (
        Sink {
            tx,
            memory_quota: memory_quota.clone(),
        },
        Drain {
            conn_id,
            rx,
            memory_quota,
            buffer: None,
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
}

impl From<MemoryQuotaExceeded> for SendError {
    fn from(_: MemoryQuotaExceeded) -> Self {
        SendError::Congested
    }
}

struct ObservedEvent {
    pub created: Instant,
    pub event: CdcEvent,
    pub size: usize,
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

#[derive(Clone)]
pub struct Sink {
    tx: UnboundedSender<ObservedEvent>,
    memory_quota: Arc<MemoryQuota>,
}

impl Sink {
    fn send(&self, event: CdcEvent, force: bool) -> StdResult<(), SendError> {
        let bytes = event.size();
        if force {
            self.memory_quota.alloc_force(bytes);
        } else {
            self.memory_quota.alloc(bytes)?;
        }

        let event = ObservedEvent::new(Instant::now_coarse(), event, bytes);
        match self.tx.unbounded_send(event) {
            Ok(_) => Ok(()),
            Err(e) => {
                self.memory_quota.free(bytes);
                Err(SendError::from(e))
            }
        }
    }

    async fn send_all(&mut self, events: Vec<CdcEvent>) -> StdResult<(), SendError> {
        let now = Instant::now_coarse();
        let mut bytes = 0;
        for event in events {
            bytes += event.size();
            let event = ObservedEvent::new(now, event, bytes);
            if let Err(e) = self.tx.feed(event).await {
                self.memory_quota.free(bytes);
                return Err(SendError::from(e));
            }
        }
        if let Err(e) = self.tx.flush().await {
            self.memory_quota.free(bytes);
            return Err(SendError::from(e));
        }
        Ok(())
    }

    pub fn send_resolved_ts(&self, ts_event: CdcEvent) -> StdResult<(), SendError> {
        self.send(ts_event, false)
    }
}

pub struct Drain {
    conn_id: ConnId,
    rx: UnboundedReceiver<ObservedEvent>,
    memory_quota: Arc<MemoryQuota>,

    buffer: Option<ChangeDataEvent>,
}

impl Stream for Drain {
    type Item = ChangeDataEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut();
        if let Some(event) = this.buffer.take() {
            return Poll::Ready(Some(event));
        }

        let mut event = ChangeDataEvent::default();
        let mut size = 0;
        Poll::Ready(loop {
            if let Some(item) = ready!(Pin::new(&mut this.rx).poll_next(cx)) {
                this.memory_quota.free(item.size);
                CDC_EVENTS_PENDING_DURATION
                    .observe(item.created.saturating_elapsed_secs() * 1000.0);
                match item.event {
                    CdcEvent::Barrier(barrier) => barrier(()),
                    CdcEvent::ResolvedTs(x) => {
                        let mut ts_event = ChangeDataEvent::default();
                        ts_event.set_resolved_ts(x);
                        self.buffer = Some(ts_event);
                        break Some(event);
                    }
                    CdcEvent::Event(x) => {
                        event.mut_events().push(x);
                        size += item.size;
                        if size >= CDC_RESP_MAX_BYTES {
                            break Some(event);
                        }
                    }
                }
            } else {
                break None;
            }
        })
    }
}

impl Drop for Drain {
    fn drop(&mut self) {
        self.rx.close();
        let start = Instant::now();
        block_on(async { while self.next().await.is_some() {} });
        let takes = start.saturating_elapsed();
        if takes >= Duration::from_millis(200) {
            warn!("drop Drain too slow"; "conn_id" => ?self.conn_id, "takes" => ?takes);
        }
    }
}

#[derive(Clone)]
pub struct DownstreamSink {
    region_id: u64,
    downstream_id: DownstreamId,
    req_id: RequestId,
    canceled: Arc<Mutex<bool>>,
    sink: Sink,
}

impl DownstreamSink {
    pub fn new(region_id: u64, downstream_id: DownstreamId, req_id: RequestId, sink: Sink) -> Self {
        DownstreamSink {
            region_id,
            downstream_id,
            req_id,
            canceled: Arc::new(Mutex::new(false)),
            sink,
        }
    }

    fn handle_error(&self, e: SendError) -> Error {
        match e {
            SendError::Disconnected => {
                debug!("cdc send event failed, disconnected";
                    "region_id" => self.region_id, "downstream_id" => ?self.downstream_id);
                Error::Sink(SendError::Disconnected)
            }
            e @ SendError::Full | e @ SendError::Congested => {
                info!("cdc send event failed, full";
                    "region_id" => self.region_id, "downstream_id" => ?self.downstream_id);
                Error::Sink(e)
            }
        }
    }

    fn send(&self, event: CdcEvent, force: bool) -> Result<()> {
        if let Err(e) = self.sink.send(event, force) {
            return Err(self.handle_error(e));
        }
        Ok(())
    }

    async fn send_all(&mut self, events: Vec<CdcEvent>) -> Result<()> {
        if let Err(e) = self.sink.send_all(events).await {
            return Err(self.handle_error(e));
        }
        Ok(())
    }

    pub async fn send_observed_raw(&self, index: u64, events: Vec<EventRow>) -> Result<()> {
        let event = CdcEvent::Event(Event {
            region_id: self.region_id,
            index,
            request_id: self.req_id.0,
            event: Some(Event_oneof_event::Entries(EventEntries {
                entries: events.into(),
                ..Default::default()
            })),
            ..Default::default()
        });
        if !*self.canceled.lock().await {
            return self.send(event, false);
        }
        Ok(())
    }

    pub async fn send_observed_tidb(&self, events: Vec<EventRow>) -> Result<()> {
        let event = CdcEvent::Event(Event {
            region_id: self.region_id,
            request_id: self.req_id.0,
            event: Some(Event_oneof_event::Entries(EventEntries {
                entries: events.into(),
                ..Default::default()
            })),
            ..Default::default()
        });
        if !*self.canceled.lock().await {
            return self.send(event, false);
        }
        Ok(())
    }

    pub async fn send_scaned(&mut self, events: Vec<Vec<EventRow>>) -> Result<()> {
        let mut rows = Vec::with_capacity(events.len());
        for x in events.into_iter().filter(|x| !x.is_empty()) {
            rows.push(CdcEvent::Event(Event {
                region_id: self.region_id,
                request_id: self.req_id.0,
                event: Some(Event_oneof_event::Entries(EventEntries {
                    entries: x.into(),
                    ..Default::default()
                })),
                ..Default::default()
            }));
        }
        if !*self.canceled.lock().await {
            return self.send_all(rows).await;
        }
        Ok(())
    }

    pub async fn send_barrier(&self, barrier: Box<dyn FnOnce(()) + Send>) -> Result<()> {
        if !*self.canceled.lock().await {
            return self.send(CdcEvent::Barrier(barrier), true);
        }
        Ok(())
    }

    pub async fn cancel_by_error(&self, err_event: ErrorEvent) -> Result<()> {
        let mut canceled = self.canceled.lock().await;
        if !*canceled {
            *canceled = true;
            let event = Event {
                region_id: self.region_id,
                request_id: self.req_id.0,
                event: Some(Event_oneof_event::Error(err_event)),
                ..Default::default()
            };
            return self.send(CdcEvent::Event(event), true);
        }
        Ok(())
    }
}

#[allow(clippy::result_unit_err)]
pub fn recv_timeout<S, I>(s: &mut S, dur: std::time::Duration) -> StdResult<Option<I>, ()>
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

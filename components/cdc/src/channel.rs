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
        }
    }
}

impl fmt::Debug for CdcEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
    req_id: RequestId,
    canceled: Arc<Mutex<bool>>,
    sink: Sink,
}

impl DownstreamSink {
    pub fn new(region_id: u64, req_id: RequestId, sink: Sink) -> Self {
        DownstreamSink {
            region_id,
            req_id,
            canceled: Arc::new(Mutex::new(false)),
            sink,
        }
    }

    fn handle_error(&self, e: SendError) -> Error {
        match e {
            SendError::Disconnected => {
                debug!("cdc send event failed, disconnected";
                    "region_id" => self.region_id, "request_id" => self.req_id.0);
            }
            SendError::Congested => {
                info!("cdc send event failed, congested";
                    "region_id" => self.region_id, "request_id" => self.req_id.0);
            }
        }
        Error::Sink(e)
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
    use std::sync::Arc;

    use futures::executor::block_on;
    use kvproto::cdcpb::{Event, ResolvedTs};

    use super::*;

    #[test]
    fn test_send_all() {}

    #[test]
    fn test_send() {}

    #[test]
    fn test_set_capacity() {}

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
                resolved_ts.compute_size() as usize,
                CdcEvent::ResolvedTs(resolved_ts).size()
            );
        }
    }
}

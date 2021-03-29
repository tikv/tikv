// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::Entry;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use collections::HashMap;
use futures::future::{self, TryFutureExt};
use futures::sink::{Sink, SinkExt};
use futures::stream::TryStreamExt;
use futures::task::{Context, Poll};
use grpcio::{
    DuplexSink, Error as GrpcError, RequestStream, RpcContext, RpcStatus, RpcStatusCode, WriteFlags,
};
use kvproto::cdcpb::{
    ChangeData, ChangeDataEvent, ChangeDataRequest, Compatibility, Event, ResolvedTs,
};
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use protobuf::Message;
use tikv_util::worker::*;

use crate::delegate::{Downstream, DownstreamID};
use crate::endpoint::{Deregister, Task};
use crate::metrics::*;
use crate::rate_limiter::{new_pair, DrainerError, RateLimiter};
use futures::ready;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::event::Event as Event_oneof_event;
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::Event_oneof_event;
use std::collections::VecDeque;
use std::pin::Pin;

static CONNECTION_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

const CDC_MAX_RESP_SIZE: u32 = 6 * 1024 * 1024; // 6MB
const CDC_EVENT_MAX_BATCH_SIZE: usize = 128;

/// A unique identifier of a Connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnID(usize);

impl ConnID {
    pub fn new() -> ConnID {
        ConnID(CONNECTION_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

#[derive(Clone)]
pub enum CdcEvent {
    ResolvedTs(ResolvedTs),
    Event(Event),
}

impl CdcEvent {
    pub fn size(&self) -> u32 {
        match self {
            CdcEvent::ResolvedTs(ref r) => r.compute_size(),
            CdcEvent::Event(ref e) => e.compute_size(),
        }
    }

    pub fn event(&self) -> &Event {
        match self {
            CdcEvent::ResolvedTs(_) => unreachable!(),
            CdcEvent::Event(ref e) => e,
        }
    }

    pub fn resolved_ts(&self) -> &ResolvedTs {
        match self {
            CdcEvent::ResolvedTs(ref r) => r,
            CdcEvent::Event(_) => unreachable!(),
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
                #[cfg(not(feature = "prost-codec"))]
                if e.has_entries() {
                    d.field("entries count", &e.get_entries().get_entries().len());
                }
                #[cfg(feature = "prost-codec")]
                if e.event.is_some() {
                    use kvproto::cdcpb::event;
                    if let Some(event::Event::Entries(ref es)) = e.event.as_ref() {
                        d.field("entries count", &es.entries.len());
                    }
                }
                d.finish()
            }
        }
    }
}

struct EventBatcher {
    buffer: Vec<ChangeDataEvent>,
    last_size: u32,
}

impl EventBatcher {
    fn with_capacity(cap: usize) -> EventBatcher {
        EventBatcher {
            buffer: Vec::with_capacity(cap),
            last_size: 0,
        }
    }

    // The size of the response should not exceed CDC_MAX_RESP_SIZE.
    // Split the events into multiple responses by CDC_MAX_RESP_SIZE here.
    fn push(&mut self, event: CdcEvent) {
        let size = event.size();
        if size >= CDC_MAX_RESP_SIZE {
            warn!("cdc event too large"; "size" => size, "event" => ?event);
        }
        match event {
            CdcEvent::Event(e) => {
                if self.buffer.is_empty() || self.last_size + size >= CDC_MAX_RESP_SIZE {
                    self.last_size = 0;
                    self.buffer.push(ChangeDataEvent::default());
                }
                self.last_size += size;

                let event = e.event.as_ref().unwrap();
                if let Event_oneof_event::Error(err) = event {
                    info!("cdc sending error"; "region_id" => e.region_id, "error" => ?err);
                }

                self.buffer.last_mut().unwrap().mut_events().push(e);
            }
            CdcEvent::ResolvedTs(r) => {
                let mut change_data_event = ChangeDataEvent::default();
                change_data_event.set_resolved_ts(r);
                self.buffer.push(change_data_event);

                // Make sure the next message is not batched with ResolvedTs.
                self.last_size = CDC_MAX_RESP_SIZE;
            }
        }
    }

    fn build(self) -> Vec<ChangeDataEvent> {
        self.buffer
    }
}

pub struct EventBatcherSink<S, E>
where
    S: Sink<(ChangeDataEvent, grpcio::WriteFlags), Error = E> + Send + Unpin,
{
    // buf stores events just received.
    buf: Option<Vec<CdcEvent>>,
    // a queue for protobuf events ready to send out.
    send_buf: VecDeque<ChangeDataEvent>,
    // the final downstream sink
    inner_sink: S,
}

impl<S, E> EventBatcherSink<S, E>
where
    S: Sink<(ChangeDataEvent, grpcio::WriteFlags), Error = E> + Send + Unpin,
{
    pub fn new(sink: S) -> Self {
        Self {
            buf: None,
            send_buf: VecDeque::new(),
            inner_sink: sink,
        }
    }

    /// converts all buffered CdcEvents into ChangeDataEvents.
    /// consumes `buf`
    fn prepare_flush(&mut self) {
        assert!(self.buf.is_some());

        let mut batcher = EventBatcher::with_capacity(CDC_EVENT_MAX_BATCH_SIZE);
        self.buf
            .take()
            .unwrap()
            .into_iter()
            .for_each(|event| batcher.push(event));
        batcher
            .build()
            .into_iter()
            .for_each(|event| self.send_buf.push_back(event));
    }
}

impl<S, E> Sink<(CdcEvent, grpcio::WriteFlags)> for EventBatcherSink<S, E>
where
    S: Sink<(ChangeDataEvent, grpcio::WriteFlags), Error = E> + Send + Unpin,
{
    type Error = E;

    /// Will block the task if:
    /// 1) there are ChangeDataEvents not sent out, or buf` has at least 1024 messages,
    /// and 2) they cannot be successfully flushed down the `inner_sink` immediately.
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        if !this.send_buf.is_empty() {
            ready!(this.poll_flush_unpin(cx))?;
        }

        if this.buf.is_none() {
            this.buf = Some(vec![]);
            return Poll::Ready(Ok(()));
        }

        if this.buf.as_ref().unwrap().len() >= 32 {
            this.prepare_flush();
            ready!(this.inner_sink.poll_flush_unpin(cx))?;

            assert!(this.buf.is_none());
            this.buf = Some(vec![]);
            return Poll::Ready(Ok(()));
        }

        Poll::Ready(Ok(()))
    }

    /// Will always succeed as long as called correctly.
    fn start_send(self: Pin<&mut Self>, item: (CdcEvent, WriteFlags)) -> Result<(), Self::Error> {
        let this = self.get_mut();
        assert!(this.buf.is_some());

        let (event, _) = item;
        this.buf.as_mut().unwrap().push(event);
        Ok(())
    }

    /// Converts any CdcEvents in `buf` to ChangeDataEvents,
    /// then try to feed them to the `inner_sink`,
    /// and after finishing sending the ChangeDataEvents,
    /// it flushes the `inner_sink`.
    ///
    /// The above process will block and yield if any step is blocked.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        if this.buf.is_some() {
            this.prepare_flush();
        }

        assert!(this.buf.is_none());

        let flag = WriteFlags::default().buffer_hint(false);
        while !this.send_buf.is_empty() {
            ready!(this.inner_sink.poll_ready_unpin(cx))?;
            let event = this.send_buf.pop_front().unwrap();

            let latest_resolved_ts = event.get_resolved_ts().get_ts();
            if latest_resolved_ts > 0 {
                CDC_GRPC_WRITE_RESOLVED_TS.set(
                    txn_types::TimeStamp::new(event.get_resolved_ts().get_ts()).physical() as i64,
                );
            }

            this.inner_sink.start_send_unpin((event, flag))?;
        }

        ready!(this.inner_sink.poll_flush_unpin(cx))?;
        Poll::Ready(Ok(()))
    }

    /// Closes the underlying `inner_sink`.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.inner_sink.poll_close_unpin(cx)
    }
}

bitflags::bitflags! {
    pub struct FeatureGate: u8 {
        const BATCH_RESOLVED_TS = 0b00000001;
        // Uncomment when its ready.
        // const LargeTxn       = 0b00000010;

        // Enables advanced flow control so that TiKV will not OOM when
        // Unified Sorter is enabled in TiCDC.
        const ADVANCED_FLOW_CONTROL = 0b00000100;
    }
}

pub struct Conn {
    id: ConnID,
    // sink: BatchSender<CdcEvent>,
    sink: RateLimiter<CdcEvent>,
    downstreams: HashMap<u64, DownstreamID>,
    peer: String,
    version: Option<(semver::Version, FeatureGate)>,
}

impl Conn {
    pub fn new(sink: RateLimiter<CdcEvent>, peer: String) -> Conn {
        Conn {
            id: ConnID::new(),
            sink,
            downstreams: HashMap::default(),
            version: None,
            peer,
        }
    }

    // TODO refactor into Error::Version.
    pub fn check_version_and_set_feature(&mut self, ver: semver::Version) -> Option<Compatibility> {
        // Assume batch resolved ts will be release in v4.0.7
        // For easy of testing (nightly CI), we lower the gate to v4.0.6
        // TODO bump the version when cherry pick to release branch.
        let v407_bacth_resoled_ts = semver::Version::new(4, 0, 6);

        // Enable this only on 5.0.x
        let v500_advanced_flow_control = semver::Version::new(4, 9999, 0);

        match &self.version {
            Some((version, _)) => {
                if version == &ver {
                    None
                } else {
                    error!("different version on the same connection";
                        "previous version" => ?version, "version" => ?ver,
                        "downstream" => ?self.peer, "conn_id" => ?self.id);
                    Some(Compatibility {
                        required_version: version.to_string(),
                        ..Default::default()
                    })
                }
            }
            None => {
                let mut features = FeatureGate::empty();
                if v407_bacth_resoled_ts <= ver {
                    features.toggle(FeatureGate::BATCH_RESOLVED_TS);
                }
                if v500_advanced_flow_control <= ver {
                    features.toggle(FeatureGate::ADVANCED_FLOW_CONTROL);
                }
                info!("cdc connection version"; "version" => ver.to_string(), "features" => ?features);
                self.version = Some((ver, features));
                None
            }
        }
        // Return Err(Compatibility) when TiKV reaches the next major release,
        // so that we can remove feature gates.
    }

    pub fn get_feature(&self) -> Option<&FeatureGate> {
        self.version.as_ref().map(|(_, f)| f)
    }

    pub fn get_peer(&self) -> &str {
        &self.peer
    }

    pub fn get_id(&self) -> ConnID {
        self.id
    }

    pub fn take_downstreams(self) -> HashMap<u64, DownstreamID> {
        self.downstreams
    }

    pub fn get_sink(&self) -> RateLimiter<CdcEvent> {
        self.sink.clone()
    }

    pub fn subscribe(&mut self, region_id: u64, downstream_id: DownstreamID) -> bool {
        match self.downstreams.entry(region_id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(downstream_id);
                true
            }
        }
    }

    pub fn unsubscribe(&mut self, region_id: u64) {
        self.downstreams.remove(&region_id);
    }

    pub fn downstream_id(&self, region_id: u64) -> Option<DownstreamID> {
        self.downstreams.get(&region_id).copied()
    }

    pub fn flush(&self) {
        self.sink.start_flush();
    }
}

/// Service implements the `ChangeData` service.
///
/// It's a front-end of the CDC service, schedules requests to the `Endpoint`.
#[derive(Clone)]
pub struct Service {
    scheduler: Scheduler<Task>,
    // We are using a tokio runtime because there are some futures that require a timer,
    // and the tokio library provides a good implementation for using timers with futures.
    // runtime: Arc<tokio::runtime::Runtime>,
}

impl Service {
    /// Create a ChangeData service.
    ///
    /// It requires a scheduler of an `Endpoint` in order to schedule tasks.
    pub fn new(scheduler: Scheduler<Task>) -> Service {
        /*let tokio_runtime = tokio::runtime::Builder::new()
        .threaded_scheduler()
        .enable_time()
        .build()
        .unwrap();*/
        Service {
            scheduler,
            // runtime: Arc::new(tokio_runtime),
        }
    }
}

impl ChangeData for Service {
    fn event_feed(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<ChangeDataRequest>,
        mut sink: DuplexSink<ChangeDataEvent>,
    ) {
        // TODO determine the right values
        // 2048 is perfect fine with batching resolved-ts enabled.
        let (rate_limiter, drainer) = new_pair::<CdcEvent>(128, 2048);
        let peer = ctx.peer();
        let conn = Conn::new(rate_limiter, peer.clone());
        let conn_id = conn.get_id();

        debug!("cdc streaming request accepted"; "peer" => ?peer, "conn_id" => ?conn_id);
        if let Err(status) = self
            .scheduler
            .schedule(Task::OpenConn { conn })
            .map_err(|e| RpcStatus::new(RpcStatusCode::INVALID_ARGUMENT, Some(format!("{:?}", e))))
        {
            error!("cdc connection failed to initialize"; "error" => ?status);
            ctx.spawn(sink.fail(status).unwrap_or_else(
                |e| error!("cdc failed to failed to initialize error"; "error" => ?e),
            ));
            return;
        }

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();
        let recv_req = stream.try_for_each(move |request| {
            let region_epoch = request.get_region_epoch().clone();
            let req_id = request.get_request_id();
            let enable_old_value = request.get_extra_op() == TxnExtraOp::ReadOldValue;
            let version = match semver::Version::parse(request.get_header().get_ticdc_version()) {
                Ok(v) => v,
                Err(e) => {
                    warn!("empty or invalid TiCDC version, please upgrading TiCDC";
                        "version" => request.get_header().get_ticdc_version(),
                        "error" => ?e);
                    semver::Version::new(0, 0, 0)
                }
            };
            debug!("new cdc request"; "request_id" => req_id);
            let downstream = Downstream::new(
                peer.clone(),
                region_epoch,
                req_id,
                conn_id,
                enable_old_value,
            );

            let ret = scheduler
                .schedule(Task::Register {
                    request,
                    downstream,
                    conn_id,
                    version,
                })
                .map_err(|e| {
                    GrpcError::RpcFailure(RpcStatus::new(
                        RpcStatusCode::INVALID_ARGUMENT,
                        Some(format!("{:?}", e)),
                    ))
                });
            debug!("cdc request ready"; "request_id" => req_id);
            future::ready(ret)
        });

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();
        ctx.spawn(async move {
            let res = recv_req.await;
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("cdc deregister failed"; "error" => ?e, "conn_id" => ?conn_id);
            }
            match res {
                Ok(()) => {
                    info!("cdc receive closed"; "downstream" => peer, "conn_id" => ?conn_id);
                }
                Err(e) => {
                    warn!("cdc receive failed"; "error" => ?e, "downstream" => peer, "conn_id" => ?conn_id);
                }
            }
        });

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();

        ctx.spawn(async move {
            // EventBatcherSink is used to pack CdcEvents into ChangeDataEvents.
            // Internally, EventBatcherSink composes a "inverted flat map" in front of the final sink.
            let batched_sink = EventBatcherSink::new(&mut sink);
            // The drainer will block asynchronously, until
            // 1) all senders have exited,
            // 2) the grpc sink has been closed,
            // 3) an error has occurred in the grpc sink,
            // or 4) the sink has been forced to close due to a congestion.
            let drain_res = drainer.drain(
                batched_sink,
                // We disable buffering in the grpc library
                WriteFlags::default().buffer_hint(false)
            ).await;
            match drain_res {
                Ok(_) => {
                    info!("cdc drainer exit"; "downstream" => peer.clone(), "conn_id" => ?conn_id);
                    let _ = sink.fail(RpcStatus::new(RpcStatusCode::CANCELLED, Some("upstreams closed".into()))).await;
                },
                Err(e) => {
                    error!("cdc drainer exit"; "downstream" => peer.clone(), "conn_id" => ?conn_id, "error" => ?e);
                    if let DrainerError::RateLimitExceededError = e {
                        let _ = sink.fail(RpcStatus::new(RpcStatusCode::CANCELLED, Some("stream congested".into()))).await;
                    }
                }
            }
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("cdc deregister failed"; "error" => ?e);
            }

            info!("cdc send closed"; "downstream" => peer.clone(), "conn_id" => ?conn_id);
        });
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "prost-codec")]
    use kvproto::cdcpb::event::{
        Entries as EventEntries, Event as Event_oneof_event, Row as EventRow,
    };
    use kvproto::cdcpb::{ChangeDataEvent, Event, ResolvedTs};
    #[cfg(not(feature = "prost-codec"))]
    use kvproto::cdcpb::{EventEntries, EventRow, Event_oneof_event};

    use crate::service::{
        CdcEvent, EventBatcher, EventBatcherSink, CDC_EVENT_MAX_BATCH_SIZE, CDC_MAX_RESP_SIZE,
    };
    use futures::{SinkExt, StreamExt};

    #[tokio::test]
    async fn test_event_batcher_sink() {
        let (sink, mut rx) =
            futures::channel::mpsc::unbounded::<(ChangeDataEvent, grpcio::WriteFlags)>();
        let mut batch_sink = EventBatcherSink::new(sink);
        let send_task = tokio::spawn(async move {
            let flag = grpcio::WriteFlags::default().buffer_hint(false);
            for i in 0..10000000u64 {
                let mut resolved_ts = ResolvedTs::default();
                resolved_ts.set_ts(i);
                batch_sink
                    .send((CdcEvent::ResolvedTs(resolved_ts), flag))
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        });

        let mut expected = 0u64;
        while let Some((e, _)) = rx.next().await {
            assert_eq!(e.get_resolved_ts().get_ts(), expected);
            expected += 1;
        }
        assert_eq!(expected, 10000000);
        send_task.await.unwrap();
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
        row_big.set_key(vec![0_u8; CDC_MAX_RESP_SIZE as usize]);
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
        let mut batcher = EventBatcher::with_capacity(CDC_EVENT_MAX_BATCH_SIZE);
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
                vec![CdcEvent::Event(event_small); 2],
                vec![CdcEvent::Event(event_big)],
            ],
        );
    }
}

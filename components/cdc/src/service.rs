// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::Entry;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use collections::HashMap;
use futures::future::{self, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::TryStreamExt;
use grpcio::{DuplexSink, Error as GrpcError, RequestStream, RpcContext, RpcStatus, RpcStatusCode};
use kvproto::cdcpb::{
    ChangeData, ChangeDataEvent, ChangeDataRequest, Compatibility, Event, ResolvedTs,
};
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use protobuf::Message;
use tikv_util::worker::*;
use tikv_util::{error, info, warn};

use crate::channel::{channel, MemoryQuota, Sink};
use crate::delegate::{Downstream, DownstreamID};
use crate::endpoint::{Deregister, Task};

static CONNECTION_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

const CDC_MAX_RESP_SIZE: u32 = 6 * 1024 * 1024; // 6MB

/// A unique identifier of a Connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnID(usize);

impl ConnID {
    pub fn new() -> ConnID {
        ConnID(CONNECTION_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

pub enum CdcEvent {
    ResolvedTs(ResolvedTs),
    Event(Event),
    Barrier(Option<Box<dyn FnOnce(()) + Send>>),
}

impl CdcEvent {
    pub fn size(&self) -> u32 {
        match self {
            CdcEvent::ResolvedTs(ref r) => r.compute_size(),
            CdcEvent::Event(ref e) => e.compute_size(),
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

pub struct EventBatcher {
    buffer: Vec<ChangeDataEvent>,
    last_size: u32,
}

impl EventBatcher {
    pub fn with_capacity(cap: usize) -> EventBatcher {
        EventBatcher {
            buffer: Vec::with_capacity(cap),
            last_size: 0,
        }
    }

    // The size of the response should not exceed CDC_MAX_RESP_SIZE.
    // Split the events into multiple responses by CDC_MAX_RESP_SIZE here.
    pub fn push(&mut self, event: CdcEvent) {
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
                self.buffer.last_mut().unwrap().mut_events().push(e);
            }
            CdcEvent::ResolvedTs(r) => {
                let mut change_data_event = ChangeDataEvent::default();
                change_data_event.set_resolved_ts(r);
                self.buffer.push(change_data_event);

                // Make sure the next message is not batched with ResolvedTs.
                self.last_size = CDC_MAX_RESP_SIZE;
            }
            CdcEvent::Barrier(_) => {
                // Barrier requires events must be batched accross the barrier.
                self.last_size = CDC_MAX_RESP_SIZE;
            }
        }
    }

    pub fn build(self) -> Vec<ChangeDataEvent> {
        self.buffer
    }
}

bitflags::bitflags! {
    pub struct FeatureGate: u8 {
        const BATCH_RESOLVED_TS = 0b00000001;
        // Uncomment when its ready.
        // const LargeTxn       = 0b00000010;
    }
}

pub struct Conn {
    id: ConnID,
    sink: Sink,
    downstreams: HashMap<u64, DownstreamID>,
    peer: String,
    version: Option<(semver::Version, FeatureGate)>,
}

impl Conn {
    pub fn new(sink: Sink, peer: String) -> Conn {
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
        let v407_batch_resolved_ts = semver::Version::new(4, 0, 6);

        match &self.version {
            Some((version, _)) => {
                if version == &ver {
                    None
                } else {
                    error!("cdc different version on the same connection";
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
                if v407_batch_resolved_ts <= ver {
                    features.toggle(FeatureGate::BATCH_RESOLVED_TS);
                }
                info!("cdc connection version";
                    "version" => ver.to_string(), "features" => ?features, "downstream" => ?self.peer);
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

    pub fn get_sink(&self) -> &Sink {
        &self.sink
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
}

/// Service implements the `ChangeData` service.
///
/// It's a front-end of the CDC service, schedules requests to the `Endpoint`.
#[derive(Clone)]
pub struct Service {
    scheduler: Scheduler<Task>,
    memory_quota: MemoryQuota,
}

impl Service {
    /// Create a ChangeData service.
    ///
    /// It requires a scheduler of an `Endpoint` in order to schedule tasks.
    pub fn new(scheduler: Scheduler<Task>, memory_quota: MemoryQuota) -> Service {
        Service {
            scheduler,
            memory_quota,
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
        // TODO explain buffer.
        let buffer = 1024;
        let (event_sink, mut event_drain) = channel(buffer, self.memory_quota.clone());
        let peer = ctx.peer();
        let conn = Conn::new(event_sink, peer);
        let conn_id = conn.get_id();

        if let Err(status) = self
            .scheduler
            .schedule(Task::OpenConn { conn })
            .map_err(|e| {
                RpcStatus::with_message(RpcStatusCode::INVALID_ARGUMENT, format!("{:?}", e))
            })
        {
            error!("cdc connection initiate failed"; "error" => ?status);
            ctx.spawn(
                sink.fail(status)
                    .unwrap_or_else(|e| error!("cdc failed to send error"; "error" => ?e)),
            );
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
                    GrpcError::RpcFailure(RpcStatus::with_message(
                        RpcStatusCode::INVALID_ARGUMENT,
                        format!("{:?}", e),
                    ))
                });
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
            let res = event_drain.forward(&mut sink).await;
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("cdc deregister failed"; "error" => ?e);
            }
            match res {
                Ok(_s) => {
                    info!("cdc send closed"; "downstream" => peer, "conn_id" => ?conn_id);
                    let _ = sink.close().await;
                }
                Err(e) => {
                    warn!("cdc send failed"; "error" => ?e, "downstream" => peer, "conn_id" => ?conn_id);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures::executor::block_on;
    use grpcio::{self, ChannelBuilder, EnvBuilder, Server, ServerBuilder, WriteFlags};
    #[cfg(feature = "prost-codec")]
    use kvproto::cdcpb::event::{
        Entries as EventEntries, Event as Event_oneof_event, Row as EventRow,
    };
    use kvproto::cdcpb::{
        create_change_data, ChangeDataClient, ChangeDataEvent, Event, ResolvedTs,
    };
    #[cfg(not(feature = "prost-codec"))]
    use kvproto::cdcpb::{EventEntries, EventRow, Event_oneof_event};

    use crate::channel::{poll_timeout, recv_timeout, CDC_EVENT_MAX_BATCH_SIZE};
    use crate::service::{CdcEvent, EventBatcher, CDC_MAX_RESP_SIZE};

    use super::*;

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
                vec![
                    CdcEvent::Event(event_small.clone()),
                    CdcEvent::Event(event_small),
                ],
                vec![CdcEvent::Event(event_big)],
            ],
        );
    }

    fn new_rpc_suite(capacity: usize) -> (Server, ChangeDataClient, ReceiverWrapper<Task>) {
        let memory_quota = MemoryQuota::new(capacity);
        let (scheduler, rx) = dummy_scheduler();
        let cdc_service = Service::new(scheduler, memory_quota);
        let env = Arc::new(EnvBuilder::new().build());
        let builder =
            ServerBuilder::new(env.clone()).register_service(create_change_data(cdc_service));
        let mut server = builder.bind("127.0.0.1", 0).build().unwrap();
        server.start();
        let (_, port) = server.bind_addrs().next().unwrap();
        let addr = format!("127.0.0.1:{}", port);
        let channel = ChannelBuilder::new(env).connect(&addr);
        let client = ChangeDataClient::new(channel);
        (server, client, rx)
    }

    #[test]
    fn test_flow_control() {
        // Disable CDC sink memory quota.
        let capacity = usize::MAX;
        let (_server, client, mut task_rx) = new_rpc_suite(capacity);
        // Create a event feed stream.
        let (mut tx, mut rx) = client.event_feed().unwrap();
        let mut req = ChangeDataRequest {
            region_id: 1,
            ..Default::default()
        };
        req.mut_header().set_ticdc_version("4.0.7".into());
        block_on(tx.send((req, WriteFlags::default()))).unwrap();
        let task = task_rx.recv_timeout(Duration::from_millis(100)).unwrap();
        let conn = if let Some(Task::OpenConn { conn }) = task {
            conn
        } else {
            panic!("expect to be Task::OpenConn");
        };
        let sink = conn.get_sink().clone();
        // Approximate 1 KB.
        let mut rts = ResolvedTs::default();
        rts.set_regions(vec![u64::MAX; 128]);

        let send = || {
            let rts_ = rts.clone();
            let mut sink_ = sink.clone();
            Box::pin(async move { sink_.send_all(vec![CdcEvent::ResolvedTs(rts_)]).await })
        };
        let must_fill_window = || {
            let mut window_size = 0;
            loop {
                if matches!(
                    poll_timeout(&mut send(), Duration::from_millis(100)),
                    Err(_) | Ok(Err(_))
                ) {
                    // Window is filled and flow control in sink is triggered.
                    break;
                }
                window_size += 1;
                // gRPC window size should not be larger than 1GB.
                assert!(window_size <= 1024 * 1024, "window_size: {}", window_size);
            }
            window_size
        };

        // Fill gRPC window.
        let window_size = must_fill_window();
        assert_ne!(window_size, 0);
        // After receiving a message, sink should be able to send again.
        recv_timeout(&mut rx, Duration::from_millis(100))
            .unwrap()
            .unwrap()
            .unwrap();
        poll_timeout(&mut send(), Duration::from_millis(100))
            .unwrap()
            .unwrap();
        // gRPC client may update window size after receiving a message,
        // though server should not be able to send messages infinitely.
        let window_size = must_fill_window();
        assert_ne!(window_size, 0);
    }
}

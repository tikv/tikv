// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::{stream, Future, Sink, Stream};
use grpcio::*;
use kvproto::cdcpb::{ChangeData, ChangeDataEvent, ChangeDataRequest, Event, ResolvedTs};
use protobuf::Message;
use security::{check_common_name, SecurityManager};
use tikv_util::collections::HashMap;
use tikv_util::mpsc::batch::{self, BatchReceiver, Sender as BatchSender, VecCollector};
use tikv_util::worker::*;

use crate::delegate::{Downstream, DownstreamID};
use crate::endpoint::{Deregister, Task};

static CONNECTION_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

const CDC_MSG_NOTIFY_COUNT: usize = 8;
const CDC_MAX_RESP_SIZE: u32 = 6 * 1024 * 1024; // 6MB
const CDC_MSG_MAX_BATCH_SIZE: usize = 128;

/// A unique identifier of a Connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnID(usize);

impl ConnID {
    pub fn new() -> ConnID {
        ConnID(CONNECTION_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

#[derive(Clone, Debug)]
pub enum CdcEvent {
    ResolvedTs(ResolvedTs),
    Event(Event),
}

impl CdcEvent {
    pub fn size(&self) -> u32 {
        match self {
            CdcEvent::ResolvedTs(_) => 0,
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

struct EventBatcher {
    events: Vec<ChangeDataEvent>,
    last_size: u32,
}

impl EventBatcher {
    pub fn with_capacity(cap: usize) -> EventBatcher {
        EventBatcher {
            events: Vec::with_capacity(cap),
            last_size: 0,
        }
    }

    // The size of the response should not exceed CDC_MAX_RESP_SIZE.
    // Split the events into multiple responses by CDC_MAX_RESP_SIZE here.
    pub fn push(&mut self, event: CdcEvent) {
        let size = event.size();
        if self.events.is_empty() || self.last_size + size >= CDC_MAX_RESP_SIZE {
            self.last_size = 0;
            self.events.push(ChangeDataEvent::default());
        }
        match event {
            CdcEvent::Event(e) => {
                self.last_size += size;
                self.events.last_mut().unwrap().mut_events().push(e);
            }
            CdcEvent::ResolvedTs(r) => {
                let mut change_data_event = ChangeDataEvent::default();
                change_data_event.set_resolved_ts(r);
                self.events.push(change_data_event);
                // Set last_size to MAX-1 for sending resolved ts as an individual event.
                // '-1' is to avoid empty event when the next event is still resolved ts.
                self.last_size = CDC_MAX_RESP_SIZE - 1;
            }
        }
    }

    pub fn build(self) -> Vec<ChangeDataEvent> {
        self.events
            .into_iter()
            .filter(|e| e.has_resolved_ts() || !e.events.is_empty())
            .collect()
    }
}

pub struct Conn {
    id: ConnID,
    sink: BatchSender<CdcEvent>,
    downstreams: HashMap<u64, DownstreamID>,
}

impl Conn {
    pub fn new(sink: BatchSender<CdcEvent>) -> Conn {
        Conn {
            id: ConnID::new(),
            sink,
            downstreams: HashMap::default(),
        }
    }

    pub fn get_id(&self) -> ConnID {
        self.id
    }

    pub fn take_downstreams(self) -> HashMap<u64, DownstreamID> {
        self.downstreams
    }

    pub fn get_sink(&self) -> BatchSender<CdcEvent> {
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
        if !self.sink.is_empty() {
            if let Some(notifier) = self.sink.get_notifier() {
                notifier.notify();
            }
        }
    }
}

/// Service implements the `ChangeData` service.
///
/// It's a front-end of the CDC service, schedules requests to the `Endpoint`.
#[derive(Clone)]
pub struct Service {
    scheduler: Scheduler<Task>,
    security_mgr: Arc<SecurityManager>,
}

impl Service {
    /// Create a ChangeData service.
    ///
    /// It requires a scheduler of an `Endpoint` in order to schedule tasks.
    pub fn new(scheduler: Scheduler<Task>, security_mgr: Arc<SecurityManager>) -> Service {
        Service {
            scheduler,
            security_mgr,
        }
    }
}

impl ChangeData for Service {
    fn event_feed(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<ChangeDataRequest>,
        sink: DuplexSink<ChangeDataEvent>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        // TODO: make it a bounded channel.
        let (tx, rx) = batch::unbounded(CDC_MSG_NOTIFY_COUNT);
        let conn = Conn::new(tx);
        let conn_id = conn.get_id();

        if let Err(status) = self
            .scheduler
            .schedule(Task::OpenConn { conn })
            .map_err(|e| RpcStatus::new(RpcStatusCode::INVALID_ARGUMENT, Some(format!("{:?}", e))))
        {
            error!("cdc connection initiate failed"; "error" => ?status);
            ctx.spawn(sink.fail(status).map_err(|e| {
                error!("cdc failed to send error"; "error" => ?e);
            }));
            return;
        }

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();
        let recv_req = stream.for_each(move |request| {
            let region_epoch = request.get_region_epoch().clone();
            let req_id = request.get_request_id();
            let downstream = Downstream::new(peer.clone(), region_epoch, req_id, conn_id);
            scheduler
                .schedule(Task::Register {
                    request,
                    downstream,
                    conn_id,
                })
                .map_err(|e| {
                    Error::RpcFailure(RpcStatus::new(
                        RpcStatusCode::INVALID_ARGUMENT,
                        Some(format!("{:?}", e)),
                    ))
                })
        });

        let rx = BatchReceiver::new(rx, CDC_MSG_MAX_BATCH_SIZE, Vec::new, VecCollector);
        let send_resp = sink.send_all(
            rx.map(|events| {
                let mut batcher = EventBatcher::with_capacity(events.len());
                events.into_iter().for_each(|e| batcher.push(e));
                let resps = batcher
                    .build()
                    .into_iter()
                    .map(|e| (e, WriteFlags::default()));
                stream::iter_ok(resps)
            })
            .flatten()
            .map_err(|_: ()| {
                Error::RpcFailure(RpcStatus::new(RpcStatusCode::INVALID_ARGUMENT, None))
            }),
        );

        let scheduler = self.scheduler.clone();
        ctx.spawn(recv_req.then(move |res| {
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("cdc deregister failed"; "error" => ?e);
            }
            match res {
                Ok(_s) => {
                    info!("cdc send half closed");
                }
                Err(e) => {
                    error!("cdc send failed"; "error" => ?e);
                }
            }
            Ok(())
        }));

        let scheduler = self.scheduler.clone();
        ctx.spawn(send_resp.then(move |res| {
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("cdc deregister failed"; "error" => ?e);
            }
            match res {
                Ok(_s) => {
                    info!("cdc send half closed");
                }
                Err(e) => {
                    error!("cdc send failed"; "error" => ?e);
                }
            }
            Ok(())
        }));
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

    use crate::service::{CdcEvent, EventBatcher, CDC_MAX_RESP_SIZE};

    #[test]
    fn test_event_batcher() {
        let mut batcher = EventBatcher::with_capacity(1024);

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

        let mut event_small = Event::default();
        let row_small = EventRow::default();
        let mut event_entries = EventEntries::default();
        event_entries.entries = vec![row_small].into();
        event_small.event = Some(Event_oneof_event::Entries(event_entries));

        let mut event_big = Event::default();
        let mut row_big = EventRow::default();
        row_big.set_key(vec![0 as u8; CDC_MAX_RESP_SIZE as usize]);
        let mut event_entries = EventEntries::default();
        event_entries.entries = vec![row_big].into();
        event_big.event = Some(Event_oneof_event::Entries(event_entries));

        batcher.push(CdcEvent::Event(event_small.clone()));
        batcher.push(CdcEvent::ResolvedTs(ResolvedTs::default()));
        batcher.push(CdcEvent::ResolvedTs(ResolvedTs::default()));
        batcher.push(CdcEvent::Event(event_big.clone()));
        batcher.push(CdcEvent::Event(event_small.clone()));
        batcher.push(CdcEvent::Event(event_small.clone()));
        batcher.push(CdcEvent::Event(event_big.clone()));

        check_events(
            batcher.build(),
            vec![
                vec![CdcEvent::Event(event_small.clone())],
                vec![CdcEvent::ResolvedTs(ResolvedTs::default())],
                vec![CdcEvent::ResolvedTs(ResolvedTs::default())],
                vec![CdcEvent::Event(event_big.clone())],
                vec![CdcEvent::Event(event_small); 2],
                vec![CdcEvent::Event(event_big)],
            ],
        );
    }
}

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{Future, Sink, Stream};
use grpcio::*;
use kvproto::cdcpb::{ChangeData, ChangeDataEvent, ChangeDataRequest, Event};
use tikv_util::collections::HashMap;
use tikv_util::mpsc::batch::{self, Sender as BatchSender, SizedBatchReceiver};
use tikv_util::worker::*;

use crate::delegate::{Downstream, DownstreamID};
use crate::endpoint::Task;

static CONNECTION_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

const CDC_MSG_NOTIFY_COUNT: usize = 8;
const CDC_MAX_RESP_SIZE: usize = 6 * 1024 * 1024; // 6MB

/// A unique identifier of a Connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnID(usize);

impl ConnID {
    pub fn new() -> ConnID {
        ConnID(CONNECTION_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

pub struct Conn {
    id: ConnID,
    sink: BatchSender<(usize, Event)>,
    downstreams: HashMap<u64, DownstreamID>,
}

impl Conn {
    pub fn new(sink: BatchSender<(usize, Event)>) -> Conn {
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

    pub fn get_sink(&self) -> BatchSender<(usize, Event)> {
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
}

impl Service {
    /// Create a ChangeData service.
    ///
    /// It requires a scheduler of an `Endpoint` in order to schedule tasks.
    pub fn new(scheduler: Scheduler<Task>) -> Service {
        Service { scheduler }
    }
}

impl ChangeData for Service {
    fn event_feed(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<ChangeDataRequest>,
        sink: DuplexSink<ChangeDataEvent>,
    ) {
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
            let downstream = Downstream::new(peer.clone(), region_epoch);
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

        let rx = SizedBatchReceiver::new(rx, CDC_MAX_RESP_SIZE, Vec::new, |v, e| v.push(e));
        let send_resp = sink.send_all(rx.then(|events| match events {
            Ok(events) => {
                let mut resp = ChangeDataEvent::default();
                resp.set_events(events.into());
                Ok((resp, WriteFlags::default()))
            }
            Err(e) => {
                error!("cdc send failed"; "error" => ?e);
                Err(Error::RpcFailure(RpcStatus::new(
                    RpcStatusCode::UNKNOWN,
                    Some(format!("{:?}", e)),
                )))
            }
        }));

        let scheduler = self.scheduler.clone();
        ctx.spawn(recv_req.then(move |res| {
            // Unregister this downstream only.
            if let Err(e) = scheduler.schedule(Task::Deregister {
                region_id: None,
                downstream_id: None,
                conn_id: Some(conn_id),
                err: None,
            }) {
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
            if let Err(e) = scheduler.schedule(Task::Deregister {
                region_id: None,
                downstream_id: None,
                conn_id: Some(conn_id),
                err: None,
            }) {
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

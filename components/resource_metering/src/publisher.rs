// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::{IGNORED_DATA_COUNTER, REPORT_DATA_COUNTER, REPORT_DURATION_HISTOGRAM};
use crate::reporter::ClientRegistry;
use crate::{Client, Records};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::channel::mpsc::{channel, Sender};
use futures::{SinkExt, StreamExt};
use grpcio::{RpcContext, ServerStreamingSink, WriteFlags};
use kvproto::resource_usage_agent::{ResourceMeteringRequest, ResourceUsageRecord};
use kvproto::resource_usage_agent_grpc::ResourceMeteringPubSub;
use tikv_util::{defer, warn};

/// `ResourceMeteringPublisher` implements [ResourceMeteringPubSub].
///
/// If a client subscribes to resource metering records, the `ResourceMeteringPublisher`
/// is responsible for registering them to the reporter. Then the reporter sends data to
/// the client periodically.
///
/// [ResourceMeteringPubSub]: kvproto::resource_usage_agent_grpc::ResourceMeteringPubSub
#[derive(Clone)]
pub struct ResourceMeteringPublisher {
    client_registry: ClientRegistry,
}

impl ResourceMeteringPublisher {
    pub fn new(client_registry: ClientRegistry) -> Self {
        Self { client_registry }
    }
}

impl ResourceMeteringPubSub for ResourceMeteringPublisher {
    fn subscribe(
        &mut self,
        ctx: RpcContext,
        _: ResourceMeteringRequest,
        mut sink: ServerStreamingSink<ResourceUsageRecord>,
    ) {
        // The `tx` is for the reporter and the `rx` is for the gRPC stream sender.
        //
        // The reporter calls `tx.try_send` roughly every minute. If the the gRPC
        // stream sender does not send data out over the network in time, it discards
        // the incoming records to prevent memory overflow.
        let (tx, mut rx) = channel(1);

        // If there is an issue with the RPC, e.g. the connection is disconnected,
        // the routine that drives the gRPC stream sender will exit but the `tx`
        // installed on the reporter will leak.
        //
        // The AtomicBool `is_closed` is used to notify the reporter that the
        // subscription is finished and can perform a cleanup.
        let is_closed = Arc::new(AtomicBool::new(false));
        let client = PubClient::new(tx, is_closed.clone());
        self.client_registry.register(Box::new(client));

        let report_task = async move {
            defer! {{ is_closed.store(true, Ordering::SeqCst);}}
            loop {
                let records = rx.next().await;
                if records.is_none() {
                    break;
                }

                let _t = REPORT_DURATION_HISTOGRAM.start_timer();
                let records = records.unwrap();
                for (tag, record) in &records.records {
                    let mut req = ResourceUsageRecord::default();
                    req.set_resource_group_tag(tag.clone());
                    req.set_record_list_timestamp_sec(record.timestamps.clone());
                    req.set_record_list_cpu_time_ms(record.cpu_time_list.clone());
                    let resp = sink.send((req, WriteFlags::default())).await;
                    if let Err(err) = resp {
                        warn!("failed to send records"; "error" => ?err);
                        return;
                    };
                    REPORT_DATA_COUNTER.with_label_values(&["sent"]).inc();
                }
                let others = &records.others;
                if !others.is_empty() {
                    let mut req = ResourceUsageRecord::default();
                    req.set_record_list_timestamp_sec(others.keys().copied().collect());
                    req.set_record_list_cpu_time_ms(others.values().map(|r| r.cpu_time).collect());
                    let resp = sink.send((req, WriteFlags::default())).await;
                    if let Err(err) = resp {
                        warn!("failed to send records"; "error" => ?err);
                        return;
                    }
                    REPORT_DATA_COUNTER.with_label_values(&["sent"]).inc();
                }
            }
        };

        ctx.spawn(report_task);
    }
}

struct PubClient {
    tx: Sender<Arc<Records>>,
    is_closed: Arc<AtomicBool>,
}

impl PubClient {
    fn new(tx: Sender<Arc<Records>>, is_closed: Arc<AtomicBool>) -> Self {
        Self { tx, is_closed }
    }
}

impl Client for PubClient {
    fn upload_records(&mut self, records: Arc<Records>) {
        if self.tx.try_send(records).is_err() {
            IGNORED_DATA_COUNTER.with_label_values(&["report"]).inc();
        }
    }

    fn is_down(&self) -> bool {
        self.is_closed.load(Ordering::SeqCst)
    }
}

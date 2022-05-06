// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::{
    channel::mpsc::{channel, Sender},
    SinkExt, StreamExt,
};
use grpcio::{RpcContext, ServerStreamingSink, WriteFlags};
use kvproto::{
    resource_usage_agent::{ResourceMeteringRequest, ResourceUsageRecord},
    resource_usage_agent_grpc::ResourceMeteringPubSub,
};
use tikv_util::{info, warn};

use super::DataSinkRegHandle;
use crate::{
    error::Result,
    metrics::{IGNORED_DATA_COUNTER, REPORT_DATA_COUNTER, REPORT_DURATION_HISTOGRAM},
    DataSink,
};

/// `PubSubService` implements [ResourceMeteringPubSub].
///
/// If a client subscribes to resource metering records, the `PubSubService` is responsible for
/// registering them to the reporter. Then the reporter sends data to the client periodically.
///
/// [ResourceMeteringPubSub]: kvproto::resource_usage_agent_grpc::ResourceMeteringPubSub
#[derive(Clone)]
pub struct PubSubService {
    data_sink_reg_handle: DataSinkRegHandle,
}

impl PubSubService {
    pub fn new(data_sink_reg_handle: DataSinkRegHandle) -> Self {
        Self {
            data_sink_reg_handle,
        }
    }
}

impl ResourceMeteringPubSub for PubSubService {
    fn subscribe(
        &mut self,
        ctx: RpcContext<'_>,
        _: ResourceMeteringRequest,
        mut sink: ServerStreamingSink<ResourceUsageRecord>,
    ) {
        info!("accept a new subscriber"; "from" => ?ctx.peer());

        // The `tx` is for the reporter and the `rx` is for the gRPC stream sender.
        //
        // The reporter calls `tx.try_send` roughly every minute. If the the gRPC
        // stream sender does not send data out over the network in time, it discards
        // the incoming records to prevent memory overflow.
        let (tx, mut rx) = channel(1);

        let data_sink = DataSinkImpl { tx };
        let handle = self.data_sink_reg_handle.register(Box::new(data_sink));

        let report_task = async move {
            let _h = handle;

            loop {
                let records = rx.next().await;
                if records.is_none() {
                    break;
                }

                let _t = REPORT_DURATION_HISTOGRAM.start_timer();
                let records = records.unwrap();
                REPORT_DATA_COUNTER
                    .with_label_values(&["to_send"])
                    .inc_by(records.len() as _);
                for record in records.iter() {
                    if let Err(err) = sink.send((record.clone(), WriteFlags::default())).await {
                        warn!("failed to send records to the pubsub subscriber"; "error" => ?err);
                        return;
                    }
                    REPORT_DATA_COUNTER.with_label_values(&["sent"]).inc();
                }
            }
        };

        ctx.spawn(report_task);
    }
}

/// A [DataSink] implementation for scheduling [Task::Records].
struct DataSinkImpl {
    tx: Sender<Arc<Vec<ResourceUsageRecord>>>,
}

impl DataSink for DataSinkImpl {
    fn try_send(&mut self, records: Arc<Vec<ResourceUsageRecord>>) -> Result<()> {
        let record_cnt = records.len();
        if self.tx.try_send(records).is_err() {
            IGNORED_DATA_COUNTER
                .with_label_values(&["report"])
                .inc_by(record_cnt as _);
            Err("failed to schedule records to pubsub datasink".into())
        } else {
            Ok(())
        }
    }
}

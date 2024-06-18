// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::{
    channel::mpsc::{channel, Sender},
    stream, StreamExt,
};
use kvproto::{
    resource_usage_agent::{ResourceMeteringRequest, ResourceUsageRecord},
    resource_usage_agent_grpc::resource_metering_pub_sub_server::ResourceMeteringPubSub,
};
use tikv_util::{info, stream::GuardedStream};
use tonic::{self, codegen::BoxStream};

use super::DataSinkRegHandle;
use crate::{
    error::Result,
    metrics::{IGNORED_DATA_COUNTER, REPORT_DATA_COUNTER, REPORT_DURATION_HISTOGRAM},
    DataSink,
};

/// `PubSubService` implements [ResourceMeteringPubSub].
///
/// If a client subscribes to resource metering records, the `PubSubService` is
/// responsible for registering them to the reporter. Then the reporter sends
/// data to the client periodically.
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

#[tonic::async_trait]
impl ResourceMeteringPubSub for PubSubService {
    type SubscribeStream = tonic::codegen::BoxStream<ResourceUsageRecord>;
    async fn subscribe(
        &self,
        _request: tonic::Request<ResourceMeteringRequest>,
    ) -> std::result::Result<
        tonic::Response<tonic::codegen::BoxStream<ResourceUsageRecord>>,
        tonic::Status,
    > {
        info!("accept a new subscriber"; "from" => /* TODO ?ctx.peer()*/ "unknown");

        // The `tx` is for the reporter and the `rx` is for the gRPC stream sender.
        //
        // The reporter calls `tx.try_send` roughly every minute. If the the gRPC
        // stream sender does not send data out over the network in time, it discards
        // the incoming records to prevent memory overflow.
        let (tx, rx) = channel(1);
        let data_sink = DataSinkImpl { tx };
        let handle = self.data_sink_reg_handle.register(Box::new(data_sink));

        let stream = rx.flat_map(|r: Arc<Vec<ResourceUsageRecord>>| {
            let _t = REPORT_DURATION_HISTOGRAM.start_timer();
            let records = r.as_ref().clone();
            REPORT_DATA_COUNTER
                .with_label_values(&["to_send"])
                .inc_by(records.len() as _);
            stream::iter(records).map(|r| {
                REPORT_DATA_COUNTER.with_label_values(&["sent"]).inc();
                Ok(r)
            })
        });

        Ok(tonic::Response::new(
            Box::pin(GuardedStream::new(handle, stream)) as BoxStream<ResourceUsageRecord>,
        ))
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

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use futures::channel::mpsc;
use kvproto::{logbackup_grpc::log_backup_server::LogBackup, logbackuppb::*, metapb::Region};
use tikv_util::{warn, worker::Scheduler};

use crate::{
    checkpoint_manager::{GetCheckpointResult, RegionIdWithVersion},
    endpoint::{RegionCheckpointOperation, RegionSet},
    try_send, Task,
};

#[derive(Clone)]
pub struct Service {
    endpoint: Scheduler<Task>,
}

impl Service {
    pub fn new(endpoint: Scheduler<Task>) -> Self {
        Self { endpoint }
    }
}

fn id_of(region: &Region) -> RegionIdentity {
    let mut id = RegionIdentity::default();
    id.set_id(region.get_id());
    id.set_epoch_version(region.get_region_epoch().get_version());
    id
}

impl From<RegionIdWithVersion> for RegionIdentity {
    fn from(val: RegionIdWithVersion) -> Self {
        let mut id = RegionIdentity::default();
        id.set_id(val.region_id);
        id.set_epoch_version(val.region_epoch_version);
        id
    }
}

#[tonic::async_trait]
impl LogBackup for Service {
    async fn get_last_flush_ts_of_region(
        &self,
        request: tonic::Request<GetLastFlushTsOfRegionRequest>,
    ) -> tonic::Result<tonic::Response<GetLastFlushTsOfRegionResponse>> {
        let mut req = request.into_inner();
        let regions = req
            .take_regions()
            .into_iter()
            .map(|id| (id.id, id.epoch_version))
            .collect::<HashSet<_>>();
        let (tx, rx) = futures::channel::oneshot::channel();
        let t = Task::RegionCheckpointsOp(RegionCheckpointOperation::Get(
            RegionSet::Regions(regions),
            Box::new(move |rs| {
                let mut resp = GetLastFlushTsOfRegionResponse::new();
                resp.set_checkpoints(
                    rs.into_iter()
                        .map(|r| match r {
                            GetCheckpointResult::Ok { region, checkpoint } => {
                                let mut r = RegionCheckpoint::new();
                                let id = id_of(&region);
                                r.set_region(id);
                                r.set_checkpoint(checkpoint.into_inner());
                                r
                            }
                            GetCheckpointResult::NotFound { id, err } => {
                                let mut r = RegionCheckpoint::new();
                                r.set_region(id.into());
                                r.set_err(err);
                                r
                            }
                            GetCheckpointResult::EpochNotMatch { region, err } => {
                                let mut r = RegionCheckpoint::new();
                                r.set_region(id_of(&region));
                                r.set_err(err);
                                r
                            }
                        })
                        .collect(),
                );
                let _ = tx.send(resp);
            }),
        ));
        if let Err(e) = self.endpoint.schedule(t) {
            return Err(tonic::Status::unknown(format!("{:?}", e)));
        }

        match rx.await {
            Ok(resp) => Ok(tonic::Response::new(resp)),
            Err(e) => Err(tonic::Status::aborted(format!("{:?}", e))),
        }
    }
    /// Server streaming response type for the SubscribeFlushEvent method.
    type SubscribeFlushEventStream = tonic::codegen::BoxStream<SubscribeFlushEventResponse>;
    async fn subscribe_flush_event(
        &self,
        request: tonic::Request<SubscribeFlushEventRequest>,
    ) -> tonic::Result<tonic::Response<Self::SubscribeFlushEventStream>> {
        #[cfg(test)]
        panic!("Service should not be used in an unit test");
        #[cfg(not(test))]
        {
            let (tx, rx) = mpsc::unbounded();
            if !try_send!(
                self.endpoint,
                Task::RegionCheckpointsOp(RegionCheckpointOperation::Subscribe(tx))
            ) {
                return Err(tonic::Status::aborted("send RegionCheckpointsOp failed"));
            }
            Ok(tonic::Response::new(Box::pin(rx) as _))
        }
    }
}

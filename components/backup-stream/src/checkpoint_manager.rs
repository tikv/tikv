// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc, time::Duration};

use kvproto::{
    errorpb::{Error as PbError, *},
    metapb::Region,
};
use pd_client::PdClient;
use tikv_util::{info, worker::Scheduler};
use txn_types::TimeStamp;

use crate::{
    errors::{Error, Result},
    metadata::{store::MetaStore, Checkpoint, CheckpointProvider, MetadataClient},
    metrics, try_send, RegionCheckpointOperation, Task,
};

/// A manager for maintaining the last flush ts.
/// This information is provided for the `advancer` in checkpoint V3,
/// which involved a central node (typically TiDB) for collecting all regions'
/// checkpoint then advancing the global checkpoint.
#[derive(Debug, Default)]
pub struct CheckpointManager {
    items: HashMap<u64, LastFlushTsOfRegion>,
}

/// The result of getting a checkpoint.
/// The possibility of failed to getting checkpoint is pretty high:
/// because there is a gap between region leader change and flushing.
#[derive(Debug)]
pub enum GetCheckpointResult {
    Ok {
        region: Region,
        checkpoint: TimeStamp,
    },
    NotFound {
        id: RegionIdWithVersion,
        err: PbError,
    },
    EpochNotMatch {
        region: Region,
        err: PbError,
    },
}

impl GetCheckpointResult {
    /// create an "ok" variant with region.
    pub fn ok(region: Region, checkpoint: TimeStamp) -> Self {
        Self::Ok { region, checkpoint }
    }

    fn not_found(id: RegionIdWithVersion) -> Self {
        Self::NotFound {
            id,
            err: not_leader(id.region_id),
        }
    }

    /// create a epoch not match variant with region
    fn epoch_not_match(provided: RegionIdWithVersion, real: &Region) -> Self {
        Self::EpochNotMatch {
            region: real.clone(),
            err: epoch_not_match(
                provided.region_id,
                provided.region_epoch_version,
                real.get_region_epoch().get_version(),
            ),
        }
    }
}

impl CheckpointManager {
    /// clear the manager.
    pub fn clear(&mut self) {
        self.items.clear();
    }

    /// update a region checkpoint in need.
    pub fn update_region_checkpoint(&mut self, region: &Region, checkpoint: TimeStamp) {
        let e = self.items.entry(region.get_id());
        e.and_modify(|old_cp| {
            if old_cp.checkpoint < checkpoint
                && old_cp.region.get_region_epoch().get_version()
                    <= region.get_region_epoch().get_version()
            {
                *old_cp = LastFlushTsOfRegion {
                    checkpoint,
                    region: region.clone(),
                };
            }
        })
        .or_insert_with(|| LastFlushTsOfRegion {
            checkpoint,
            region: region.clone(),
        });
    }

    /// get checkpoint from a region.
    pub fn get_from_region(&self, region: RegionIdWithVersion) -> GetCheckpointResult {
        let checkpoint = self.items.get(&region.region_id);
        if checkpoint.is_none() {
            return GetCheckpointResult::not_found(region);
        }
        let checkpoint = checkpoint.unwrap();
        if checkpoint.region.get_region_epoch().get_version() != region.region_epoch_version {
            return GetCheckpointResult::epoch_not_match(region, &checkpoint.region);
        }
        GetCheckpointResult::ok(checkpoint.region.clone(), checkpoint.checkpoint)
    }

    /// get all checkpoints stored.
    pub fn get_all(&self) -> Vec<LastFlushTsOfRegion> {
        self.items.values().cloned().collect()
    }
}

fn not_leader(r: u64) -> PbError {
    let mut err = PbError::new();
    let mut nl = NotLeader::new();
    nl.set_region_id(r);
    err.set_not_leader(nl);
    err.set_message(
        format!("the region {} isn't in the region_manager of log backup, maybe not leader or not flushed yet.", r));
    err
}

fn epoch_not_match(id: u64, sent: u64, real: u64) -> PbError {
    let mut err = PbError::new();
    let en = EpochNotMatch::new();
    err.set_epoch_not_match(en);
    err.set_message(format!(
        "the region {} has recorded version {}, but you sent {}",
        id, real, sent,
    ));
    err
}

#[derive(Debug, PartialEq, Hash, Clone, Copy)]
/// A simple region id, but versioned.
pub struct RegionIdWithVersion {
    pub region_id: u64,
    pub region_epoch_version: u64,
}

impl RegionIdWithVersion {
    pub fn new(id: u64, version: u64) -> Self {
        Self {
            region_id: id,
            region_epoch_version: version,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LastFlushTsOfRegion {
    pub region: Region,
    pub checkpoint: TimeStamp,
}

// Allow some type to
#[async_trait::async_trait]
pub trait FlushObserver: Send + 'static {
    /// The callback when the flush has advanced the resolver.
    async fn before(&mut self, checkpoints: Vec<(Region, TimeStamp)>);
    /// The callback when the flush is done. (Files are fully written to
    /// external storage.)
    async fn after(&mut self, task: &str, rts: u64) -> Result<()>;
    /// The optional callback to rewrite the resolved ts of this flush.
    /// Because the default method (collect all leader resolved ts in the store,
    /// and use the minimal TS.) may lead to resolved ts rolling back, if we
    /// desire a stronger consistency, we can rewrite a safer resolved ts here.
    /// Note the new resolved ts cannot be greater than the old resolved ts.
    async fn rewrite_resolved_ts(
        &mut self,
        #[allow(unused_variables)] task: &str,
    ) -> Option<TimeStamp> {
        None
    }
}

pub struct BasicFlushObserver<PD> {
    pd_cli: Arc<PD>,
    store_id: u64,
}

impl<PD> BasicFlushObserver<PD> {
    pub fn new(pd_cli: Arc<PD>, store_id: u64) -> Self {
        Self { pd_cli, store_id }
    }
}

#[async_trait::async_trait]
impl<PD: PdClient + 'static> FlushObserver for BasicFlushObserver<PD> {
    async fn before(&mut self, _checkpoints: Vec<(Region, TimeStamp)>) {}

    async fn after(&mut self, task: &str, rts: u64) -> Result<()> {
        if let Err(err) = self
            .pd_cli
            .update_service_safe_point(
                format!("backup-stream-{}-{}", task, self.store_id),
                TimeStamp::new(rts),
                // Add a service safe point for 30 mins (6x the default flush interval).
                // It would probably be safe.
                Duration::from_secs(1800),
            )
            .await
        {
            Error::from(err).report("failed to update service safe point!");
            // don't give up?
        }

        // Currently, we only support one task at the same time,
        // so use the task as label would be ok.
        metrics::STORE_CHECKPOINT_TS
            .with_label_values(&[task])
            .set(rts as _);
        Ok(())
    }
}

pub struct CheckpointV3FlushObserver<S, O> {
    /// We should modify the rts (the local rts isn't right.)
    /// This should be a BasicFlushObserver or something likewise.
    baseline: O,
    sched: Scheduler<Task>,
    meta_cli: MetadataClient<S>,

    checkpoints: Vec<(Region, TimeStamp)>,
    global_checkpoint_cache: HashMap<String, Checkpoint>,
}

impl<S, O> CheckpointV3FlushObserver<S, O> {
    pub fn new(sched: Scheduler<Task>, meta_cli: MetadataClient<S>, baseline: O) -> Self {
        Self {
            sched,
            meta_cli,
            checkpoints: vec![],
            // We almost always have only one entry.
            global_checkpoint_cache: HashMap::with_capacity(1),
            baseline,
        }
    }
}

impl<S, O> CheckpointV3FlushObserver<S, O>
where
    S: MetaStore + 'static,
    O: FlushObserver + Send,
{
    async fn get_checkpoint(&mut self, task: &str) -> Result<Checkpoint> {
        let cp = match self.global_checkpoint_cache.get(task) {
            Some(cp) => *cp,
            None => {
                let global_checkpoint = self.meta_cli.global_checkpoint_of_task(task).await?;
                self.global_checkpoint_cache
                    .insert(task.to_owned(), global_checkpoint);
                global_checkpoint
            }
        };
        Ok(cp)
    }
}

#[async_trait::async_trait]
impl<S, O> FlushObserver for CheckpointV3FlushObserver<S, O>
where
    S: MetaStore + 'static,
    O: FlushObserver + Send,
{
    async fn before(&mut self, checkpoints: Vec<(Region, TimeStamp)>) {
        self.checkpoints = checkpoints;
    }

    async fn after(&mut self, task: &str, _rts: u64) -> Result<()> {
        let t = Task::RegionCheckpointsOp(RegionCheckpointOperation::Update(std::mem::take(
            &mut self.checkpoints,
        )));
        try_send!(self.sched, t);
        let global_checkpoint = self.get_checkpoint(task).await?;
        info!("getting global checkpoint from cache for updating."; "checkpoint" => ?global_checkpoint);
        self.baseline
            .after(task, global_checkpoint.ts.into_inner())
            .await?;
        Ok(())
    }

    async fn rewrite_resolved_ts(&mut self, task: &str) -> Option<TimeStamp> {
        let global_checkpoint = self
            .get_checkpoint(task)
            .await
            .map_err(|err| err.report("failed to get resolved ts for rewriting"))
            .ok()?;
        info!("getting global checkpoint for updating."; "checkpoint" => ?global_checkpoint);
        matches!(global_checkpoint.provider, CheckpointProvider::Global)
            .then(|| global_checkpoint.ts)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use kvproto::metapb::*;
    use txn_types::TimeStamp;

    use super::RegionIdWithVersion;
    use crate::GetCheckpointResult;

    fn region(id: u64, version: u64, conf_version: u64) -> Region {
        let mut r = Region::new();
        let mut e = RegionEpoch::new();
        e.set_version(version);
        e.set_conf_ver(conf_version);
        r.set_id(id);
        r.set_region_epoch(e);
        r
    }

    #[test]
    fn test_mgr() {
        let mut mgr = super::CheckpointManager::default();
        mgr.update_region_checkpoint(&region(1, 32, 8), TimeStamp::new(8));
        mgr.update_region_checkpoint(&region(2, 34, 8), TimeStamp::new(15));
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 32));
        assert_matches::assert_matches!(r, GetCheckpointResult::Ok{checkpoint, ..} if checkpoint.into_inner() == 8);
        let r = mgr.get_from_region(RegionIdWithVersion::new(2, 33));
        assert_matches::assert_matches!(r, GetCheckpointResult::EpochNotMatch { .. });
        let r = mgr.get_from_region(RegionIdWithVersion::new(3, 44));
        assert_matches::assert_matches!(r, GetCheckpointResult::NotFound { .. });
        mgr.update_region_checkpoint(&region(1, 30, 8), TimeStamp::new(16));
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 32));
        assert_matches::assert_matches!(r, GetCheckpointResult::Ok{checkpoint, ..} if checkpoint.into_inner() == 8);

        mgr.update_region_checkpoint(&region(1, 30, 8), TimeStamp::new(16));
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 32));
        assert_matches::assert_matches!(r, GetCheckpointResult::Ok{checkpoint, ..} if checkpoint.into_inner() == 8);
        mgr.update_region_checkpoint(&region(1, 32, 8), TimeStamp::new(16));
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 32));
        assert_matches::assert_matches!(r, GetCheckpointResult::Ok{checkpoint, ..} if checkpoint.into_inner() == 16);
        mgr.update_region_checkpoint(&region(1, 33, 8), TimeStamp::new(24));
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 33));
        assert_matches::assert_matches!(r, GetCheckpointResult::Ok{checkpoint, ..} if checkpoint.into_inner() == 24);
    }
}

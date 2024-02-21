// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::RefCell, error::Error as StdError, result, thread::JoinHandle};

use engine_traits::{Engines, KvEngine, RaftEngine, CF_RAFT};
use futures::channel::mpsc::UnboundedSender;
use kvproto::{
    raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState},
    recoverdatapb::*,
};
use thiserror::Error;
use tikv_util::sys::thread::StdThreadBuildWrapper;

use crate::metrics::REGION_EVENT_COUNTER;

pub type Result<T> = result::Result<T, Error>;

#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument {0:?}")]
    InvalidArgument(String),

    #[error("Not Found {0:?}")]
    NotFound(String),

    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

/// `RegionMetaCollector` is the collector that collector all region meta
pub struct RegionMetaCollector<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    /// The engine we are working on
    engines: Engines<EK, ER>,
    /// region meta report to br
    tx: UnboundedSender<RegionMeta>,
    /// Current working workers
    worker_handle: RefCell<Option<JoinHandle<()>>>,
}

#[allow(dead_code)]
impl<EK, ER> RegionMetaCollector<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(engines: Engines<EK, ER>, tx: UnboundedSender<RegionMeta>) -> Self {
        RegionMetaCollector {
            engines,
            tx,
            worker_handle: RefCell::new(None),
        }
    }
    /// Start a collector and region meta report.
    pub fn start_report(&self) {
        let worker = CollectWorker::new(self.engines.clone(), self.tx.clone());
        let props = tikv_util::thread_group::current_properties();
        *self.worker_handle.borrow_mut() = Some(
            std::thread::Builder::new()
                .name("collector_region_meta".to_string())
                .spawn_wrapper(move || {
                    tikv_util::thread_group::set_properties(props);

                    worker
                        .collect_report()
                        .expect("collect region meta and report to br failure.");
                })
                .expect("failed to spawn collector_region_meta thread"),
        );
    }

    // join and wait until the thread exit
    pub fn wait(&self) {
        if let Err(e) = self.worker_handle.take().unwrap().join() {
            error!("failed to join thread: {:?}", e);
        }
    }
}

struct CollectWorker<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    /// The engine we are working on
    engines: Engines<EK, ER>,
    tx: UnboundedSender<RegionMeta>,
}

impl<EK, ER> CollectWorker<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(engines: Engines<EK, ER>, tx: UnboundedSender<RegionMeta>) -> Self {
        CollectWorker { engines, tx }
    }

    fn get_local_region(&self, region_id: u64) -> Result<LocalRegion> {
        let raft_state = box_try!(self.engines.raft.get_raft_state(region_id));

        let apply_state_key = keys::apply_state_key(region_id);
        let apply_state = box_try!(
            self.engines
                .kv
                .get_msg_cf::<RaftApplyState>(CF_RAFT, &apply_state_key)
        );

        let region_state_key = keys::region_state_key(region_id);
        let region_state = box_try!(
            self.engines
                .kv
                .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
        );

        match (raft_state, apply_state, region_state) {
            (None, None, None) => Err(Error::NotFound(format!("info for region {}", region_id))),
            (raft_state, apply_state, region_state) => {
                Ok(LocalRegion::new(raft_state, apply_state, region_state))
            }
        }
    }

    /// collect all region and report to br
    pub fn collect_report(&self) -> Result<bool> {
        let db = &self.engines.kv;
        let cf = CF_RAFT;
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let mut regions = Vec::with_capacity(1024);
        box_try!(db.scan(cf, start_key, end_key, false, |key, _| {
            let (id, suffix) = box_try!(keys::decode_region_meta_key(key));
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }
            regions.push(id);
            Ok(true)
        }));

        for region_id in regions {
            let region_state = self.get_local_region(region_id)?;

            // It's safe to unwrap region_local_state here, since region_id  guarantees that
            // the region state exists
            if region_state.region_local_state.as_ref().unwrap().state == PeerState::Tombstone {
                continue;
            }

            region_state.raft_local_state.as_ref().ok_or_else(|| {
                Error::Other(format!("No RaftLocalState found for region {}", region_id).into())
            })?;
            region_state.raft_apply_state.as_ref().ok_or_else(|| {
                Error::Other(format!("No RaftApplyState found for region {}", region_id).into())
            })?;

            // send to br
            let response = region_state.to_region_meta();

            REGION_EVENT_COUNTER.collect_meta.inc();
            if let Err(e) = self.tx.unbounded_send(response) {
                warn!("send the region meta failure";
                "err" => ?e);
                if e.is_disconnected() {
                    warn!("channel is disconnected.");
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}

#[derive(PartialEq, Debug, Default)]
pub struct LocalRegion {
    pub raft_local_state: Option<RaftLocalState>,
    pub raft_apply_state: Option<RaftApplyState>,
    pub region_local_state: Option<RegionLocalState>,
}

impl LocalRegion {
    fn new(
        raft_local: Option<RaftLocalState>,
        raft_apply: Option<RaftApplyState>,
        region_local: Option<RegionLocalState>,
    ) -> Self {
        LocalRegion {
            raft_local_state: raft_local,
            raft_apply_state: raft_apply,
            region_local_state: region_local,
        }
    }

    // fetch local region info into a gRPC message structure RegionMeta
    fn to_region_meta(&self) -> RegionMeta {
        let mut region_meta = RegionMeta::default();
        region_meta.region_id = self.region_local_state.as_ref().unwrap().get_region().id;
        region_meta.peer_id = self
            .region_local_state
            .as_ref()
            .unwrap()
            .get_region()
            .get_peers()
            .to_vec()
            .iter()
            .max_by_key(|p| p.id)
            .unwrap()
            .get_id();
        region_meta.version = self
            .region_local_state
            .as_ref()
            .unwrap()
            .get_region()
            .get_region_epoch()
            .version;
        region_meta.tombstone =
            self.region_local_state.as_ref().unwrap().state == PeerState::Tombstone;
        region_meta.start_key = self
            .region_local_state
            .as_ref()
            .unwrap()
            .get_region()
            .get_start_key()
            .to_owned();
        region_meta.end_key = self
            .region_local_state
            .as_ref()
            .unwrap()
            .get_region()
            .get_end_key()
            .to_owned();
        region_meta.last_log_term = self
            .raft_local_state
            .as_ref()
            .unwrap()
            .get_hard_state()
            .term;
        region_meta.last_index = self.raft_local_state.as_ref().unwrap().last_index;

        region_meta
    }
}

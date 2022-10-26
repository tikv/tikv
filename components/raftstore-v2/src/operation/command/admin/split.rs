// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::VecDeque, io, sync::Arc};

use batch_system::BasicMailbox;
use collections::{HashMap, HashMapEntry, HashSet};
use crossbeam::channel::internal::SelectHandle;
use engine_traits::{
    CfOptions, DeleteStrategy, KvEngine, MiscExt, OpenOptions, RaftEngine, RaftEngineReadOnly,
    RaftLogBatch, Range, TabletFactory, CF_DEFAULT, DATA_CFS,
};
use keys::enc_end_key;
use kvproto::{
    metapb::{self, Region},
    raft_cmdpb::{AdminRequest, AdminResponse},
    raft_serverpb::{RaftMessage, RegionLocalState},
};
use raft::{eraftpb::Message, prelude::MessageType, RawNode};
use raftstore::{
    coprocessor::RegionChangeReason,
    store::{
        fsm::apply::{
            self, amend_new_split_regions, init_split_regions, validate_and_get_split_keys,
            ApplyResult, NewSplitPeer,
        },
        metrics::PEER_ADMIN_CMD_COUNTER,
        util::{self, KeysInfoFormatter},
        InspectedRaftMessage, PdTask, PeerPessimisticLocks, PeerStat, ReadDelegate, ReadProgress,
        TrackVer, Transport, RAFT_INIT_LOG_INDEX,
    },
    Result,
};
use slog::{error, info, warn};
use tikv_util::box_err;
use time::Timespec;

use crate::{
    batch::StoreContext,
    fsm::{PeerFsm, PeerFsmDelegate},
    operation::AdminCmdResult,
    raft::{raft_config, write_initial_states, write_peer_state, Apply, Peer, Storage},
    router::{message::PeerCreation, ApplyRes, ExecResult, PeerMsg, StoreMsg},
};

fn create_checkpoint(
    src_tablet_dir: &std::path::Path,
    target_tablet_dir: &std::path::Path,
) -> io::Result<()> {
    std::fs::create_dir_all(target_tablet_dir)?;
    for file in std::fs::read_dir(src_tablet_dir)? {
        let file = file?;
        std::fs::hard_link(file.path(), target_tablet_dir.join(file.file_name()))?;
    }

    Ok(())
}

#[derive(Debug)]
pub struct SplitResult {
    pub regions: Vec<Region>,
    pub derived: Region,
    pub tablet_index: u64,
    pub auto_compactions: HashMap<&'static str, bool>,
}

#[derive(Debug)]
pub struct SplitRegionInitInfo {
    pub source_state: RegionLocalState,
    pub region: metapb::Region,
    pub parent_is_leader: bool,
    pub parent_stat: PeerStat,
    pub approximate_size: Option<u64>,
    pub approximate_keys: Option<u64>,
    pub locks: PeerPessimisticLocks,
    pub last_split_region: bool,
}

#[derive(Debug)]
pub struct SplitRegionInitResp {
    pub parent_state: RegionLocalState,
    pub child_region_id: u64,
    pub result: bool,
}

pub enum AcrossPeerMsg {
    SplitRegionInit(Box<SplitRegionInitInfo>),
    SplitRegionInitResp(Box<SplitRegionInitResp>),
}

impl<EK: KvEngine, ER: RaftEngine, R> Apply<EK, ER, R> {
    pub fn exec_batch_split(
        &mut self,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        PEER_ADMIN_CMD_COUNTER.batch_split.all.inc();

        let split_reqs = req.get_splits();
        let mut derived = self.region_state().get_region().clone();
        let region_id = derived.id;

        let mut split_keys =
            validate_and_get_split_keys(split_reqs, self.region_state().get_region())?;

        info!(
            self.logger,
            "split region";
            "region_id" => region_id,
            "peer_id" => self.peer().id,
            "region" => ?derived,
            "keys" => %KeysInfoFormatter(split_keys.iter()),
        );

        let (regions, _) =
            init_split_regions(self.store_id, split_reqs, &mut derived, &mut split_keys);

        // todo(SpadeA): Here: we use a temporary solution that we disable auto
        // compaction and use checkpoint API to clone new tablets. It may cause large
        // jitter as checkpoint with log_size_for_flush being 0 will flush the memtable.
        // We will freeze the memtable rather than flush it in the following PR.
        let region_id = derived.get_id();
        let tablet = self.tablet().unwrap();
        let mut auto_compactions = HashMap::default();
        for &cf in DATA_CFS {
            let mut cf_option = tablet.get_options_cf(cf).unwrap();
            auto_compactions.insert(cf, cf_option.get_disable_auto_compactions());
            tablet
                .set_options_cf(cf, &[("disable_auto_compactions", "true")])
                .unwrap();
        }
        let current_tablet_path = std::path::Path::new(tablet.path());

        for new_region in &regions {
            let new_region_id = new_region.id;
            if new_region_id == region_id {
                continue;
            }

            let new_tablet_path = self
                .tablet_factory
                .split_tablet_path(new_region_id, RAFT_INIT_LOG_INDEX);
            tablet
                .create_checkpoint(&new_tablet_path)
                .unwrap_or_else(|e| {
                    panic!(
                        "{:?} fails to create checkpoint with path {:?}: {:?}",
                        self.logger.list(),
                        new_tablet_path,
                        e
                    )
                });
        }

        let new_tablet_path = self.tablet_factory.split_tablet_path(region_id, log_index);
        // Change the tablet path by using the new tablet suffix
        tablet
            .create_checkpoint(&new_tablet_path)
            .unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to create checkpoint with path {:?}: {:?}",
                    self.logger.list(),
                    new_tablet_path,
                    e
                )
            });
        let tablet = self
            .tablet_factory
            .open_tablet(
                region_id,
                Some(log_index),
                OpenOptions::default().set_create(true).set_split_use(true),
            )
            .unwrap();
        self.publish_tablet(tablet);

        // Clear the write batch belonging to the old tablet
        self.clear_write_batch();

        self.region_state_mut().set_region(derived.clone());
        self.region_state_mut().set_tablet_index(log_index);
        // self.metrics.size_diff_hint = 0;
        // self.metrics.delete_keys_hint = 0;

        let mut resp = AdminResponse::default();
        resp.mut_splits().set_regions(regions.clone().into());
        PEER_ADMIN_CMD_COUNTER.batch_split.success.inc();

        Ok((
            resp,
            AdminCmdResult::SplitRegion(SplitResult {
                regions,
                derived,
                tablet_index: log_index,
                auto_compactions,
            }),
        ))
    }
}

fn get_range_not_in_region(region: &metapb::Region) -> Vec<Range<'_>> {
    let mut ranges = Vec::new();
    if !region.get_start_key().is_empty() {
        ranges.push(Range::new(b"", region.get_start_key()));
    }
    if !region.get_end_key().is_empty() {
        ranges.push(Range::new(region.get_end_key(), b""));
    }
    ranges
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn init_split_region<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        init_info: Box<SplitRegionInitInfo>,
    ) {
        let SplitRegionInitInfo {
            source_state,
            region,
            parent_is_leader,
            parent_stat,
            approximate_keys,
            approximate_size,
            locks,
            last_split_region,
        } = Box::into_inner(init_info);
        // todo: check if it is inited
        let region_id = region.id;

        if !self.storage().is_initialized() {
            let mut wb = store_ctx.engine.log_batch(5);
            write_initial_states(&mut wb, region.clone()).unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to save split region {:?}: {:?}",
                    self.logger.list(),
                    region,
                    e
                )
            });

            store_ctx.engine.consume(&mut wb, true).unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to consume the write: {:?}",
                    self.logger.list(),
                    e,
                )
            });

            let split_tablet_path = store_ctx
                .tablet_factory
                .split_tablet_path(region_id, RAFT_INIT_LOG_INDEX);
            println!("{:?}", split_tablet_path);
            let tablet_path = store_ctx
                .tablet_factory
                .tablet_path(region_id, RAFT_INIT_LOG_INDEX);
            std::fs::rename(&split_tablet_path, &tablet_path).unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to rename from tablet path {:?} to normal path {:?} :{:?}",
                    self.logger.list(),
                    split_tablet_path,
                    tablet_path,
                    e
                )
            });

            let storage = Storage::new(
                region_id,
                store_ctx.store_id,
                store_ctx.engine.clone(),
                store_ctx.log_fetch_scheduler.clone(),
                &store_ctx.logger,
            )
            .unwrap_or_else(|e| panic!("fail to create storage: {:?}", e))
            .unwrap();

            let applied_index = storage.apply_state().get_applied_index();
            let peer_id = storage.peer().get_id();
            let raft_cfg = raft_config(peer_id, applied_index, &store_ctx.cfg);

            let mut raft_group = RawNode::new(&raft_cfg, storage, &self.logger).unwrap();
            // If this region has only one peer and I am the one, campaign directly.
            if region.get_peers().len() == 1 {
                raft_group.campaign().unwrap();
                self.set_has_ready();
            }
            // FIXME: unwrap
            self.set_raft_group(raft_group);
        } else {
        }

        let mut meta = store_ctx.store_meta.lock().unwrap();

        info!(
            self.logger,
            "init split region";
            "region_id" => region_id,
            "region" => ?region,
        );

        // todo(SpadeA)
        // let mut replication_state =
        // self.ctx.global_replication_state.lock().unwrap(); new_peer.peer.
        // init_replication_mode(&mut replication_state);
        // drop(replication_state);

        for p in region.get_peers() {
            self.insert_peer_cache(p.clone());
        }

        // New peer derive write flow from parent region,
        // this will be used by balance write flow.
        self.set_peer_stat(parent_stat);
        let last_compacted_idx = self
            .storage()
            .apply_state()
            .get_truncated_state()
            .get_index()
            + 1;
        self.set_last_compacted_idx(last_compacted_idx);

        let campaigned = self.maybe_campaign(parent_is_leader);
        if campaigned {
            self.set_has_ready();
        }

        if parent_is_leader {
            self.set_approximate_size(approximate_size);
            self.set_approximate_keys(approximate_keys);
            *self.txn_ext().pessimistic_locks.write() = locks;
            // The new peer is likely to become leader, send a heartbeat immediately to
            // reduce client query miss.
            self.heartbeat_pd(store_ctx);
        }

        let mut tablet = store_ctx
            .tablet_factory
            .open_tablet(
                region_id,
                Some(RAFT_INIT_LOG_INDEX),
                OpenOptions::default().set_create(true),
            )
            .unwrap();
        self.tablet_mut().set(tablet.clone());

        meta.tablet_caches.insert(region_id, self.tablet().clone());
        meta.regions.insert(region_id, region.clone());
        let not_exist = meta
            .region_ranges
            .insert(enc_end_key(&region), region_id)
            .is_none();
        assert!(not_exist, "[region {}] should not exist", region_id);
        meta.readers
            .insert(region_id, self.generate_read_delegate());
        meta.region_read_progress
            .insert(region_id, self.read_progress().clone());

        if !campaigned {
            if let Some(msg) = meta
                .pending_msgs
                .swap_remove_front(|m| m.get_to_peer() == self.peer())
            {
                let peer_msg = PeerMsg::RaftMessage(Box::new(msg));
                if let Err(e) = store_ctx.router.force_send(region_id, peer_msg) {
                    warn!(self.logger, "handle first requset failed"; "region_id" => region_id, "error" => ?e);
                }
            }
        }

        drop(meta);

        let ranges_to_delete = get_range_not_in_region(&region);
        // todo: need to do it asynchrounously?
        tablet
            .delete_ranges_cfs(DeleteStrategy::DeleteFiles, &ranges_to_delete)
            .unwrap_or_else(|e| {
                error!(self.logger,"failed to delete files in range"; "err" => %e);
            });

        if last_split_region {
            // todo: cfg validation should be called to make
            // region_split_check_diff be Some(x)

            // To prevent from big region, the right region needs run split
            // check again after split.
            // new_peer
            //     .peer_mut()
            //     .set_size_diff_hint(store_ctx.cfg.
            // region_split_check_diff().0);
        }

        store_ctx.router.force_send(
            source_state.get_region().id,
            PeerMsg::AcrossPeerMsg(AcrossPeerMsg::SplitRegionInitResp(Box::new(
                SplitRegionInitResp {
                    parent_state: source_state,
                    child_region_id: region_id,
                    result: true,
                },
            ))),
        );
    }

    pub fn handle_peer_split_response<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        resp: Box<SplitRegionInitResp>,
    ) {
        let SplitRegionInitResp {
            parent_state,
            child_region_id,
            result,
        } = Box::into_inner(resp);

        assert_eq!(parent_state, *self.storage().region_state());

        let mut split_progress = self.split_progress_mut();
        *split_progress.get_mut(&child_region_id).unwrap() = true;

        if split_progress.values().all(|v| *v) {
            // Split can be finished
            let mut wb = store_ctx.engine.log_batch(5);
            let state = self.storage().region_state();
            wb.put_region_state(self.region_id(), state)
                .unwrap_or_else(|e| {
                    panic!(
                        "{:?} fails to update region {:?}: {:?}",
                        self.logger.list(),
                        state,
                        e
                    )
                });
            store_ctx.engine.consume(&mut wb, true).unwrap();

            let split_tablet_path = store_ctx
                .tablet_factory
                .split_tablet_path(self.region_id(), self.storage().region_state().tablet_index);
            let tablet_path = store_ctx
                .tablet_factory
                .tablet_path(self.region_id(), self.storage().region_state().tablet_index);
            std::fs::rename(&split_tablet_path, &tablet_path).unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to rename from tablet path {:?} to normal path {:?} :{:?}",
                    self.logger.list(),
                    split_tablet_path,
                    tablet_path,
                    e
                )
            });
        }
    }

    pub fn on_ready_split_region<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        derived: Region,
        tablet_index: u64,
        regions: Vec<Region>,
        auto_compactions: HashMap<&'static str, bool>,
    ) {
        let region_id = derived.get_id();

        // Group in-memory pessimistic locks in the original region into new regions.
        // The locks of new regions will be put into the corresponding new regions
        // later. And the locks belonging to the old region will stay in the original
        // map.
        let region_locks = {
            let mut pessimistic_locks = self.txn_ext().pessimistic_locks.write();
            info!(self.logger, "moving {} locks to new regions", pessimistic_locks.len(); "region_id"=> region_id);
            // Update the version so the concurrent reader will fail due to EpochNotMatch
            // instead of PessimisticLockNotFound.
            pessimistic_locks.version = derived.get_region_epoch().get_version();
            pessimistic_locks.group_by_regions(&regions, &derived)
        };

        // Roughly estimate the size and keys for new regions.
        let new_region_count = regions.len() as u64;
        let estimated_size = self.approximate_size().map(|v| v / new_region_count);
        let estimated_keys = self.approximate_keys().map(|v| v / new_region_count);
        let mut meta = store_ctx.store_meta.lock().unwrap();
        meta.set_region(derived, self, RegionChangeReason::Split, tablet_index);
        self.post_split();

        // It's not correct anymore, so set it to false to schedule a split
        // check task.
        self.set_may_skip_split_check(false);

        let is_leader = self.is_leader();
        if is_leader {
            self.set_approximate_size(estimated_size);
            self.set_approximate_keys(estimated_keys);
            self.heartbeat_pd(store_ctx);

            info!(
                self.logger,
                "notify pd with split";
                "region_id" => self.region_id(),
                "peer_id" => self.peer_id(),
                "split_count" => regions.len(),
            );

            // todo: report to PD
        }

        self.split_progress_mut().clear();
        let last_key = enc_end_key(regions.last().unwrap());
        if meta.region_ranges.remove(&last_key).is_none() {
            panic!("{:?} original region should exist", self.logger.list());
        }
        let last_region_id = regions.last().unwrap().get_id();
        for (new_region, locks) in regions.into_iter().zip(region_locks) {
            let new_region_id = new_region.get_id();

            if new_region_id == region_id {
                let not_exist = meta
                    .region_ranges
                    .insert(enc_end_key(&new_region), new_region_id)
                    .is_none();
                assert!(not_exist, "[region {}] should not exist", new_region_id);

                // recover the auto_compaction
                let tablet = self.tablet_mut().latest().unwrap().clone();
                let ranges_to_delete = get_range_not_in_region(&new_region);

                // todo: async version
                tablet
                    .delete_ranges_cfs(DeleteStrategy::DeleteFiles, &ranges_to_delete)
                    .unwrap_or_else(|e| {
                        error!(self.logger,"failed to delete files in range"; "err" => %e);
                    });

                continue;
            }

            let mut raft_message = self.prepare_raft_message();
            raft_message.set_region_id(new_region_id);
            raft_message.set_to_peer(
                new_region
                    .get_peers()
                    .iter()
                    .find(|p| p.store_id == store_ctx.store_id)
                    .unwrap()
                    .clone(),
            );

            self.split_progress_mut().insert(new_region_id, false);
            let init_info = SplitRegionInitInfo {
                region: new_region,
                source_state: self.storage().region_state().clone(),
                parent_is_leader: self.is_leader(),
                parent_stat: self.peer_stat().clone(),
                approximate_keys: estimated_keys,
                approximate_size: estimated_size,
                locks,
                last_split_region: last_region_id == new_region_id,
            };

            store_ctx
                .router
                .send_control(StoreMsg::PeerCreation(PeerCreation {
                    raft_message: Box::new(raft_message),
                    split_region_info: Box::new(init_info),
                }));
        }
        drop(meta);

        if is_leader {
            self.on_split_region_check_tick();
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc::{channel, Receiver, Sender};

    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::TestTabletFactoryV2,
        raft,
    };
    use engine_traits::{CfOptionsExt, ALL_CFS};
    use futures::channel::mpsc::unbounded;
    use kvproto::{
        raft_cmdpb::{BatchSplitRequest, RaftCmdResponse, SplitRequest},
        raft_serverpb::{PeerState, RaftApplyState, RegionLocalState},
    };
    use raftstore::store::{cmd_resp::new_error, Config};
    use slog::o;
    use tempfile::TempDir;
    use tikv_util::{
        config::VersionTrack,
        store::{new_learner_peer, new_peer},
        worker::{FutureScheduler, Scheduler},
    };

    use super::*;
    use crate::{
        fsm::{ApplyFsm, ApplyResReporter},
        raft::Apply,
        tablet::CachedTablet,
    };

    struct MockReporter {
        sender: Sender<ApplyRes>,
    }

    impl MockReporter {
        fn new() -> (Self, Receiver<ApplyRes>) {
            let (tx, rx) = channel();
            (MockReporter { sender: tx }, rx)
        }
    }

    impl ApplyResReporter for MockReporter {
        fn report(&self, apply_res: ApplyRes) {
            self.sender.send(apply_res).unwrap();
        }
    }

    fn new_split_req(key: &[u8], id: u64, children: Vec<u64>) -> SplitRequest {
        let mut req = SplitRequest::default();
        req.set_split_key(key.to_vec());
        req.set_new_region_id(id);
        req.set_new_peer_ids(children);
        req
    }

    fn assert_split(
        apply: &mut Apply<engine_test::kv::KvTestEngine, raft::RaftTestEngine, MockReporter>,
        factory: &Arc<TestTabletFactoryV2>,
        region_to_split: &Region,
        get_region_local_state: &dyn Fn(u64) -> RegionLocalState,
        get_raft_apply_state: &dyn Fn(u64) -> RaftApplyState,
        right_derived: bool,
        new_region_ids: Vec<u64>,
        split_keys: Vec<Vec<u8>>,
        children_peers: Vec<Vec<u64>>,
        log_index: u64,
    ) -> HashMap<u64, Region> {
        let mut splits = BatchSplitRequest::default();
        splits.set_right_derive(right_derived);
        let mut region_boundries = split_keys.clone();
        region_boundries.insert(0, region_to_split.get_start_key().to_vec());
        region_boundries.push(region_to_split.get_end_key().to_vec());

        for ((new_region_id, children), split_key) in new_region_ids
            .into_iter()
            .zip(children_peers.clone())
            .zip(split_keys)
        {
            splits
                .mut_requests()
                .push(new_split_req(&split_key, new_region_id, children));
        }

        let mut req = AdminRequest::default();
        req.set_splits(splits);

        // Exec batch split
        let (resp, apply_res) = apply.exec_batch_split(&req, log_index).unwrap();
        apply.flush();

        // for cf in DATA_CFS {
        //     let opt = apply.tablet().unwrap().get_options_cf(cf).unwrap();
        //     assert!(opt.get_disable_auto_compactions());
        // }
        let regions = resp.get_splits().get_regions();
        assert!(regions.len() == region_boundries.len() - 1);

        let mut epoch = region_to_split.get_region_epoch().clone();
        epoch.version += children_peers.len() as u64;

        let mut child_idx = 0;
        let mut region_state = RegionLocalState::default();
        for (i, region) in regions.iter().enumerate() {
            let state = get_region_local_state(region.id);
            assert_eq!(state.get_state(), PeerState::Normal);
            assert_eq!(state.get_region().get_id(), region.id);
            assert_eq!(
                state.get_region().get_start_key().to_vec(),
                region_boundries[i]
            );
            assert_eq!(
                state.get_region().get_end_key().to_vec(),
                region_boundries[i + 1]
            );
            assert_eq!(*state.get_region().get_region_epoch(), epoch);

            if region.id == region_to_split.id {
                assert_eq! {
                    state.get_region().get_peers(),
                    region_to_split.get_peers()
                }
                assert_eq!(state, *apply.region_state());
                assert_eq!(state.tablet_index, log_index);
                // assert we can read the tablet by the new tablet_index
                let tablet_path = factory.tablet_path(region_to_split.id, log_index);
                assert!(factory.exists_raw(&tablet_path));

                region_state = state.clone();
            } else {
                assert_eq! {
                    state.get_region().get_peers().iter().map(|peer| peer.id).collect::<Vec<_>>(),
                    children_peers[child_idx]
                }
                child_idx += 1;

                assert_eq!(state.tablet_index, RAFT_INIT_LOG_INDEX);
                let tablet_path = factory.tablet_path(region.id, RAFT_INIT_LOG_INDEX);
                assert!(factory.exists_raw(&tablet_path));

                let initial_state = get_raft_apply_state(region.id);
                assert_eq!(initial_state.get_applied_index(), RAFT_INIT_LOG_INDEX);
                assert_eq!(
                    initial_state.get_truncated_state().get_index(),
                    RAFT_INIT_LOG_INDEX
                );
                assert_eq!(
                    initial_state.get_truncated_state().get_term(),
                    RAFT_INIT_LOG_INDEX
                );
            }
        }

        regions
            .iter()
            .map(|region| (region.id, region.clone()))
            .collect()
    }

    #[test]
    fn test_split() {
        let store_id = 2;

        let mut region = Region::default();
        region.set_id(1);
        region.set_end_key(b"k10".to_vec());
        region.mut_region_epoch().set_version(3);
        let peers = vec![new_peer(2, 3), new_peer(4, 5), new_learner_peer(6, 7)];
        region.set_peers(peers.into());

        let logger = slog_global::borrow_global().new(o!());
        let path = TempDir::new().unwrap();
        let cf_opts = ALL_CFS
            .iter()
            .copied()
            .map(|cf| (cf, CfOptions::default()))
            .collect();
        let factory = Arc::new(TestTabletFactoryV2::new(
            path.path(),
            DbOptions::default(),
            cf_opts,
        ));
        let raft_engine =
            raft::new_engine(&format!("{}", path.path().join("raft").display()), None).unwrap();

        let tablet = factory
            .open_tablet(
                region.id,
                Some(5),
                OpenOptions::default().set_create_new(true),
            )
            .unwrap();

        let mut region_state = RegionLocalState::default();
        region_state.set_state(PeerState::Normal);
        region_state.set_region(region.clone());
        region_state.set_tablet_index(5);

        let (reporter, rx) = MockReporter::new();
        let mut apply = Apply::new(
            store_id,
            region
                .get_peers()
                .iter()
                .find(|p| p.store_id == store_id)
                .unwrap()
                .clone(),
            region_state,
            reporter,
            CachedTablet::new(Some(tablet)),
            raft_engine.clone(),
            factory.clone(),
            Arc::default(),
            logger.clone(),
        );

        let mut splits = BatchSplitRequest::default();
        splits.set_right_derive(true);
        splits.mut_requests().push(new_split_req(b"k1", 1, vec![]));
        let mut req = AdminRequest::default();
        req.set_splits(splits.clone());
        let err = apply.exec_batch_split(&req, 0).unwrap_err();
        // 3 followers are required.
        assert!(err.to_string().contains("invalid new peer id count"));

        splits.mut_requests().clear();
        req.set_splits(splits.clone());
        let err = apply.exec_batch_split(&req, 0).unwrap_err();
        // Empty requests should be rejected.
        assert!(err.to_string().contains("missing split requests"));

        splits
            .mut_requests()
            .push(new_split_req(b"k11", 1, vec![11, 12, 13]));
        req.set_splits(splits.clone());
        let resp = new_error(apply.exec_batch_split(&req, 0).unwrap_err());
        // Out of range keys should be rejected.
        assert!(
            resp.get_header().get_error().has_key_not_in_region(),
            "{:?}",
            resp
        );

        splits
            .mut_requests()
            .push(new_split_req(b"", 1, vec![11, 12, 13]));
        req.set_splits(splits.clone());
        let err = apply.exec_batch_split(&req, 0).unwrap_err();
        // Empty key should be rejected.
        assert!(err.to_string().contains("missing split key"), "{:?}", err);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 1, vec![11, 12, 13]));
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 1, vec![11, 12, 13]));
        req.set_splits(splits.clone());
        let err = apply.exec_batch_split(&req, 0).unwrap_err();
        // keys should be in ascend order.
        assert!(
            err.to_string().contains("invalid split request"),
            "{:?}",
            err
        );

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 1, vec![11, 12, 13]));
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 1, vec![11, 12]));
        req.set_splits(splits.clone());
        let err = apply.exec_batch_split(&req, 0).unwrap_err();
        // All requests should be checked.
        assert!(err.to_string().contains("id count"), "{:?}", err);

        let get_region_local_state = |region_id| -> RegionLocalState {
            raft_engine.get_region_state(region_id).unwrap().unwrap()
        };

        let get_raft_apply_state = |region_id| -> RaftApplyState {
            raft_engine.get_apply_state(region_id).unwrap().unwrap()
        };

        let mut log_index = 10;
        // After split: region 1 ["", "k09"], region 10 ["k09", "k10"]
        let regions = assert_split(
            &mut apply,
            &factory,
            &region,
            &get_region_local_state,
            &get_raft_apply_state,
            false,
            vec![10],
            vec![b"k09".to_vec()],
            vec![vec![11, 12, 13]],
            log_index,
        );
        rx.recv().unwrap();

        log_index = 20;
        // After split: region 20 ["", "k01"], region 1 ["k01", "k09"]
        let regions = assert_split(
            &mut apply,
            &factory,
            regions.get(&1).unwrap(),
            &get_region_local_state,
            &get_raft_apply_state,
            true,
            vec![20],
            vec![b"k01".to_vec()],
            vec![vec![21, 22, 23]],
            log_index,
        );
        rx.recv().unwrap();

        log_index = 30;
        // After split: region 30 ["k01", "k02"], region 40 ["k02", "k03"],
        //              region 1 ["k03", "k09"]
        let regions = assert_split(
            &mut apply,
            &factory,
            regions.get(&1).unwrap(),
            &get_region_local_state,
            &get_raft_apply_state,
            true,
            vec![30, 40],
            vec![b"k02".to_vec(), b"k03".to_vec()],
            vec![vec![31, 32, 33], vec![41, 42, 43]],
            log_index,
        );
        rx.recv().unwrap();

        // After split: region 50 ["k07", "k08"], region 60 ["k08", "k09"],
        //              region 1 ["k03", "k07"]
        log_index = 40;
        let regions = assert_split(
            &mut apply,
            &factory,
            regions.get(&1).unwrap(),
            &get_region_local_state,
            &get_raft_apply_state,
            false,
            vec![50, 60],
            vec![b"k07".to_vec(), b"k08".to_vec()],
            vec![vec![51, 52, 53], vec![61, 62, 63]],
            log_index,
        );
        rx.recv().unwrap();
    }
}

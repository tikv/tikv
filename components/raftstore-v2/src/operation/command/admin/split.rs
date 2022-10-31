// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::VecDeque, io, sync::Arc};

use batch_system::BasicMailbox;
use collections::{HashMap, HashMapEntry, HashSet};
use crossbeam::channel::internal::SelectHandle;
use engine_traits::{
    CfOptions, DeleteStrategy, KvEngine, MiscExt, OpenOptions, RaftEngine, RaftEngineReadOnly,
    RaftLogBatch, Range, TabletFactory, CF_DEFAULT, DATA_CFS, SPLIT_PREFIX,
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
        fsm::apply::{self, validate_and_get_split_keys, ApplyResult, NewSplitPeer},
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
    raft::{write_initial_states, Apply, Peer, Storage},
    router::{ApplyRes, PeerMsg, StoreMsg},
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
}

impl<EK: KvEngine, R> Apply<EK, R> {
    pub fn exec_batch_split(
        &mut self,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        PEER_ADMIN_CMD_COUNTER.batch_split.all.inc();

        let split_reqs = req.get_splits();
        let mut derived = self.region_state().get_region().clone();
        let region_id = derived.id;

        let mut keys = validate_and_get_split_keys(split_reqs, self.region_state().get_region())?;

        info!(
            self.logger,
            "split region";
            "region_id" => region_id,
            "peer_id" => self.peer().id,
            "region" => ?derived,
            "keys" => %KeysInfoFormatter(keys.iter()),
        );

        let new_region_cnt = split_reqs.get_requests().len();
        let new_version = derived.get_region_epoch().get_version() + new_region_cnt as u64;
        derived.mut_region_epoch().set_version(new_version);

        let right_derive = split_reqs.get_right_derive();
        let mut regions = Vec::with_capacity(new_region_cnt + 1);
        // Note that the split requests only contain ids for new regions, so we need
        // to handle new regions and old region separately.
        if right_derive {
            // So the range of new regions is [old_start_key, split_key1, ...,
            // last_split_key].
            keys.push_front(derived.get_start_key().to_vec());
        } else {
            // So the range of new regions is [split_key1, ..., last_split_key,
            // old_end_key].
            keys.push_back(derived.get_end_key().to_vec());
            derived.set_end_key(keys.front().unwrap().to_vec());
            regions.push(derived.clone());
        }

        // Init split regions' meta info
        for req in split_reqs.get_requests() {
            let mut new_region = Region::default();
            new_region.set_id(req.get_new_region_id());
            new_region.set_region_epoch(derived.get_region_epoch().to_owned());
            new_region.set_start_key(keys.pop_front().unwrap());
            new_region.set_end_key(keys.front().unwrap().to_vec());
            new_region.set_peers(derived.get_peers().to_vec().into());
            for (peer, peer_id) in new_region
                .mut_peers()
                .iter_mut()
                .zip(req.get_new_peer_ids())
            {
                peer.set_id(*peer_id);
            }
            regions.push(new_region);
        }

        if right_derive {
            derived.set_start_key(keys.pop_front().unwrap());
            regions.push(derived.clone());
        }

        // todo(SpadeA): Here: we use a temporary solution that we use checkpoint API to
        // clone new tablets. It may cause large jitter as we need to flush the
        // memtable. We will freeze the memtable rather than flush it in the
        // following PR.
        let region_id = derived.get_id();
        let tablet = self.tablet().unwrap();

        for new_region in &regions {
            let new_region_id = new_region.id;
            if new_region_id == region_id {
                continue;
            }

            let split_temp_path = self.tablet_factory().tablet_path_with_prefix(
                SPLIT_PREFIX,
                new_region_id,
                RAFT_INIT_LOG_INDEX,
            );
            tablet
                .create_checkpoint(&split_temp_path)
                .unwrap_or_else(|e| {
                    panic!(
                        "{:?} fails to create checkpoint with path {:?}: {:?}",
                        self.logger.list(),
                        split_temp_path,
                        e
                    )
                });
        }

        let derived_temp_path =
            self.tablet_factory()
                .tablet_path_with_prefix(SPLIT_PREFIX, region_id, log_index);
        // Change the tablet path by using the new tablet suffix
        tablet
            .create_checkpoint(&derived_temp_path)
            .unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to create checkpoint with path {:?}: {:?}",
                    self.logger.list(),
                    derived_temp_path,
                    e
                )
            });
        // Here, we open the tablet without registering as it is the split version. We
        // will register it when switching it to the normal version.
        let tablet = self
            .tablet_factory()
            .open_tablet_raw(
                &derived_temp_path,
                region_id,
                log_index,
                OpenOptions::default(),
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
            }),
        ))
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
        apply: &mut Apply<engine_test::kv::KvTestEngine, MockReporter>,
        factory: &Arc<TestTabletFactoryV2>,
        region_to_split: &Region,
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

        let regions = resp.get_splits().get_regions();
        assert!(regions.len() == region_boundries.len() - 1);

        let mut epoch = region_to_split.get_region_epoch().clone();
        epoch.version += children_peers.len() as u64;

        let mut child_idx = 0;
        for (i, region) in regions.iter().enumerate() {
            assert_eq!(region.get_start_key().to_vec(), region_boundries[i]);
            assert_eq!(region.get_end_key().to_vec(), region_boundries[i + 1]);
            assert_eq!(*region.get_region_epoch(), epoch);

            if region.id == region_to_split.id {
                let state = apply.region_state();
                assert_eq!(state.tablet_index, log_index);
                assert_eq!(state.get_region(), region);
                let tablet_path =
                    factory.tablet_path_with_prefix(SPLIT_PREFIX, region.id, log_index);
                assert!(factory.exists_raw(&tablet_path));
            } else {
                assert_eq! {
                    region.get_peers().iter().map(|peer| peer.id).collect::<Vec<_>>(),
                    children_peers[child_idx]
                }
                child_idx += 1;

                let tablet_path =
                    factory.tablet_path_with_prefix(SPLIT_PREFIX, region.id, RAFT_INIT_LOG_INDEX);
                assert!(factory.exists_raw(&tablet_path));
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

        let (reporter, _) = MockReporter::new();
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
            factory.clone(),
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

        let mut log_index = 10;
        // After split: region 1 ["", "k09"], region 10 ["k09", "k10"]
        let regions = assert_split(
            &mut apply,
            &factory,
            &region,
            false,
            vec![10],
            vec![b"k09".to_vec()],
            vec![vec![11, 12, 13]],
            log_index,
        );

        log_index = 20;
        // After split: region 20 ["", "k01"], region 1 ["k01", "k09"]
        let regions = assert_split(
            &mut apply,
            &factory,
            regions.get(&1).unwrap(),
            true,
            vec![20],
            vec![b"k01".to_vec()],
            vec![vec![21, 22, 23]],
            log_index,
        );

        log_index = 30;
        // After split: region 30 ["k01", "k02"], region 40 ["k02", "k03"],
        //              region 1 ["k03", "k09"]
        let regions = assert_split(
            &mut apply,
            &factory,
            regions.get(&1).unwrap(),
            true,
            vec![30, 40],
            vec![b"k02".to_vec(), b"k03".to_vec()],
            vec![vec![31, 32, 33], vec![41, 42, 43]],
            log_index,
        );

        // After split: region 50 ["k07", "k08"], region 60 ["k08", "k09"],
        //              region 1 ["k03", "k07"]
        log_index = 40;
        let regions = assert_split(
            &mut apply,
            &factory,
            regions.get(&1).unwrap(),
            false,
            vec![50, 60],
            vec![b"k07".to_vec(), b"k08".to_vec()],
            vec![vec![51, 52, 53], vec![61, 62, 63]],
            log_index,
        );
    }
}

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    Checkpointer, KvEngine, OpenOptions, RaftEngine, TabletFactory, CF_DEFAULT, SPLIT_PREFIX,
};
use itertools::Itertools;
use kvproto::{
    metapb::Region,
    raft_cmdpb::{AdminRequest, AdminResponse, RaftCmdRequest, SplitRequest},
    raft_serverpb::RegionLocalState,
};
use protobuf::Message;
use raftstore::{
    coprocessor::split_observer::{is_valid_split_key, strip_timestamp_if_exists},
    store::{
        fsm::apply::extract_split_keys,
        metrics::PEER_ADMIN_CMD_COUNTER,
        util::{self, KeysInfoFormatter},
        PeerStat, ProposalContext, RAFT_INIT_LOG_INDEX,
    },
    Result,
};
use slog::{info, warn, Logger};
use tikv_util::box_err;

use crate::{
    batch::StoreContext,
    operation::AdminCmdResult,
    raft::{Apply, Peer},
    router::ApplyRes,
};

#[derive(Debug)]
pub struct SplitResult {
    pub regions: Vec<Region>,
    pub derived: Region,
    pub tablet_index: u64,
}

pub fn validate_batch_split(
    logger: &Logger,
    req: &mut AdminRequest,
    region: &Region,
) -> Result<()> {
    if !req.has_splits() {
        return Err(box_err!(
            "cmd_type is BatchSplit but it doesn't have splits request, message maybe \
             corrupted!"
                .to_owned()
        ));
    }
    let mut split_reqs: Vec<SplitRequest> = req.mut_splits().take_requests().into();
    let split_reqs = split_reqs
        .into_iter()
        .enumerate()
        .filter_map(|(i, mut split)| {
            let key = split.take_split_key();
            let key = strip_timestamp_if_exists(key);
            if is_valid_split_key(&key, i, region) {
                split.split_key = key;
                Some(split)
            } else {
                None
            }
        })
        .coalesce(|prev, curr| {
            // Make sure that the split keys are sorted and unique.
            if prev.split_key < curr.split_key {
                Err((prev, curr))
            } else {
                warn!(
                    logger,
                    "skip invalid split key: key should not be larger than the previous.";
                    "key" => log_wrappers::Value::key(&curr.split_key),
                    "previous" => log_wrappers::Value::key(&prev.split_key),
                );
                Ok(prev)
            }
        })
        .collect::<Vec<_>>();

    if split_reqs.is_empty() {
        Err(box_err!("no valid key found for split.".to_owned()))
    } else {
        req.mut_splits().set_requests(split_reqs.into());
        Ok(())
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn propose_split<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
    ) -> Result<u64> {
        validate_batch_split(&self.logger, req.mut_admin_request(), self.region())?;
        let mut proposal_ctx = ProposalContext::empty();
        proposal_ctx.insert(ProposalContext::SYNC_LOG);
        proposal_ctx.insert(ProposalContext::SPLIT);

        let data = req.write_to_bytes().unwrap();
        self.propose_with_proposal_ctx(store_ctx, data, proposal_ctx.to_vec())
    }
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

        let mut keys = extract_split_keys(split_reqs, self.region_state().get_region())?;

        info!(
            self.logger,
            "split region";
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

        // We will create checkpoint of the current tablet for both derived region and
        // split regions. Before the creation, we should flush the writes and remove the
        // write batch
        self.flush_write();
        self.write_batch_mut().take();

        // todo(SpadeA): Here: we use a temporary solution that we use checkpoint API to
        // clone new tablets. It may cause large jitter as we need to flush the
        // memtable. We will freeze the memtable rather than flush it in the
        // following PR.
        let region_id = derived.get_id();
        let tablet = self.tablet().clone();
        let mut checkpointer = tablet.new_checkpointer().unwrap_or_else(|e| {
            panic!(
                "{:?} fails to create checkpoint object: {:?}",
                self.logger.list(),
                e
            )
        });

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
            checkpointer
                .create_at(&split_temp_path, None, 0)
                .unwrap_or_else(|e| {
                    panic!(
                        "{:?} fails to create checkpoint with path {:?}: {:?}",
                        self.logger.list(),
                        split_temp_path,
                        e
                    )
                });
        }

        let derived_path = self.tablet_factory().tablet_path(region_id, log_index);
        checkpointer
            .create_at(&derived_path, None, 0)
            .unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to create checkpoint with path {:?}: {:?}",
                    self.logger.list(),
                    derived_path,
                    e
                )
            });
        let tablet = self
            .tablet_factory()
            .open_tablet(region_id, Some(log_index), OpenOptions::default())
            .unwrap();
        self.publish_tablet(tablet);

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
    use std::sync::{
        mpsc::{channel, Receiver, Sender},
        Arc,
    };

    use byteorder::{BigEndian, WriteBytesExt};
    use collections::HashMap;
    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::TestTabletFactoryV2,
        raft,
    };
    use engine_traits::{CfOptionsExt, Peekable, WriteBatch, ALL_CFS};
    use futures::channel::mpsc::unbounded;
    use kvproto::{
        raft_cmdpb::{AdminCmdType, BatchSplitRequest, PutRequest, RaftCmdResponse, SplitRequest},
        raft_serverpb::{PeerState, RaftApplyState, RegionLocalState},
    };
    use raftstore::store::{cmd_resp::new_error, Config};
    use slog::o;
    use tempfile::TempDir;
    use tidb_query_datatype::{
        codec::{datum, table, Datum},
        expr::EvalContext,
    };
    use tikv_util::{
        codec::bytes::encode_bytes,
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
                let tablet_path = factory.tablet_path(region.id, log_index);
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

        // After split: region 1 ["k03", "k07"], region 50 ["k07", "k08"],
        //              region 60["k08", "k09"]
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

        // Split will checkpoint tablet, so if there are some writes before split, they
        // should be flushed immediately.
        apply.apply_put(CF_DEFAULT, b"k04", b"v4").unwrap();
        assert!(!apply.write_batch_mut().as_ref().unwrap().is_empty());
        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k05", 70, vec![71, 72, 73]));
        req.set_splits(splits);
        apply.exec_batch_split(&req, 50).unwrap();
        assert!(apply.write_batch_mut().is_none());
        assert_eq!(apply.tablet().get_value(b"k04").unwrap().unwrap(), b"v4");
    }

    fn new_row_key(table_id: i64, row_id: i64, version_id: u64) -> Vec<u8> {
        let mut key = table::encode_row_key(table_id, row_id);
        key = encode_bytes(&key);
        key.write_u64::<BigEndian>(version_id).unwrap();
        key
    }

    fn new_index_key(table_id: i64, idx_id: i64, datums: &[Datum], version_id: u64) -> Vec<u8> {
        let mut key = table::encode_index_seek_key(
            table_id,
            idx_id,
            &datum::encode_key(&mut EvalContext::default(), datums).unwrap(),
        );
        key = encode_bytes(&key);
        key.write_u64::<BigEndian>(version_id).unwrap();
        key
    }

    fn new_batch_split_request(keys: Vec<Vec<u8>>) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::BatchSplit);
        for key in keys {
            let mut split_req = SplitRequest::default();
            split_req.set_split_key(key);
            req.mut_splits().mut_requests().push(split_req);
        }
        req
    }

    #[test]
    fn test_validate_batch_split() {
        let mut region = Region::default();
        let start_key = new_row_key(1, 1, 1);
        region.set_start_key(start_key.clone());
        let logger = slog_global::borrow_global().new(o!());

        let mut req = AdminRequest::default();
        // default admin request should be rejected
        assert!(
            validate_batch_split(&logger, &mut req, &region)
                .unwrap_err()
                .to_string()
                .contains("cmd_type is BatchSplit but it doesn't have splits request")
        );

        req.set_cmd_type(AdminCmdType::Split);
        let mut split_req = SplitRequest::default();
        split_req.set_split_key(new_row_key(1, 2, 0));
        req.set_split(split_req);
        // Split is deprecated
        assert!(
            validate_batch_split(&logger, &mut req, &region)
                .unwrap_err()
                .to_string()
                .contains("cmd_type is BatchSplit but it doesn't have splits request")
        );

        // Empty key should be skipped.
        let mut split_keys = vec![vec![]];
        // Start key should be skipped.
        split_keys.push(start_key);

        req = new_batch_split_request(split_keys.clone());
        // Although invalid keys should be skipped, but if all keys are
        // invalid, errors should be reported.
        assert!(
            validate_batch_split(&logger, &mut req, &region)
                .unwrap_err()
                .to_string()
                .contains("no valid key found for split")
        );

        let mut key = new_row_key(1, 2, 0);
        let mut expected_key = key[..key.len() - 8].to_vec();
        split_keys.push(key);
        let mut expected_keys = vec![expected_key.clone()];

        // Extra version of same key will be ignored.
        key = new_row_key(1, 2, 1);
        split_keys.push(key);

        key = new_index_key(2, 2, &[Datum::I64(1), Datum::Bytes(b"brgege".to_vec())], 0);
        expected_key = key[..key.len() - 8].to_vec();
        split_keys.push(key);
        expected_keys.push(expected_key.clone());

        // Extra version of same key will be ignored.
        key = new_index_key(2, 2, &[Datum::I64(1), Datum::Bytes(b"brgege".to_vec())], 5);
        split_keys.push(key);

        expected_key =
            encode_bytes(b"t\x80\x00\x00\x00\x00\x00\x00\xea_r\x80\x00\x00\x00\x00\x05\x82\x7f");
        key = expected_key.clone();
        key.extend_from_slice(b"\x80\x00\x00\x00\x00\x00\x00\xd3");
        split_keys.push(key);
        expected_keys.push(expected_key.clone());

        // Split at table prefix.
        key = encode_bytes(b"t\x80\x00\x00\x00\x00\x00\x00\xee");
        split_keys.push(key.clone());
        expected_keys.push(key);

        // Raw key should be preserved.
        split_keys.push(b"xyz".to_vec());
        expected_keys.push(b"xyz".to_vec());

        key = encode_bytes(b"xyz:1");
        key.write_u64::<BigEndian>(0).unwrap();
        split_keys.push(key);
        expected_key = encode_bytes(b"xyz:1");
        expected_keys.push(expected_key);

        req = new_batch_split_request(split_keys);
        req.mut_splits().set_right_derive(true);
        validate_batch_split(&logger, &mut req, &region).unwrap();
        assert!(req.get_splits().get_right_derive());
        assert_eq!(req.get_splits().get_requests().len(), expected_keys.len());
        for (i, (req, expected_key)) in req
            .get_splits()
            .get_requests()
            .iter()
            .zip(expected_keys)
            .enumerate()
        {
            assert_eq!(
                req.get_split_key(),
                expected_key.as_slice(),
                "case {}",
                i + 1
            );
        }
    }
}

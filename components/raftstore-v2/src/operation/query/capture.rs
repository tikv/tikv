// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use raftstore::{
    coprocessor::{Cmd, CmdBatch, ObserveHandle, ObserveLevel},
    store::{
        cmd_resp,
        fsm::{
            apply::{notify_stale_req_with_msg, ObserverType, SHRINK_PENDING_CMD_QUEUE_CAP},
            new_read_index_request, ChangeObserver,
        },
        msg::ErrorCallback,
        util::compare_region_epoch,
        RegionSnapshot,
    },
};
use slog::info;
use txn_types::WriteBatchFlags;

use crate::{
    fsm::{ApplyResReporter, PeerFsmDelegate},
    raft::Apply,
    router::{message::CaptureChange, ApplyTask, QueryResChannel, QueryResult},
};

impl<'a, EK: KvEngine, ER: RaftEngine, T: raftstore::store::Transport>
    PeerFsmDelegate<'a, EK, ER, T>
{
    pub fn on_leader_callback(&mut self, ch: QueryResChannel) {
        let peer = self.fsm.peer();
        let mut msg = new_read_index_request(
            peer.region_id(),
            peer.region().get_region_epoch().clone(),
            peer.peer().clone(),
        );

        // Allow to capture change even is in flashback state.
        // TODO: add a test case for this kind of situation.
        if self.fsm.peer().region().get_is_in_flashback() {
            let mut flags = WriteBatchFlags::from_bits_check(msg.get_header().get_flags());
            flags.insert(WriteBatchFlags::FLASHBACK);
            msg.mut_header().set_flags(flags.bits());
        }

        self.on_query(msg, ch);
    }

    pub fn on_capture_change(&mut self, capture_change: CaptureChange) {
        fail_point!("raft_on_capture_change");

        let apply_scheduler = self.fsm.peer().apply_scheduler().cloned();
        let id = self.fsm.peer().region_id();
        let term = self.fsm.peer().term();
        let (ch, _) = QueryResChannel::with_callback(Box::new(move |res| {
            if let QueryResult::Response(resp) = res
                && resp.get_header().has_error()
            {
                // Return error
                capture_change.snap_cb.report_error(resp.clone());
                return;
            }
            if let Some(scheduler) = apply_scheduler {
                scheduler.send(ApplyTask::CaptureApply(capture_change))
            } else {
                let mut resp = cmd_resp::err_resp(raftstore::Error::RegionNotFound(id), term);
                resp.mut_header()
                    .mut_error()
                    .set_message("apply scheduler is None".to_owned());
                capture_change.snap_cb.report_error(resp);
            }
        }));
        self.on_leader_callback(ch);
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    pub fn on_capture_apply(&mut self, capture_change: CaptureChange) {
        let CaptureChange {
            observer,
            region_epoch,
            snap_cb,
        } = capture_change;
        let ChangeObserver { region_id, ty } = observer;

        let is_stale_cmd = match ty {
            ObserverType::Cdc(ObserveHandle { id, .. }) => self.observe().info.cdc_id.id > id,
            ObserverType::Rts(ObserveHandle { id, .. }) => self.observe().info.rts_id.id > id,
            ObserverType::Pitr(ObserveHandle { id, .. }) => self.observe().info.pitr_id.id > id,
        };
        if is_stale_cmd {
            notify_stale_req_with_msg(
                self.term(),
                format!(
                    "stale observe id {:?}, current id: {:?}",
                    ty.handle().id,
                    self.observe().info,
                ),
                snap_cb,
            );
            return;
        }

        assert_eq!(self.region_id(), region_id);
        let snapshot = match compare_region_epoch(
            &region_epoch,
            self.region(),
            false, // check_conf_ver
            true,  // check_ver
            true,  // include_region
        ) {
            Ok(()) => {
                // Commit the writebatch for ensuring the following snapshot can get all
                // previous writes.
                self.flush();
                let (applied_index, _) = self.apply_progress();
                let snap = RegionSnapshot::from_snapshot(
                    Arc::new(self.tablet().snapshot(None)),
                    Arc::new(self.region().clone()),
                );
                snap.set_apply_index(applied_index);
                snap
            }
            Err(e) => {
                // Return error if epoch not match
                snap_cb.report_error(cmd_resp::new_error(e));
                return;
            }
        };

        let observe = self.observe_mut();
        match ty {
            ObserverType::Cdc(id) => {
                observe.info.cdc_id = id;
            }
            ObserverType::Rts(id) => {
                observe.info.rts_id = id;
            }
            ObserverType::Pitr(id) => {
                observe.info.pitr_id = id;
            }
        }
        let level = observe.info.observe_level();
        observe.level = level;
        info!(self.logger, "capture update observe level"; "level" => ?level);
        snap_cb.set_result((RaftCmdResponse::default(), Some(Box::new(snapshot))));
    }

    pub fn observe_apply(
        &mut self,
        index: u64,
        term: u64,
        req: RaftCmdRequest,
        resp: &RaftCmdResponse,
    ) {
        if self.observe().level == ObserveLevel::None {
            return;
        }

        let cmd = Cmd::new(index, term, req, resp.clone());
        self.observe_mut().cmds.push(cmd);
    }

    pub fn flush_observed_apply(&mut self) {
        let level = self.observe().level;
        if level == ObserveLevel::None {
            return;
        }

        let region_id = self.region_id();
        let observe = self.observe_mut();
        let mut cmd_batch = CmdBatch::new(&observe.info, region_id);
        cmd_batch.extend(&observe.info, region_id, observe.cmds.drain(..));
        if observe.cmds.capacity() > SHRINK_PENDING_CMD_QUEUE_CAP {
            observe.cmds.shrink_to(SHRINK_PENDING_CMD_QUEUE_CAP);
        }
        self.coprocessor_host()
            .on_flush_applied_cmd_batch(level, vec![cmd_batch], self.tablet());
    }
}

#[cfg(test)]
mod test {
    use std::sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    };

    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::{KvTestEngine, TestTabletFactory},
    };
    use engine_traits::{
        FlushState, Peekable, SstApplyState, TabletContext, TabletRegistry, CF_DEFAULT, DATA_CFS,
    };
    use futures::executor::block_on;
    use kvproto::{
        metapb::Region,
        raft_serverpb::{PeerState, RegionLocalState},
    };
    use raft::StateRole;
    use raftstore::{
        coprocessor::{BoxCmdObserver, CmdObserver, CoprocessorHost},
        store::Config,
    };
    use slog::o;
    use tempfile::TempDir;
    use tikv_util::{
        store::new_peer,
        worker::dummy_scheduler,
        yatp_pool::{DefaultTicker, YatpPoolBuilder},
    };

    use super::*;
    use crate::{
        operation::{
            test_util::{create_tmp_importer, new_put_entry, MockReporter},
            CommittedEntries,
        },
        raft::Apply,
        router::build_any_channel,
    };

    #[derive(Clone)]
    struct TestObserver {
        sender: Sender<Vec<CmdBatch>>,
    }

    impl TestObserver {
        fn new() -> (Self, Receiver<Vec<CmdBatch>>) {
            let (tx, rx) = channel();
            (TestObserver { sender: tx }, rx)
        }
    }

    impl raftstore::coprocessor::Coprocessor for TestObserver {}
    impl<E: KvEngine> CmdObserver<E> for TestObserver {
        fn on_flush_applied_cmd_batch(
            &self,
            _max_level: ObserveLevel,
            cmd_batches: &mut Vec<CmdBatch>,
            _engine: &E,
        ) {
            self.sender.send(cmd_batches.clone()).unwrap();
        }

        fn on_applied_current_term(&self, _: StateRole, _: &Region) {}
    }

    #[test]
    fn test_capture_apply() {
        let store_id = 2;

        let mut region = Region::default();
        region.set_id(1);
        region.set_end_key(b"k20".to_vec());
        region.mut_region_epoch().set_version(3);
        let peers = vec![new_peer(2, 3)];
        region.set_peers(peers.into());

        let logger = slog_global::borrow_global().new(o!());
        let path = TempDir::new().unwrap();
        let cf_opts = DATA_CFS
            .iter()
            .copied()
            .map(|cf| (cf, CfOptions::default()))
            .collect();
        let factory = Box::new(TestTabletFactory::new(DbOptions::default(), cf_opts));
        let reg = TabletRegistry::new(factory, path.path()).unwrap();
        let ctx = TabletContext::new(&region, Some(5));
        reg.load(ctx, true).unwrap();

        let mut region_state = RegionLocalState::default();
        region_state.set_state(PeerState::Normal);
        region_state.set_region(region.clone());
        region_state.set_tablet_index(5);

        let (read_scheduler, _rx) = dummy_scheduler();
        let (reporter, _) = MockReporter::new();
        let (_tmp_dir, importer) = create_tmp_importer();
        let (ob, cmds_rx) = TestObserver::new();
        let mut host = CoprocessorHost::<KvTestEngine>::default();
        host.registry
            .register_cmd_observer(0, BoxCmdObserver::new(ob));

        let (dummy_scheduler1, _) = dummy_scheduler();
        let high_priority_pool = YatpPoolBuilder::new(DefaultTicker::default()).build_future_pool();
        let mut apply = Apply::new(
            &Config::default(),
            region
                .get_peers()
                .iter()
                .find(|p| p.store_id == store_id)
                .unwrap()
                .clone(),
            region_state,
            reporter,
            reg,
            read_scheduler,
            Arc::new(FlushState::new(5)),
            SstApplyState::default(),
            None,
            5,
            None,
            importer,
            host,
            dummy_scheduler1,
            high_priority_pool,
            logger.clone(),
        );

        let snap = Arc::new(Mutex::new(None));
        let snap_ = snap.clone();
        let (snap_cb, _) = build_any_channel(Box::new(move |args| {
            let snap = args.1.take().unwrap();
            let snapshot: RegionSnapshot<engine_rocks::RocksSnapshot> = match snap.downcast() {
                Ok(s) => *s,
                Err(t) => unreachable!("snapshot type should be the same: {:?}", t),
            };
            *snap_.lock().unwrap() = Some(snapshot);
        }));

        // put (k1, v1);
        // capture_apply;
        // put (k2, v2);
        let apply_tasks = vec![
            ApplyTask::CommittedEntries(CommittedEntries {
                entry_and_proposals: vec![(
                    new_put_entry(
                        region.id,
                        region.get_region_epoch().clone(),
                        b"k1",
                        b"v1",
                        5,
                        6,
                    ),
                    vec![],
                )],
            }),
            ApplyTask::CaptureApply(CaptureChange {
                observer: ChangeObserver::from_cdc(region.id, ObserveHandle::new()),
                region_epoch: region.get_region_epoch().clone(),
                snap_cb,
            }),
            ApplyTask::CommittedEntries(CommittedEntries {
                entry_and_proposals: vec![(
                    new_put_entry(
                        region.id,
                        region.get_region_epoch().clone(),
                        b"k2",
                        b"v2",
                        5,
                        7,
                    ),
                    vec![],
                )],
            }),
        ];

        for task in apply_tasks {
            match task {
                ApplyTask::CommittedEntries(ce) => {
                    block_on(async { apply.apply_committed_entries(ce).await });
                }
                ApplyTask::CaptureApply(capture_change) => {
                    apply.on_capture_apply(capture_change);
                }
                _ => unreachable!(),
            }
        }
        apply.flush();

        // must read (k1, v1) from snapshot and capture (k2, v2)
        let snap = snap.lock().unwrap().take().unwrap();
        let v1 = snap.get_value_cf(CF_DEFAULT, b"k1").unwrap().unwrap();
        assert_eq!(v1, b"v1");
        let v2 = snap.get_value_cf(CF_DEFAULT, b"k2").unwrap();
        assert!(v2.is_none());

        let cmds = cmds_rx.try_recv().unwrap();
        assert_eq!(cmds[0].len(), 1);
        let put2 = &cmds[0].cmds[0];
        assert_eq!(put2.term, 5);
        assert_eq!(put2.index, 7);
        let request = &put2.request.requests[0];
        assert_eq!(request.get_put().get_cf(), CF_DEFAULT);
        assert_eq!(request.get_put().get_key(), b"k2");
        assert_eq!(request.get_put().get_value(), b"v2");
        let response = &put2.response;
        assert!(!response.get_header().has_error());
    }
}

// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use kvproto::raft_cmdpb::RaftCmdResponse;
use raftstore::{
    coprocessor::ObserveHandle,
    store::{
        cmd_resp,
        fsm::{
            apply::{notify_stale_req_with_msg, ObserverType},
            new_read_index_request, ChangeObserver,
        },
        msg::ErrorCallback,
        util::compare_region_epoch,
        RegionSnapshot,
    },
};

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
        let msg = new_read_index_request(
            peer.region_id(),
            peer.region().get_region_epoch().clone(),
            peer.peer().clone(),
        );
        self.on_query(msg, ch);
    }

    pub fn on_capture_change(&mut self, capture_change: CaptureChange) {
        fail_point!("raft_on_capture_change");

        // TODO: Allow to capture change even is in flashback state.
        // TODO: add a test case for this kind of situation.

        let apply_router = self.fsm.peer().apply_scheduler().unwrap().clone();
        let (ch, _) = QueryResChannel::with_callback(Box::new(move |res| {
            if let QueryResult::Response(resp) = res && resp.get_header().has_error() {
                // Return error
                capture_change.snap_cb.report_error(resp.clone());
                return;
            }
            apply_router.send(ApplyTask::CaptureApply(capture_change))
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
            ObserverType::Cdc(ObserveHandle { id, .. }) => self.observe_info_mut().cdc_id.id > id,
            ObserverType::Rts(ObserveHandle { id, .. }) => self.observe_info_mut().rts_id.id > id,
            ObserverType::Pitr(ObserveHandle { id, .. }) => self.observe_info_mut().pitr_id.id > id,
        };
        if is_stale_cmd {
            notify_stale_req_with_msg(
                self.term(),
                format!(
                    "stale observe id {:?}, current id: {:?}",
                    ty.handle().id,
                    self.observe_info_mut().pitr_id.id
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
                RegionSnapshot::from_snapshot(
                    Arc::new(self.tablet().snapshot()),
                    Arc::new(self.region().clone()),
                )
            }
            Err(e) => {
                // Return error if epoch not match
                snap_cb.report_error(cmd_resp::new_error(e));
                return;
            }
        };

        match ty {
            ObserverType::Cdc(id) => {
                self.observe_info_mut().cdc_id = id;
            }
            ObserverType::Rts(id) => {
                self.observe_info_mut().rts_id = id;
            }
            ObserverType::Pitr(id) => {
                self.observe_info_mut().pitr_id = id;
            }
        }
        snap_cb.set_result((RaftCmdResponse::default(), Some(Box::new(snapshot))));
    }
}

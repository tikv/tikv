// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{mem, sync::Arc};

use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use raftstore::{
    coprocessor::{Cmd, CmdBatch, ObserveHandle, ObserveLevel},
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
use slog::info;

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
                    Arc::new(self.tablet().snapshot()),
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
        let mut cmd_batch = CmdBatch::new(&self.observe().info, region_id);
        let cmds = mem::replace(&mut self.observe_mut().cmds, vec![]);
        cmd_batch.extend(&self.observe().info, region_id, cmds);
        self.coprocessor_host()
            .on_flush_applied_cmd_batch(level, vec![cmd_batch], self.tablet());
    }
}

// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains merge related processing logic.
//!
//! ## Propose
//!
//! The proposal is initiated by the source region. After `PrepareMerge` is
//! applied, the source peer will send an `AskCommitMerge` message to the target
//! peer. (For simplicity, we send this message regardless of whether the target
//! peer is leader.) The message will also carry some source region logs that
//! may not be committed by some source peers.
//!
//! The source region cannot serve any writes until the merge is committed or
//! rollback-ed. This is guaranteed by `MergeContext::prepare_status`.
//!
//! ## Apply (`Apply::apply_commit_merge`)
//!
//! At first, target region will not apply the `CommitMerge` command. Instead
//! the apply progress will be paused and it redirects the log entries from
//! source region, as a `CatchUpLogs` message, to the local source region peer.
//! When the source region peer has applied all logs up to the prior
//! `PrepareMerge` command, it will signal the target peer. Here we use a
//! temporary channel instead of directly sending message between apply FSMs
//! like in v1.
//!
//! Here is a complete view of the process:
//!
//! ```text
//! |               Store 1               |              Store 2              |
//! |   Source Peer   |   Target Leader   |   Source Peer   |   Target Peer   |
//!         |
//!  apply PrepareMerge
//!          \
//!           +--------------+
//!           `AskCommitMerge`\
//!                            \
//!                    propose CommitMerge ---------------> append CommitMerge
//!                     apply CommitMerge                    apply CommitMerge
//!                        on apply res                             /|
//!                           /|                      +------------+ |
//!          +---------------+ |                     / `CatchUpLogs` |
//!         / `AckCommitMerge` |                    /                |
//!        /              (complete)          append logs         (pause)
//!    destroy self                                |                 .
//!                                        apply PrepareMerge        .
//!                                                |                 .
//!                                                +-----------> (continue)
//!                                                |                 |
//!                                           destroy self       (complete)
//! ```

use std::{
    any::Any,
    cmp, fs, io,
    path::{Path, PathBuf},
};

use crossbeam::channel::SendError;
use engine_traits::{KvEngine, RaftEngine, RaftLogBatch, TabletContext, TabletRegistry};
use futures::channel::oneshot;
use kvproto::{
    metapb::Region,
    raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, CommitMergeRequest, RaftCmdRequest},
    raft_serverpb::{MergedRecord, PeerState, RegionLocalState},
};
use protobuf::Message;
use raft::{GetEntriesContext, Storage, INVALID_ID, NO_LIMIT};
use raftstore::{
    coprocessor::RegionChangeReason,
    store::{
        fsm::new_admin_request, metrics::PEER_ADMIN_CMD_COUNTER, util, ProposalContext, Transport,
    },
    Result,
};
use slog::{debug, error, info, Logger};
use tikv_util::{
    config::ReadableDuration,
    log::SlogFormat,
    slog_panic,
    store::{find_peer, region_on_same_stores},
    time::Instant,
};

use super::{merge_source_path, PrepareStatus};
use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    operation::{AdminCmdResult, SharedReadTablet},
    raft::{Apply, Peer},
    router::{CmdResChannel, PeerMsg, PeerTick, StoreMsg},
};

#[derive(Debug)]
pub struct CommitMergeResult {
    pub index: u64,
    // Only used to respond `CatchUpLogs` to source peer.
    prepare_merge_index: u64,
    source_path: PathBuf,
    region_state: RegionLocalState,
    source: Region,
    source_safe_ts: u64,
    tablet: Box<dyn Any + Send + Sync>,
}

#[derive(Debug)]
pub struct CatchUpLogs {
    target_region_id: u64,
    merge: CommitMergeRequest,
    // safe_ts.
    tx: oneshot::Sender<u64>,
}

pub const MERGE_IN_PROGRESS_PREFIX: &str = "merge-in-progress";

struct MergeInProgressGuard(PathBuf);

impl MergeInProgressGuard {
    // `index` is the commit index of `CommitMergeRequest`
    fn new<EK>(
        logger: &Logger,
        registry: &TabletRegistry<EK>,
        target_region_id: u64,
        index: u64,
        tablet_path: &Path,
    ) -> io::Result<Option<Self>> {
        let name = registry.tablet_name(MERGE_IN_PROGRESS_PREFIX, target_region_id, index);
        let marker_path = registry.tablet_root().join(name);
        if !marker_path.exists() {
            if tablet_path.exists() {
                return Ok(None);
            } else {
                fs::create_dir(&marker_path)?;
                file_system::sync_dir(marker_path.parent().unwrap())?;
            }
        } else if tablet_path.exists() {
            info!(logger, "remove incomplete merged tablet"; "path" => %tablet_path.display());
            fs::remove_dir_all(tablet_path)?;
        }
        Ok(Some(Self(marker_path)))
    }

    fn defuse(self) -> io::Result<()> {
        fs::remove_dir(&self.0)?;
        file_system::sync_dir(self.0.parent().unwrap())
    }
}

fn commit_of_merge(r: &CommitMergeRequest) -> u64 {
    r.get_source_state().get_merge_state().get_commit()
}

// Source peer initiates commit merge on target peer.
impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Called after applying `PrepareMerge`.
    pub fn start_commit_merge<T: Transport>(&mut self, store_ctx: &mut StoreContext<EK, ER, T>) {
        fail::fail_point!("start_commit_merge");
        assert!(self.applied_merge_state().is_some());
        self.on_check_merge(store_ctx);
    }

    // Match v1::on_check_merge.
    pub fn on_check_merge<T: Transport>(&mut self, store_ctx: &mut StoreContext<EK, ER, T>) {
        if !self.serving() || self.applied_merge_state().is_none() {
            return;
        }
        self.add_pending_tick(PeerTick::CheckMerge);
        self.ask_target_peer_to_commit_merge(store_ctx);
    }

    // Match v1::schedule_merge.
    fn ask_target_peer_to_commit_merge<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
    ) {
        fail::fail_point!("on_schedule_merge", |_| {});
        fail::fail_point!(
            "ask_target_peer_to_commit_merge_2",
            self.region_id() == 2,
            |_| {}
        );
        fail::fail_point!(
            "ask_target_peer_to_commit_merge_store_1",
            store_ctx.store_id == 1,
            |_| {}
        );
        let state = self.applied_merge_state().unwrap();
        let target = state.get_target();
        let target_id = target.get_id();

        let (min_index, _) = self.calculate_min_progress().unwrap();
        let low = cmp::max(min_index + 1, state.get_min_index());
        // TODO: move this into raft module.
        // > over >= to include the PrepareMerge proposal.
        let entries = if low > state.get_commit() {
            Vec::new()
        } else {
            // TODO: fetch entries in async way
            match self.storage().entries(
                low,
                state.get_commit() + 1,
                NO_LIMIT,
                GetEntriesContext::empty(false),
            ) {
                Ok(ents) => ents,
                Err(e) => slog_panic!(
                    self.logger,
                    "failed to get merge entries";
                    "err" => ?e,
                    "low" => low,
                    "commit" => state.get_commit()
                ),
            }
        };

        let target_peer = find_peer(target, store_ctx.store_id).unwrap();
        let mut request = new_admin_request(target.get_id(), target_peer.clone());
        request
            .mut_header()
            .set_region_epoch(target.get_region_epoch().clone());
        let mut admin = AdminRequest::default();
        admin.set_cmd_type(AdminCmdType::CommitMerge);
        admin.mut_commit_merge().set_entries(entries.into());
        admin
            .mut_commit_merge()
            .set_source_state(self.storage().region_state().clone());
        request.set_admin_request(admin);
        // Please note that, here assumes that the unit of network isolation is store
        // rather than peer. So a quorum stores of source region should also be the
        // quorum stores of target region. Otherwise we need to enable proposal
        // forwarding.
        let msg = PeerMsg::AskCommitMerge(request);
        let router = store_ctx.router.clone();
        let logger = self.logger.clone();
        self.start_pre_flush(
            store_ctx,
            "commit_merge",
            true,
            &target.clone(),
            Box::new(move || {
                // If target peer is destroyed, life.rs is responsible for telling us to
                // rollback.
                match router.force_send(target_id, msg) {
                    Ok(_) => (),
                    Err(SendError(PeerMsg::AskCommitMerge(msg))) => {
                        if let Err(e) = router.force_send_control(StoreMsg::AskCommitMerge(msg)) {
                            if router.is_shutdown() {
                                return;
                            }
                            slog_panic!(
                                logger,
                                "fails to send `AskCommitMerge` msg to store";
                                "error" => ?e,
                            );
                        }
                    }
                    _ => unreachable!(),
                }
            }),
        );
    }
}

// Target peer handles the commit merge request.
impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_ask_commit_merge<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) {
        fail::fail_point!("on_ask_commit_merge", |_| {});
        let expected_epoch = req.get_header().get_region_epoch();
        let merge = req.get_admin_request().get_commit_merge();
        assert!(merge.has_source_state() && merge.get_source_state().has_merge_state());
        let source_region = merge.get_source_state().get_region();
        let source_id = source_region.get_id();
        let region = self.region();
        if let Some(r) = self
            .storage()
            .region_state()
            .get_merged_records()
            .iter()
            .find(|p| p.get_source_region_id() == source_id)
        {
            info!(
                self.logger,
                "ack commit merge because peer is already in merged_records";
                "source" => ?source_region,
                "index" => r.get_index(),
            );
            let index = commit_of_merge(req.get_admin_request().get_commit_merge());
            // If target caught up by snapshot, the source checkpoint hasn't been used.
            let source_path = merge_source_path(&store_ctx.tablet_registry, source_id, index);
            if source_path.exists() {
                self.record_tombstone_tablet_path(store_ctx, source_path, r.get_index());
            }
            let _ = store_ctx.router.force_send(
                source_id,
                PeerMsg::AckCommitMerge {
                    index,
                    target_id: self.region_id(),
                },
            );
            return;
        }
        // current region_epoch > region epoch in commit merge.
        if util::is_epoch_stale(expected_epoch, region.get_region_epoch()) {
            info!(
                self.logger,
                "reject commit merge because of stale";
                "current_epoch" => ?region.get_region_epoch(),
                "expected_epoch" => ?expected_epoch,
            );
            let index = commit_of_merge(req.get_admin_request().get_commit_merge());
            let _ = store_ctx
                .router
                .force_send(source_id, PeerMsg::RejectCommitMerge { index });
            return;
        }
        // current region_epoch < region epoch in commit merge.
        if util::is_epoch_stale(region.get_region_epoch(), expected_epoch) {
            info!(
                self.logger,
                "target region still not catch up, skip.";
                "source" => ?source_region,
                "target_region_epoch" => ?expected_epoch,
                "exist_region_epoch" => ?self.region().get_region_epoch(),
            );
            return;
        }
        assert!(
            util::is_sibling_regions(source_region, region),
            "{}: {:?}, {:?}",
            SlogFormat(&self.logger),
            source_region,
            region
        );
        assert!(
            region_on_same_stores(source_region, region),
            "{:?}, {:?}",
            source_region,
            region
        );
        assert!(!self.storage().has_dirty_data());
        let (ch, res) = CmdResChannel::pair();
        self.on_admin_command(store_ctx, req, ch);
        if let Some(res) = res.take_result()
            && res.get_header().has_error()
        {
            error!(
                self.logger,
                "failed to propose commit merge";
                "source" => source_id,
                "res" => ?res,
            );
            fail::fail_point!(
                "on_propose_commit_merge_fail_store_1",
                store_ctx.store_id == 1,
                |_| {}
            );
        } else {
            fail::fail_point!("on_propose_commit_merge_success");
        }
    }

    pub fn propose_commit_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) -> Result<u64> {
        (|| fail::fail_point!("propose_commit_merge_1", store_ctx.store_id == 1, |_| {}))();
        let mut proposal_ctx = ProposalContext::empty();
        proposal_ctx.insert(ProposalContext::COMMIT_MERGE);
        let data = req.write_to_bytes().unwrap();
        self.propose_with_ctx(store_ctx, data, proposal_ctx.to_vec())
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    // Match v1::exec_commit_merge.
    pub async fn apply_commit_merge(
        &mut self,
        req: &AdminRequest,
        index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        fail::fail_point!("apply_commit_merge");
        PEER_ADMIN_CMD_COUNTER.commit_merge.all.inc();

        self.flush();

        // Note: compared to v1, doesn't validate region state from kvdb any more.
        let reg = self.tablet_registry();
        let merge = req.get_commit_merge();
        let merge_commit = commit_of_merge(merge);
        let source_state = merge.get_source_state();
        let source_region = source_state.get_region();
        let source_path = merge_source_path(reg, source_region.get_id(), merge_commit);
        let mut source_safe_ts = 0;

        let mut start_time = Instant::now_coarse();
        let mut wait_duration = None;
        let force_send = (|| {
            fail::fail_point!("force_send_catch_up_logs", |_| true);
            false
        })();
        if !source_path.exists() || force_send {
            let (tx, rx) = oneshot::channel();
            self.res_reporter().redirect_catch_up_logs(CatchUpLogs {
                target_region_id: self.region_id(),
                merge: merge.clone(),
                tx,
            });
            match rx.await {
                Ok(ts) => {
                    source_safe_ts = ts;
                }
                Err(_) => {
                    if tikv_util::thread_group::is_shutdown(!cfg!(test)) {
                        return futures::future::pending().await;
                    } else {
                        slog_panic!(
                            self.logger,
                            "source peer is missing when getting checkpoint for merge"
                        );
                    }
                }
            }
            let now = Instant::now_coarse();
            wait_duration = Some(now.saturating_duration_since(start_time));
            start_time = now;
        };
        fail::fail_point!("after_acquire_source_checkpoint", |_| Err(
            tikv_util::box_err!("fp")
        ));

        info!(
            self.logger,
            "execute CommitMerge";
            "commit" => merge_commit,
            "entries" => merge.get_entries().len(),
            "index" => index,
            "source_region" => ?source_region,
        );

        let mut region = self.region().clone();
        // Use a max value so that pd can ensure overlapped region has a priority.
        let version = cmp::max(
            source_region.get_region_epoch().get_version(),
            region.get_region_epoch().get_version(),
        ) + 1;
        region.mut_region_epoch().set_version(version);
        if keys::enc_end_key(&region) == keys::enc_start_key(source_region) {
            region.set_end_key(source_region.get_end_key().to_vec());
        } else {
            region.set_start_key(source_region.get_start_key().to_vec());
        }

        let logger = self.logger.clone();
        let region_id = self.region_id();
        let target_tablet = self.tablet().clone();
        let mut ctx = TabletContext::new(&region, Some(index));
        ctx.flush_state = Some(self.flush_state().clone());
        let reg_clone = reg.clone();
        let source_path_clone = source_path.clone();
        let source_region_clone = source_region.clone();
        let (tx, rx) = oneshot::channel();
        self.high_priority_pool()
            .spawn(async move {
                let source_ctx = TabletContext::new(&source_region_clone, None);
                let source_tablet = reg_clone
                    .tablet_factory()
                    .open_tablet(source_ctx, &source_path_clone)
                    .unwrap_or_else(|e| {
                        slog_panic!(logger, "failed to open source checkpoint"; "err" => ?e);
                    });
                let open_time = Instant::now_coarse();

                let path = reg_clone.tablet_path(region_id, index);
                // Avoid seqno jump back between self.tablet and the newly created tablet.
                // If we are recovering, this flush would just be a noop.
                target_tablet.flush_cfs(&[], true).unwrap();
                let flush_time = Instant::now_coarse();

                let guard = MergeInProgressGuard::new(&logger, &reg_clone, region_id, index, &path)
                    .unwrap_or_else(|e| {
                        slog_panic!(
                            logger,
                            "fails to create MergeInProgressGuard";
                            "path" => %path.display(),
                            "error" => ?e
                        )
                    });
                let tablet = reg_clone.tablet_factory().open_tablet(ctx, &path).unwrap();
                if let Some(guard) = guard {
                    tablet
                        .merge(&[&source_tablet, &target_tablet])
                        .unwrap_or_else(|e| {
                            slog_panic!(
                                logger,
                                "fails to merge tablet";
                                "path" => %path.display(),
                                "error" => ?e
                            )
                        });
                    guard.defuse().unwrap_or_else(|e| {
                        slog_panic!(
                            logger,
                            "fails to defuse MergeInProgressGuard";
                            "path" => %path.display(),
                            "error" => ?e
                        )
                    });
                } else {
                    info!(logger, "reuse merged tablet");
                }
                let merge_time = Instant::now_coarse();
                info!(
                    logger,
                    "applied CommitMerge";
                    "source_region" => ?source_region_clone,
                    "wait" => ?wait_duration.map(|d| format!("{}", ReadableDuration(d))),
                    "open" => %ReadableDuration(open_time.saturating_duration_since(start_time)),
                    "merge" => %ReadableDuration(flush_time.saturating_duration_since(open_time)),
                    "flush" => %ReadableDuration(merge_time.saturating_duration_since(flush_time)),
                );
                tx.send(tablet).unwrap();
            })
            .unwrap();
        let tablet = rx.await.unwrap();

        fail::fail_point!("after_merge_source_checkpoint", |_| Err(
            tikv_util::box_err!("fp")
        ));

        self.set_tablet(tablet.clone());

        let state = self.region_state_mut();
        state.set_region(region.clone());
        state.set_state(PeerState::Normal);
        assert!(!state.has_merge_state());
        state.set_tablet_index(index);
        let mut merged_records: Vec<_> = state.take_merged_records().into();
        merged_records.append(&mut source_state.get_merged_records().into());
        state.set_merged_records(merged_records.into());
        let mut merged_record = MergedRecord::default();
        merged_record.set_source_region_id(source_region.get_id());
        merged_record.set_source_epoch(source_region.get_region_epoch().clone());
        merged_record.set_source_peers(source_region.get_peers().into());
        merged_record.set_source_removed_records(source_state.get_removed_records().into());
        merged_record.set_target_region_id(region.get_id());
        merged_record.set_target_epoch(region.get_region_epoch().clone());
        merged_record.set_target_peers(region.get_peers().into());
        merged_record.set_index(index);
        merged_record.set_source_index(merge_commit);
        state.mut_merged_records().push(merged_record);

        PEER_ADMIN_CMD_COUNTER.commit_merge.success.inc();

        Ok((
            AdminResponse::default(),
            AdminCmdResult::CommitMerge(CommitMergeResult {
                index,
                prepare_merge_index: merge_commit,
                source_path,
                region_state: self.region_state().clone(),
                source: source_region.to_owned(),
                source_safe_ts,
                tablet: Box::new(tablet),
            }),
        ))
    }
}

// Source peer catches up logs (optionally), and destroy itself.
impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Target peer.
    #[inline]
    pub fn on_redirect_catch_up_logs<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        catch_up_logs: CatchUpLogs,
    ) {
        let source_id = catch_up_logs.merge.get_source_state().get_region().get_id();
        assert_eq!(catch_up_logs.target_region_id, self.region_id());
        let _ = store_ctx
            .router
            .force_send(source_id, PeerMsg::CatchUpLogs(catch_up_logs));
    }

    // Match v1::on_catch_up_logs_for_merge.
    pub fn on_catch_up_logs<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut catch_up_logs: CatchUpLogs,
    ) {
        let source_id = catch_up_logs.merge.get_source_state().get_region().get_id();
        if source_id != self.region_id() {
            slog_panic!(
                self.logger,
                "get unexpected catch_up_logs";
                "merge" => ?catch_up_logs.merge,
            );
        }

        // Context would be empty if this peer hasn't applied PrepareMerge.
        if let Some(PrepareStatus::CatchUpLogs(cul)) =
            self.merge_context().and_then(|c| c.prepare_status.as_ref())
        {
            slog_panic!(
                self.logger,
                "get conflicting catch_up_logs";
                "new" => ?catch_up_logs.merge,
                "current" => ?cul.merge,
            );
        }
        if let Some(state) = self.applied_merge_state()
            && state.get_commit() == commit_of_merge(&catch_up_logs.merge)
        {
            assert_eq!(
                state.get_target().get_id(),
                catch_up_logs.target_region_id
            );
            self.finish_catch_up_logs(store_ctx, catch_up_logs);
        } else {
            // Directly append these logs to raft log and then commit them.
            match self.maybe_append_merge_entries(&catch_up_logs.merge) {
                Some(last_index) => {
                    info!(
                        self.logger,
                        "append and commit entries to source region";
                        "last_index" => last_index,
                    );
                    self.set_has_ready();
                }
                None => {
                    info!(self.logger, "no need to catch up logs");
                }
            }
            catch_up_logs.merge.clear_entries();
            self.merge_context_mut().prepare_status = Some(PrepareStatus::CatchUpLogs(catch_up_logs));
        }
    }

    fn maybe_append_merge_entries(&mut self, merge: &CommitMergeRequest) -> Option<u64> {
        let mut entries = merge.get_entries();
        let merge_commit = commit_of_merge(merge);
        if entries.is_empty() {
            // Though the entries is empty, it is possible that one source peer has caught
            // up the logs but commit index is not updated. If other source peers are
            // already destroyed, so the raft group will not make any progress, namely the
            // source peer can not get the latest commit index anymore.
            // Here update the commit index to let source apply rest uncommitted entries.
            return if merge_commit > self.raft_group().raft.raft_log.committed {
                self.raft_group_mut().raft.raft_log.commit_to(merge_commit);
                Some(merge_commit)
            } else {
                None
            };
        }
        let first = entries.first().unwrap();
        // make sure message should be with index not smaller than committed
        let mut log_idx = first.get_index() - 1;
        debug!(
            self.logger,
            "append merge entries";
            "log_index" => log_idx,
            "merge_commit" => merge_commit,
            "commit_index" => self.raft_group().raft.raft_log.committed,
        );
        if log_idx < self.raft_group().raft.raft_log.committed {
            // There may be some logs not included in CommitMergeRequest's entries, like
            // CompactLog, so the commit index may exceed the last index of the entires from
            // CommitMergeRequest. If that, no need to append
            if self.raft_group().raft.raft_log.committed - log_idx >= entries.len() as u64 {
                return None;
            }
            entries = &entries[(self.raft_group().raft.raft_log.committed - log_idx) as usize..];
            log_idx = self.raft_group().raft.raft_log.committed;
        }
        let log_term = self.index_term(log_idx);

        let last_log = entries.last().unwrap();
        if last_log.term > self.term() {
            // Hack: In normal flow, when leader sends the entries, it will use a term
            // that's not less than the last log term. And follower will update its states
            // correctly. For merge, we append the log without raft, so we have to take care
            // of term explicitly to get correct metadata.
            info!(
                self.logger,
                "become follower for new logs";
                "first_log_term" => first.term,
                "first_log_index" => first.index,
                "new_log_term" => last_log.term,
                "new_log_index" => last_log.index,
                "term" => self.term(),
            );
            self.raft_group_mut()
                .raft
                .become_follower(last_log.term, INVALID_ID);
        }

        self.raft_group_mut()
            .raft
            .raft_log
            .maybe_append(log_idx, log_term, merge_commit, entries)
            .map(|(_, last_index)| last_index)
    }

    #[inline]
    pub fn finish_catch_up_logs<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        c: CatchUpLogs,
    ) {
        let safe_ts = store_ctx
            .store_meta
            .lock()
            .unwrap()
            .region_read_progress
            .get(&self.region_id())
            .unwrap()
            .safe_ts();
        if c.tx.send(safe_ts).is_err() {
            error!(
                self.logger,
                "failed to respond to merge target, are we shutting down?"
            );
        }
        self.mark_for_destroy(None);
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Match v1::on_ready_commit_merge.
    pub fn on_apply_res_commit_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut res: CommitMergeResult,
    ) {
        fail::fail_point!(
            "on_apply_res_commit_merge_2",
            self.peer().store_id == 2,
            |_| {}
        );

        let region = res.region_state.get_region();
        assert!(
            res.source.get_end_key() == region.get_end_key()
                || res.source.get_start_key() == region.get_start_key()
        );
        let tablet: EK = match res.tablet.downcast() {
            Ok(t) => *t,
            Err(t) => unreachable!("tablet type should be the same: {:?}", t),
        };
        let acquired_source_safe_ts_before = res.source_safe_ts > 0;

        {
            let mut meta = store_ctx.store_meta.lock().unwrap();
            if let Some(p) = meta.region_read_progress.get(&res.source.get_id()) {
                res.source_safe_ts = p.safe_ts();
            }
            meta.set_region(region, true, &self.logger);
            let (reader, read_tablet) = meta.readers.get_mut(&region.get_id()).unwrap();
            self.set_region(
                &store_ctx.coprocessor_host,
                reader,
                region.clone(),
                RegionChangeReason::CommitMerge,
                res.index,
            );

            // Tablet should be updated in lock to match the epoch.
            *read_tablet = SharedReadTablet::new(tablet.clone());

            // After the region commit merged, the region's key range is extended and the
            // region's `safe_ts` should reset to `min(source_safe_ts, target_safe_ts)`
            self.read_progress_mut().merge_safe_ts(
                res.source_safe_ts,
                res.index,
                &store_ctx.coprocessor_host,
            );
            self.txn_context()
                .after_commit_merge(store_ctx, self.term(), region, &self.logger);
        }

        // We could only have gotten safe ts by sending `CatchUpLogs` earlier. If we
        // haven't, need to acknowledge that we have committed the merge, so that the
        // source peer can destroy itself. Note that the timing is deliberately
        // delayed after reading `store_ctx.meta` to get the source safe ts
        // before its meta gets cleaned up.
        if !acquired_source_safe_ts_before {
            let _ = store_ctx.router.force_send(
                res.source.get_id(),
                PeerMsg::AckCommitMerge {
                    index: res.prepare_merge_index,
                    target_id: self.region_id(),
                },
            );
        }

        if let Some(tablet) = self.set_tablet(tablet) {
            self.record_tombstone_tablet(store_ctx, tablet, res.index);
        }
        self.record_tombstone_tablet_path(store_ctx, res.source_path, res.index);

        // make approximate size and keys updated in time.
        // the reason why follower need to update is that there is a issue that after
        // merge and then transfer leader, the new leader may have stale size and keys.
        self.force_split_check(store_ctx);
        self.region_buckets_info_mut().set_bucket_stat(None);

        let region_id = self.region_id();
        self.state_changes_mut()
            .put_region_state(region_id, res.index, &res.region_state)
            .unwrap();
        self.storage_mut().set_region_state(res.region_state);
        self.storage_mut()
            .apply_trace_mut()
            .on_admin_flush(res.index);
        self.set_has_extra_write();

        if self.is_leader() {
            self.region_heartbeat_pd(store_ctx);
            info!(
                self.logger,
                "notify pd with merge";
                "source_region" => ?res.source,
                "target_region" => ?self.region(),
            );
            self.add_pending_tick(PeerTick::SplitRegionCheck);
            self.maybe_schedule_gc_peer_tick();
        }
    }

    // Called on source peer.
    pub fn on_ack_commit_merge(&mut self, index: u64, target_id: u64) {
        // We don't check it against merge state because source peer might just restart
        // and haven't replayed `PrepareMerge` yet.
        info!(
            self.logger,
            "destroy self on AckCommitMerge";
            "index" => index,
            "target_id" => target_id,
            "prepare_status" => ?self.merge_context().and_then(|c| c.prepare_status.as_ref()),
        );
        self.take_merge_context();
        self.mark_for_destroy(None);
    }
}

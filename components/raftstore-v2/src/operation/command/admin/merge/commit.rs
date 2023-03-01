// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains merge related processing logic.
//!
//! ## Propose
//!
//! The proposal is initiated by the source region. Each source peer
//! periodically checks for the freshness of local target region peer
//! (`Peer::on_check_merge`). The source peer will send a `CommitMerge` command
//! to the target peer once target is up-to-date. (For simplicity, we send this
//! message regardless of whether the target peer is leader.) The command will
//! also carry some source region logs that may not be committed by some peers.
//!
//! The source region cannot serve any writes until the merge is committed or
//! rollback-ed. This is guaranteed by `MergeContext::prepare_status`.
//!
//! ## Apply (`Apply::apply_commit_merge`)
//!
//! ```text
//!                 (5) CommitMergeResult
//!        +------------------------------------+
//!        |                                    |
//! +------+-------+   (1) CommitMerge   +------v------+  (6) MergeResult
//! | target apply <---------------------+ target peer +----------+
//! +---^--+-------+                     +-----^v------+          |
//!     |  +-------------------------------(redirect)             |
//!     |                                       |                 |
//!     | (4) signal                            | (2) CatchUpLogs |
//!     |                                       |                 |
//! +---+----------+  (3) LogsUpToDate   +------v------+          |
//! | source apply <---------------------+ source peer <----------+
//! +--------------+                     +-------------+
//! ```
//!
//! At first, target region will not apply the `CommitMerge` command. Instead
//! the apply progress will be paused and it redirects the log entries from
//! source region, as a `CatchUpLogs` message, to the local source region peer
//! (step 2). When the source region peer has applied all logs up to the prior
//! `PrepareMerge` command, it will send its tablet to the target region (step
//! 4). Here we use a temporary channel instead of directly sending message
//! between apply FSMs like in v1.
//!
//! ## On Apply Result (`Peer::on_apply_res_commit_merge`)
//!
//! Update the target peer states and send a `MergeResult` message to source
//! peer to destroy it.

use std::{any::Any, cmp, path::PathBuf};

use crossbeam::channel::SendError;
use engine_traits::{
    Checkpointer, KvEngine, RaftEngine, RaftLogBatch, TabletContext, TabletRegistry,
};
use futures::channel::oneshot;
use kvproto::{
    metapb::{self, Region},
    raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, CommitMergeRequest, RaftCmdRequest},
    raft_serverpb::PeerState,
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
use slog::{debug, error, info, warn};
use tikv_util::{
    box_err, slog_panic,
    store::{find_peer, region_on_same_stores},
};

use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    operation::{AdminCmdResult, SharedReadTablet},
    raft::{Apply, Peer},
    router::{ApplyTask, CmdResChannel, PeerMsg, PeerTick, StoreMsg},
};

#[derive(Debug)]
pub struct CommitMergeResult {
    pub index: u64,
    source_path: PathBuf,
    region: Region,
    source: Region,
    tablet: Box<dyn Any + Send + Sync>,
}

#[derive(Debug)]
pub struct CatchUpLogs {
    pub target_region_id: u64,
    pub merge: CommitMergeRequest,
    pub tx: oneshot::Sender<()>,
}

const MERGE_SOURCE_PREFIX: &str = "merge-source";

// Index is the commit index of `CommitMergeRequest`.
fn merge_source_path<EK>(registry: &TabletRegistry<EK>, region_id: u64, index: u64) -> PathBuf {
    let tablet_name = registry.tablet_name(MERGE_SOURCE_PREFIX, region_id, index);
    registry.tablet_root().join(tablet_name)
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Match v1::on_check_merge. Called on source peer.
    pub fn on_check_merge<T: Transport>(&mut self, store_ctx: &mut StoreContext<EK, ER, T>) {
        if !self.serving() || self.applied_merge_state().is_none() {
            return;
        }
        self.add_pending_tick(PeerTick::CheckMerge);
        if let Err(e) = self.schedule_commit_merge_proposal(store_ctx) {
            info!(
                self.logger,
                "failed to schedule commit merge";
                "err" => ?e,
            );
            // TODO: self.handle_commit_merge_failure(store_ctx, e);
        }
    }

    // Match v1::schedule_merge.
    fn schedule_commit_merge_proposal<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
    ) -> Result<()> {
        let state = self.applied_merge_state().unwrap();
        let target = state.get_target();

        self.validate_merge_peer(store_ctx, target)?;
        let target_id = target.get_id();

        let (min_index, _) = self.calculate_min_progress()?;
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
                    "failed to get merge entires";
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
        admin.mut_commit_merge().set_source(self.region().clone());
        admin.mut_commit_merge().set_commit(state.get_commit());
        admin.mut_commit_merge().set_entries(entries.into());
        request.set_admin_request(admin);
        // Please note that, here assumes that the unit of network isolation is store
        // rather than peer. So a quorum stores of source region should also be the
        // quorum stores of target region. Otherwise we need to enable proposal
        // forwarding.
        let msg = PeerMsg::AskCommitMerge(request);
        match store_ctx.router.force_send(target_id, msg) {
            Ok(_) => (),
            Err(SendError(PeerMsg::AskCommitMerge(msg))) => {
                store_ctx
                    .router
                    .force_send_control(StoreMsg::AskCommitMerge(msg))
                    .unwrap_or_else(|e| {
                        slog_panic!(
                            self.logger,
                            "fails to send `AskCommitMerge` msg to store";
                            "error" => ?e,
                        )
                    });
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    // Match v1::validate_merge_peer.
    fn validate_merge_peer<T>(
        &self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        target_region: &metapb::Region,
    ) -> Result<()> {
        let target_region_id = target_region.get_id();
        let exist_region = {
            let store_meta = store_ctx.store_meta.lock().unwrap();
            store_meta
                .regions
                .get(&target_region_id)
                .map(|(r, _)| r.clone())
        };
        if let Some(r) = exist_region {
            let exist_epoch = r.get_region_epoch();
            let expect_epoch = target_region.get_region_epoch();
            // exist_epoch > expect_epoch
            if util::is_epoch_stale(expect_epoch, exist_epoch) {
                return Err(box_err!(
                    "target region changed {:?} -> {:?}",
                    target_region,
                    r
                ));
            }
        }
        // Let it send the request. If target peer is destroyed, life.rs is responsible
        // for telling us to rollback.
        Ok(())
    }

    pub fn on_reject_commit_merge(&mut self, index: u64) {
        warn!(self.logger, "target peer rejected commit merge"; "index" => index);
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_ask_commit_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) {
        let expected_epoch = req.get_header().get_region_epoch();
        if expected_epoch == self.region().get_region_epoch() {
            let (ch, _) = CmdResChannel::pair();
            self.on_admin_command(store_ctx, req, ch);
        } else if util::is_epoch_stale(expected_epoch, self.region().get_region_epoch()) {
            let commit_merge = req.get_admin_request().get_commit_merge();
            let source_id = commit_merge.get_source().get_id();
            let _ = store_ctx.router.force_send(
                source_id,
                PeerMsg::RejectCommitMerge {
                    index: commit_merge.get_commit(),
                },
            );
        }
    }

    pub fn propose_commit_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) -> Result<u64> {
        if self.storage().has_dirty_data() {
            return Err(box_err!(
                "{:?} target peer has dirty data, try again later",
                self.logger.list()
            ));
        }
        // Match v1::check_merge_proposal.
        let source_region = req.get_admin_request().get_commit_merge().get_source();
        let region = self.region();
        if !util::is_sibling_regions(source_region, region) {
            return Err(box_err!(
                "{:?} and {:?} should be sibling",
                source_region,
                region
            ));
        }
        if !region_on_same_stores(source_region, region) {
            return Err(box_err!(
                "peers not matched: {:?} {:?}",
                source_region,
                region
            ));
        }
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
        PEER_ADMIN_CMD_COUNTER.commit_merge.all.inc();

        self.flush();

        // Note: compared to v1, doesn't validate region state from kvdb any more.
        let reg = self.tablet_registry();
        let merge = req.get_commit_merge();
        let source_region = merge.get_source();
        let source_path = merge_source_path(reg, source_region.get_id(), merge.get_commit());
        if !source_path.exists() {
            let (tx, rx) = oneshot::channel();
            self.res_reporter()
                .redirect_catch_up_logs(self.region_id(), merge.clone(), tx);
            rx.await.unwrap_or_else(|_| {
                // TODO: handle this gracefully.
                slog_panic!(self.logger, "source peer is missing");
            });
        }
        let ctx = TabletContext::new(source_region, None);
        let source_tablet = reg.tablet_factory().open_tablet(ctx, &source_path).unwrap();

        info!(
            self.logger,
            "execute CommitMerge";
            "commit" => merge.get_commit(),
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

        // TODO: check both are trimmed.

        let path = reg.tablet_path(self.region_id(), index);
        // The last merge is incomplete.
        if path.exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }
        let mut ctx = TabletContext::new(&region, Some(index));
        ctx.flush_state = Some(self.flush_state().clone());
        let tablet = reg.tablet_factory().open_tablet(ctx, &path).unwrap();
        tablet.merge(&[&source_tablet, self.tablet()]).unwrap();
        // This is only to consume in-memory tombstones, so that the two tablets don't
        // overlap with each other. Remove this once trim check is finished.
        tablet.flush_cfs(&[], true).unwrap();
        self.set_tablet(tablet.clone());

        self.region_state_mut().set_region(region.clone());
        self.region_state_mut().set_state(PeerState::Normal);
        self.region_state_mut().clear_merge_state();
        self.region_state_mut().set_tablet_index(index);

        PEER_ADMIN_CMD_COUNTER.commit_merge.success.inc();

        Ok((
            AdminResponse::default(),
            AdminCmdResult::CommitMerge(CommitMergeResult {
                index,
                source_path,
                region,
                source: source_region.to_owned(),
                tablet: Box::new(tablet),
            }),
        ))
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Called on source peer.
    pub fn update_merge_progress_on_apply_res_prepare_merge<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
    ) {
        assert!(self.applied_merge_state().is_some());
        if let Some(c) = &self.merge_context().unwrap().catch_up_logs
            && self.catch_up_logs_ready(c)
        {
            let c = self.merge_context_mut().catch_up_logs.take().unwrap();
            self.apply_scheduler()
                .unwrap()
                .send(ApplyTask::LogsUpToDate(c));
            return;
        }
        self.on_check_merge(store_ctx);
    }

    #[inline]
    pub fn on_redirect_catch_up_logs<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        catch_up_logs: CatchUpLogs,
    ) {
        let source_id = catch_up_logs.merge.get_source().get_id();
        assert_eq!(catch_up_logs.target_region_id, self.region_id());
        let _ = store_ctx
            .router
            .force_send(source_id, PeerMsg::CatchUpLogs(catch_up_logs));
    }

    // Match v1::on_catch_up_logs_for_merge. Called on source peer.
    pub fn on_catch_up_logs(&mut self, mut catch_up_logs: CatchUpLogs) {
        let source_id = catch_up_logs.merge.get_source().get_id();
        if source_id != self.region_id() {
            slog_panic!(
                self.logger,
                "get unexpected catch_up_logs";
                "merge" => ?catch_up_logs.merge,
            );
        }

        if let Some(ref cul) = self.merge_context().unwrap().catch_up_logs {
            slog_panic!(
                self.logger,
                "get conflicting catch_up_logs";
                "new" => ?catch_up_logs.merge,
                "current" => ?cul.merge,
            );
        }
        if !self.catch_up_logs_ready(&catch_up_logs) {
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
            self.merge_context_mut().catch_up_logs = Some(catch_up_logs);
        } else {
            catch_up_logs.merge.clear_entries();
            self.apply_scheduler()
                .unwrap()
                .send(ApplyTask::LogsUpToDate(catch_up_logs));
        }
    }

    #[inline]
    fn catch_up_logs_ready(&self, catch_up_logs: &CatchUpLogs) -> bool {
        if let Some(state) = self.applied_merge_state()
            && state.get_commit() == catch_up_logs.merge.get_commit()
        {
            assert_eq!(
                state.get_target().get_id(),
                catch_up_logs.target_region_id
            );
            true
        } else {
            false
        }
    }

    fn maybe_append_merge_entries(&mut self, merge: &CommitMergeRequest) -> Option<u64> {
        let mut entries = merge.get_entries();
        if entries.is_empty() {
            // Though the entries is empty, it is possible that one source peer has caught
            // up the logs but commit index is not updated. If other source peers are
            // already destroyed, so the raft group will not make any progress, namely the
            // source peer can not get the latest commit index anymore.
            // Here update the commit index to let source apply rest uncommitted entries.
            return if merge.get_commit() > self.raft_group().raft.raft_log.committed {
                self.raft_group_mut()
                    .raft
                    .raft_log
                    .commit_to(merge.get_commit());
                Some(merge.get_commit())
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
            "merge_commit" => merge.get_commit(),
            "commit_index" => self.raft_group().raft.raft_log.committed,
        );
        if log_idx < self.raft_group().raft.raft_log.committed {
            // There are maybe some logs not included in CommitMergeRequest's entries, like
            // CompactLog, so the commit index may exceed the last index of the entires from
            // CommitMergeRequest. If that, no need to append
            if self.raft_group().raft.raft_log.committed - log_idx >= entries.len() as u64 {
                return None;
            }
            entries = &entries[(self.raft_group().raft.raft_log.committed - log_idx) as usize..];
            log_idx = self.raft_group().raft.raft_log.committed;
        }
        let log_term = self.get_index_term(log_idx);

        let last_log = entries.last().unwrap();
        if last_log.term > self.term() {
            // Hack: In normal flow, when leader sends the entries, it will use a term
            // that's not less than the last log term. And follower will update its states
            // correctly. For merge, we append the log without raft, so we have to take care
            // of term explicitly to get correct metadata.
            info!(
                self.logger,
                "become follower for new logs";
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
            .maybe_append(log_idx, log_term, merge.get_commit(), entries)
            .map(|(_, last_index)| last_index)
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    // Match v1::logs_up_to_date_for_merge. Called on source peer.
    pub fn on_logs_up_to_date(&mut self, catch_up_logs: CatchUpLogs) {
        info!(self.logger, "source logs are all applied now");
        let _ = self.flush();
        let tablet = self.tablet().clone();
        let mut checkpointer = tablet.new_checkpointer().unwrap_or_else(|e| {
            slog_panic!(
                self.logger,
                "fails to create checkpoint object";
                "error" => ?e
            )
        });
        let reg = self.tablet_registry();
        let path = merge_source_path(reg, self.region_id(), catch_up_logs.merge.get_commit());
        checkpointer.create_at(&path, None, 0).unwrap_or_else(|e| {
            slog_panic!(
                self.logger,
                "fails to create checkpoint";
                "path" => %path.display(),
                "error" => ?e
            )
        });
        if catch_up_logs.tx.send(()).is_err() {
            error!(
                self.logger,
                "failed to respond to merge target, are we shutting down?"
            );
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Match v1::on_ready_commit_merge.
    pub fn on_apply_res_commit_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        res: CommitMergeResult,
    ) {
        let tablet: EK = match res.tablet.downcast() {
            Ok(t) => *t,
            Err(t) => unreachable!("tablet type should be the same: {:?}", t),
        };
        {
            let mut meta = store_ctx.store_meta.lock().unwrap();

            // In v2 we don't remove source region immediately.
            let source_region = &meta.regions.get(&res.source.get_id()).unwrap().0;
            assert!(
                source_region.get_end_key() == res.region.get_end_key()
                    || source_region.get_start_key() == res.region.get_start_key()
            );
            if let Some((d, _)) = meta.readers.get_mut(&res.source.get_id()) {
                d.mark_pending_remove();
            }

            meta.set_region(&res.region, true, &self.logger);
            let (reader, read_tablet) = meta.readers.get_mut(&res.region.get_id()).unwrap();
            self.set_region(
                &store_ctx.coprocessor_host,
                reader,
                res.region.clone(),
                RegionChangeReason::CommitMerge,
                res.index,
            );

            // Tablet should be updated in lock to match the epoch.
            *read_tablet = SharedReadTablet::new(tablet.clone());

            // After the region commit merged, the region's key range is extended and the
            // region's `safe_ts` should reset to `min(source_safe_ts, target_safe_ts)`
            let source_read_progress = meta
                .region_read_progress
                .remove(&res.source.get_id())
                .unwrap();
            self.read_progress_mut().merge_safe_ts(
                source_read_progress.safe_ts(),
                res.index,
                &store_ctx.coprocessor_host,
            );
        }
        if let Some(tablet) = self.set_tablet(tablet) {
            self.record_tombstone_tablet(store_ctx, tablet, res.index);
        }
        self.record_tombstone_tablet_path(store_ctx, res.source_path, res.index);

        // If a follower merges into a leader, a more recent read may happen
        // on the leader of the follower. So max ts should be updated after
        // a region merge.
        // TODO(tabokie): v1 doesn't reset locks status?
        self.txn_context()
            .on_became_leader(store_ctx, self.term(), self.region(), &self.logger);

        // make approximate size and keys updated in time.
        // the reason why follower need to update is that there is a issue that after
        // merge and then transfer leader, the new leader may have stale size and keys.
        self.force_split_check(store_ctx);
        self.region_buckets_info_mut().set_bucket_stat(None);

        if self.is_leader() {
            self.region_heartbeat_pd(store_ctx);
            info!(
                self.logger,
                "notify pd with merge";
                "source_region" => ?res.source,
                "target_region" => ?self.region(),
            );
            self.add_pending_tick(PeerTick::SplitRegionCheck);
        }

        let region_id = self.region_id();
        let region_state = self.storage().region_state().clone();
        self.state_changes_mut()
            .put_region_state(region_id, res.index, &region_state)
            .unwrap();
        self.storage_mut()
            .apply_trace_mut()
            .on_admin_flush(res.index);
        self.set_has_extra_write();
        // destroy source.
        let _ = store_ctx.router.send(
            res.source.get_id(),
            PeerMsg::MergeResult {
                target_region_id: self.region_id(),
                target: self.peer().clone(),
            },
        );
        self.take_merge_context();
    }

    // Match v1::on_merge_result. Called on source peer.
    pub fn on_merge_result(&mut self, target_region_id: u64, target: metapb::Peer) {
        let exists = self.applied_merge_state().map_or(true, |s| {
            s.get_target()
                .get_peers()
                .iter()
                .any(|p| p.get_store_id() == target.get_store_id() && p.get_id() <= target.get_id())
        });
        if !exists {
            slog_panic!(
                self.logger,
                "unexpected merge result";
                "merge_state" => ?self.applied_merge_state(),
                "target" => ?target,
            );
        }
        self.take_merge_context();
        if self.is_handling_snapshot() {
            slog_panic!(
                self.logger,
                "applying snapshot on getting merge result";
                "target_region_id" => target_region_id,
                "target" => ?target,
            );
        }
        if !self.storage().is_initialized() {
            slog_panic!(
                self.logger,
                "not initialized on getting merge result";
                "target_region_id" => target_region_id,
                "target" => ?target,
            );
        }
        info!(
            self.logger,
            "merge finished";
            "merge_state" => ?self.applied_merge_state(),
        );
        self.mark_for_destroy(None);
    }
}

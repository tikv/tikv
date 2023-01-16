// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains merge related processing logic.
//!
//! ## Propose
//!
//! The proposal is initiated by the source region. Each source peer
//! periodically checks for the freshness of local target region peer
//! (`Peer::on_merge_check_tick`). The source peer will send a `CommitMerge`
//! command to the target peer once target is up-to-date. (For simplicity, we
//! send this message regardless of whether the target peer is leader.) The
//! command will also carry some source region logs that may not be committed by
//! some peers.
//!
//! The source region cannot serve any writes until the merge is commited or
//! rollback-ed. This is guaranteed by `Peer::pending_merge_state`.
//!
//! ## Apply (`Apply::apply_commit_merge`)
//!
//! ```text
//!                (5) CommitMergeResult
//!        +---------------------------------+
//!        |                                 |
//! +------+-------+ (1) CommitMerge  +------v------+  (6) MergeResult
//! | target apply <------------------+ target peer +----------+
//! +---^--+-------+                  +-----^v------+          |
//!     |  +----------------------------<redirect>             |
//!     |                                    |                 |
//!     | (4) SourceReady                    | (2) CatchUpLogs |
//!     |                                    |                 |
//! +---+----------+                  +------v------+          |
//! | source apply <------------------+ source peer <----------+
//! +--------------+ (3) LogsUpToDate +-------------+
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

use std::{
    any::Any,
    cmp::{self, Ordering},
    fmt::{self, Debug},
};

use engine_traits::{KvEngine, RaftEngine, RaftLogBatch};
use futures::channel::oneshot;
use kvproto::{
    metapb::{self, Region},
    raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, CommitMergeRequest},
    raft_serverpb::{MergeState, PeerState},
};
use raft::{GetEntriesContext, Storage, INVALID_ID, NO_LIMIT};
use raftstore::{
    coprocessor::RegionChangeReason,
    store::{
        fsm::new_admin_request, metrics::PEER_ADMIN_CMD_COUNTER, util, MergeResultKind, Transport,
    },
    Error, Result,
};
use slog::{debug, error, info};
use tikv_util::{box_err, slog_panic, store::find_peer};

use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    operation::{AdminCmdResult, SharedReadTablet},
    raft::{Apply, Peer},
    router::{ApplyTask, PeerMsg, PeerTick},
};

#[derive(Debug)]
pub struct CommitMergeResult {
    index: u64,
    region: Region,
    source: Region,
    tablet: Box<dyn Any + Send + Sync>,
}

struct SourceReady {
    tablet: Box<dyn Any + Send + Sync>,
}

pub struct CatchUpLogs {
    target_region_id: u64,
    merge: CommitMergeRequest,
    tx: oneshot::Sender<SourceReady>,
}

impl Debug for CatchUpLogs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatchUpLogs")
            .field("target_region_id", &self.target_region_id)
            .field("merge", &self.merge)
            .finish()
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Mirrors v1::on_check_merge. Called on source peer.
    pub fn on_check_merge<T: Transport>(&mut self, store_ctx: &mut StoreContext<EK, ER, T>) {
        if !self.serving() || self.pending_merge_state.is_none() {
            return;
        }
        self.add_pending_tick(PeerTick::CheckMerge);
        if let Err(e) = self.schedule_commit_merge_proposal(store_ctx) {
            self.handle_commit_merge_failure(store_ctx, e);
        }
    }

    // Mirrors v1::schedule_merge.
    fn schedule_commit_merge_proposal<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
    ) -> Result<()> {
        let state = self.pending_merge_state.as_ref().unwrap();
        let expect_region = state.get_target();

        if !self.validate_merge_peer(store_ctx, expect_region)? {
            // Wait till next round.
            return Ok(());
        }
        let target_id = expect_region.get_id();
        let sibling_region = expect_region;

        let (min_index, _) = self.get_min_progress()?;
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

        let sibling_peer = find_peer(sibling_region, store_ctx.store_id).unwrap();
        let mut request = new_admin_request(sibling_region.get_id(), sibling_peer.clone());
        request
            .mut_header()
            .set_region_epoch(sibling_region.get_region_epoch().clone());
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
        let (msg, _) = PeerMsg::admin_command(request);
        store_ctx
            .router
            .force_send(target_id, msg)
            .map_err(|_| Error::RegionNotFound(target_id))
    }

    // Mirrors v1::validate_merge_peer.
    fn validate_merge_peer<T>(
        &self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        target_region: &metapb::Region,
    ) -> Result<bool> {
        let target_region_id = target_region.get_id();
        let exist_region = {
            let store_meta = store_ctx.store_meta.lock().unwrap();
            store_meta
                .readers
                .get(&target_region_id)
                .map(|(r, _)| r.clone())
        };
        if let Some(r) = exist_region {
            let exist_epoch = r.region.get_region_epoch();
            let expect_epoch = target_region.get_region_epoch();
            // exist_epoch > expect_epoch
            if util::is_epoch_stale(expect_epoch, exist_epoch) {
                return Err(box_err!(
                    "target region changed {:?} -> {:?}",
                    target_region,
                    r
                ));
            }
            // exist_epoch < expect_epoch
            if util::is_epoch_stale(exist_epoch, expect_epoch) {
                info!(
                    self.logger,
                    "target region still not catch up, skip.";
                    "target_region" => ?target_region,
                    "exist_region" => ?r,
                );
                return Ok(false);
            }
            return Ok(true);
        }

        // All of the target peers must exist before merging which is guaranteed by PD.
        // Now the target peer is not in region map.
        match self.is_merge_target_region_stale(store_ctx, target_region) {
            Err(e) => {
                error!(
                    self.logger,
                    "failed to load region state, ignore";
                    "err" => %e,
                    "target_region_id" => target_region_id,
                );
                Ok(false)
            }
            Ok(true) => Err(box_err!("region {} is destroyed", target_region_id)),
            Ok(false) => {
                let msg = "something is wrong, maybe PD do not ensure all target peers exist before merging";
                if store_ctx.cfg.dev_assert {
                    slog_panic!(self.logger, msg);
                } else {
                    error!(self.logger, "{}", msg);
                }
                Ok(false)
            }
        }
    }

    /// Check if merge target region is staler than the local one in kv engine.
    /// It should be called when target region is not in region map in memory.
    /// If everything is ok, the answer should always be true because PD should
    /// ensure all target peers exist. So if not, error log will be printed
    /// and return false.
    fn is_merge_target_region_stale<T>(
        &self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        target_region: &metapb::Region,
    ) -> Result<bool> {
        let target_region_id = target_region.get_id();
        let target_peer_id = find_peer(target_region, store_ctx.store_id)
            .unwrap()
            .get_id();

        if let Some(target_state) = store_ctx
            .engine
            .get_region_state(target_region_id, u64::MAX)?
        {
            let state_epoch = target_state.get_region().get_region_epoch();
            if util::is_epoch_stale(target_region.get_region_epoch(), state_epoch) {
                return Ok(true);
            }
            // The local target region epoch is staler than target region's.
            // In the case where the peer is destroyed by receiving gc msg rather than
            // applying conf change, the epoch may staler but it's legal, so check peer id
            // to assure that.
            if let Some(local_target_peer_id) =
                find_peer(target_state.get_region(), store_ctx.store_id).map(|r| r.get_id())
            {
                match local_target_peer_id.cmp(&target_peer_id) {
                    Ordering::Equal => {
                        if target_state.get_state() == PeerState::Tombstone {
                            // The local target peer has already been destroyed.
                            return Ok(true);
                        }
                        error!(
                            self.logger,
                            "the local target peer state is not tombstone in kv engine";
                            "target_peer_id" => target_peer_id,
                            "target_peer_state" => ?target_state.get_state(),
                            "target_region" => ?target_region,
                        );
                    }
                    Ordering::Greater => {
                        if state_epoch.get_version() == 0 && state_epoch.get_conf_ver() == 0 {
                            // There is a new peer and it's destroyed without being initialised.
                            return Ok(true);
                        }
                        // The local target peer id is greater than the one in target region, but
                        // its epoch is staler than target_region's. That is contradictory.
                        slog_panic!(
                            self.logger,
                            "local target peer id is greater but its epoch is staler";
                            "local_target_region" => ?target_state.get_region(),
                            "target_region" => ?target_region,
                            "local_target_peer_id" => local_target_peer_id,
                            "target_peer_id" => target_peer_id,
                        );
                    }
                    Ordering::Less => {
                        error!(
                            self.logger,
                            "the local target peer id in kv engine is less than the one in target region";
                            "local_target_peer_id" => local_target_peer_id,
                            "target_peer_id" => target_peer_id,
                            "target_region" => ?target_region,
                        );
                    }
                }
            } else {
                // Can't get local target peer id probably because this target peer is removed
                // by applying conf change
                error!(
                    self.logger,
                    "the local target peer does not exist in target region state";
                    "target_region" => ?target_region,
                    "local_target" => ?target_state.get_region(),
                );
            }
        } else {
            error!(
                self.logger,
                "failed to load target peer's RegionLocalState from kv engine";
                "target_peer_id" => target_peer_id,
                "target_region" => ?target_region,
            );
        }
        Ok(false)
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    // Mirrors v1::exec_commit_merge.
    pub async fn apply_commit_merge(
        &mut self,
        req: &AdminRequest,
        index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        PEER_ADMIN_CMD_COUNTER.commit_merge.all.inc();

        let merge = req.get_commit_merge();
        let source_region = merge.get_source();

        // TODO(tabokie): what does this comment mean?
        // No matter whether the source peer has applied to the required index,
        // it's a race to write apply state in both source delegate and target
        // delegate. So asking the source delegate to stop first.
        let (tx, rx) = oneshot::channel();
        self.res_reporter().send(PeerMsg::CatchUpLogs(CatchUpLogs {
            target_region_id: self.region_id(),
            merge: merge.clone(),
            tx,
        }));
        let r = rx.await;
        if r.is_err() {
            // TODO: handle this gracefully.
            slog_panic!(self.logger, "source peer is missing");
        }
        let source = r.unwrap();
        // TODO: merge source tablet into self.
        let _ = source.tablet;

        info!(
            self.logger,
            "execute CommitMerge";
            "commit" => merge.get_commit(),
            "entries" => merge.get_entries().len(),
            "index" => index,
            "source_region" => ?source_region
        );

        let state = self.region_state();
        if state.get_state() != PeerState::Merging {
            slog_panic!(
                self.logger,
                "unexpected state of merging region";
                "state" => ?state,
            );
        }
        let exist_region = state.get_region().to_owned();
        if *source_region != exist_region {
            slog_panic!(
                self.logger,
                "merge source region mismatch";
                "existed" => ?exist_region,
                "provided" => ?source_region,
            );
        }
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

        self.region_state_mut().set_region(region.clone());
        self.region_state_mut().set_state(PeerState::Normal);
        self.region_state_mut()
            .set_merge_state(MergeState::default());

        PEER_ADMIN_CMD_COUNTER.commit_merge.success.inc();

        Ok((
            AdminResponse::default(),
            AdminCmdResult::CommitMerge(CommitMergeResult {
                index,
                region,
                source: source_region.to_owned(),
                tablet: Box::new(self.tablet().clone()),
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
        assert!(self.has_pending_merge_state());
        if let Some(c) = &self.catch_up_logs
            && self.catch_up_logs_ready(c)
        {
            let c = self.catch_up_logs.take().unwrap();
            self.apply_scheduler()
                .unwrap()
                .send(ApplyTask::LogsUpToDate(c));
            return;
        }
        self.on_check_merge(store_ctx);
    }

    // Mirrors v1::on_catch_up_logs_for_merge. Called on source peer.
    pub fn on_catch_up_logs<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut catch_up_logs: CatchUpLogs,
    ) {
        let source_id = catch_up_logs.merge.get_source().get_id();
        let target_id = catch_up_logs.target_region_id;
        if source_id != self.region_id() {
            if target_id != self.region_id() {
                slog_panic!(
                    self.logger,
                    "get unexpected catch_up_logs";
                    "target" => target_id,
                    "source" => source_id,
                );
            }
            // Redirect to source id.
            let _ = store_ctx
                .router
                .force_send(source_id, PeerMsg::CatchUpLogs(catch_up_logs));
            return;
        }

        if let Some(ref cul) = self.catch_up_logs {
            slog_panic!(
                self.logger,
                "get conflicting catch_up_logs";
                "new" => target_id,
                "current" => cul.target_region_id,
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
            self.catch_up_logs = Some(catch_up_logs);
        } else {
            catch_up_logs.merge.clear_entries();
            self.apply_scheduler()
                .unwrap()
                .send(ApplyTask::LogsUpToDate(catch_up_logs));
        }
    }

    #[inline]
    fn catch_up_logs_ready(&self, catch_up_logs: &CatchUpLogs) -> bool {
        if let Some(ref state) = self.pending_merge_state
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
    // Mirrors v1::logs_up_to_date_for_merge. Called on source peer.
    pub fn on_logs_up_to_date(&mut self, catch_up_logs: CatchUpLogs) {
        info!(self.logger, "source logs are all applied now");
        if catch_up_logs
            .tx
            .send(SourceReady {
                tablet: Box::new(self.tablet().clone()),
            })
            .is_err()
        {
            error!(
                self.logger,
                "failed to respond to merge target, are we shutting down?"
            );
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Mirrors v1::on_ready_commit_merge.
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

            // let prev = meta.region_ranges.remove(&enc_end_key(&res.source));
            // assert_eq!(prev, Some(res.source.get_id()));
            // let prev = if res.region.get_end_key() == res.source.get_end_key() {
            //     meta.region_ranges.remove(&enc_start_key(&res.source))
            // } else {
            //     meta.region_ranges.remove(&enc_end_key(&res.region))
            // };
            // if prev != Some(res.region.get_id()) {
            //     slog_panic!(
            //         self.logger,
            //         "meta corrupted";
            //         "prev" => ?prev,
            //         "ranges" => ?meta.region_ranges,
            //     );
            // }
            assert!(meta.regions.remove(&res.source.get_id()).is_some());
            // if let Some(d) = meta.readers.get_mut(&res.source.get_id()) {
            //     d.mark_pending_remove();
            // }

            meta.set_region(&res.region, true, &self.logger);
            let (reader, read_tablet) = meta.readers.get_mut(&res.region.get_id()).unwrap();
            self.set_region(
                &store_ctx.coprocessor_host,
                reader,
                res.region.clone(),
                RegionChangeReason::Split,
                res.index,
            );

            // Tablet should be updated in lock to match the epoch.
            *read_tablet = SharedReadTablet::new(tablet.clone());

            // After the region commit merged, the region's key range is extended and the
            // region's `safe_ts` should reset to `min(source_safe_ts, target_safe_ts)`
            let _source_read_progress = meta
                .region_read_progress
                .remove(&res.source.get_id())
                .unwrap();
            // self.read_progress.merge_safe_ts(
            //     source_read_progress.safe_ts(),
            //     res.merge_index,
            //     &store_ctx.coprocessor_host,
            // );
        }
        if let Some(tablet) = self.set_tablet(tablet) {
            self.record_tombstone_tablet(store_ctx, tablet, res.index);
        }

        // If a follower merges into a leader, a more recent read may happen
        // on the leader of the follower. So max ts should be updated after
        // a region merge.
        self.require_updating_max_ts(store_ctx);

        // make approximate size and keys updated in time.
        // the reason why follower need to update is that there is a issue that after
        // merge and then transfer leader, the new leader may have stale size and keys.
        // self.size_diff_hint = store_ctx.cfg.region_split_check_diff().0;
        self.reset_region_buckets();

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
        self.storage_mut().set_has_dirty_data(true);

        if let Err(e) = store_ctx.router.force_send(
            res.source.get_id(),
            PeerMsg::MergeResult {
                target_region_id: self.region_id(),
                target: self.peer().clone(),
                result: MergeResultKind::FromTargetLog,
            },
        ) {
            slog_panic!(
                self.logger,
                "failed to send merge result(FromTargetLog)";
                "to_source" => res.source.get_id(),
                "err" => ?e,
            );
        }

        let region_id = self.region_id();
        let region_state = self.storage().region_state().clone();
        self.state_changes_mut()
            .put_region_state(region_id, res.index, &region_state)
            .unwrap();
        self.state_changes_mut()
            .put_dirty_mark(region_id, res.index, true)
            .unwrap();
        self.set_has_extra_write();
    }

    // Mirrors v1::on_merge_result.
    // Called on source peer.
    pub fn on_merge_result(
        &mut self,
        target_region_id: u64,
        target: metapb::Peer,
        result: MergeResultKind,
    ) {
        let exists = self.pending_merge_state.as_ref().map_or(true, |s| {
            s.get_target()
                .get_peers()
                .iter()
                .any(|p| p.get_store_id() == target.get_store_id() && p.get_id() <= target.get_id())
        });
        if !exists {
            slog_panic!(
                self.logger,
                "unexpected merge result";
                "merge_state" => ?self.pending_merge_state,
                "target" => ?target,
                "result" => ?result,
            );
        }
        // TODO(tabokie): atomic_snap_regions?
        if self.is_handling_snapshot() {
            slog_panic!(
                self.logger,
                "applying snapshot on getting merge result";
                "target_region_id" => target_region_id,
                "target" => ?target,
                "result" => ?result,
            );
        }
        if !self.storage().is_initialized() {
            slog_panic!(
                self.logger,
                "not initialized on getting merge result";
                "target_region_id" => target_region_id,
                "target" => ?target,
                "result" => ?result,
            );
        }
        match result {
            MergeResultKind::FromTargetLog => {
                info!(
                    self.logger,
                    "merge finished";
                    "target_region" => ?self.pending_merge_state.as_ref().unwrap().target,
                );
            }
            MergeResultKind::FromTargetSnapshotStep1 | MergeResultKind::FromTargetSnapshotStep2 => {
                info!(
                    self.logger,
                    "merge finished with target snapshot";
                    "target_region_id" => target_region_id,
                );
            }
            MergeResultKind::Stale => {
                // Mirrors v1::on_stale_merge.
                info!(
                    self.logger,
                    "successful merge can't be continued, try to gc stale peer";
                    "target_region_id" => target_region_id,
                    "merge_state" => ?self.pending_merge_state,
                );
            }
        };
        self.mark_for_destroy(None);
    }
}

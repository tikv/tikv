// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains everything related to transaction hook.
//!
//! This is the temporary (efficient) solution, it should be implemented as one
//! type of coprocessor.

use std::sync::{atomic::Ordering, Arc};

use crossbeam::atomic::AtomicCell;
use engine_traits::{KvEngine, RaftEngine, CF_LOCK};
use kvproto::{
    kvrpcpb::{DiskFullOpt, ExtraOp},
    metapb::Region,
    raft_cmdpb::RaftRequestHeader,
};
use parking_lot::RwLockWriteGuard;
use raft::eraftpb;
use raftstore::store::{
    LocksStatus, PeerPessimisticLocks, RaftCmdExtraOpts, TxnExt, TRANSFER_LEADER_COMMAND_REPLY_CTX,
};
use slog::{error, info, Logger};

use crate::{
    batch::StoreContext,
    raft::Peer,
    router::{PeerMsg, PeerTick},
    worker::pd,
    SimpleWriteEncoder,
};

pub struct TxnContext {
    ext: Arc<TxnExt>,
    extra_op: Arc<AtomicCell<ExtraOp>>,
    reactivate_memory_lock_ticks: usize,
}

impl Default for TxnContext {
    #[inline]
    fn default() -> Self {
        Self {
            ext: Arc::default(),
            extra_op: Arc::new(AtomicCell::new(ExtraOp::Noop)),
            reactivate_memory_lock_ticks: 0,
        }
    }
}

impl TxnContext {
    #[inline]
    pub fn on_region_changed(&self, term: u64, region: &Region) {
        let mut pessimistic_locks = self.ext.pessimistic_locks.write();
        pessimistic_locks.term = term;
        pessimistic_locks.version = region.get_region_epoch().get_version();
    }

    #[inline]
    pub fn on_became_leader<EK: KvEngine, ER: RaftEngine, T>(
        &self,
        ctx: &mut StoreContext<EK, ER, T>,
        term: u64,
        region: &Region,
        logger: &Logger,
    ) {
        // A more recent read may happen on the old leader. So max ts should
        // be updated after a peer becomes leader.
        self.require_updating_max_ts(ctx, term, region, logger);

        // Init the in-memory pessimistic lock table when the peer becomes leader.
        let mut pessimistic_locks = self.ext.pessimistic_locks.write();
        pessimistic_locks.status = LocksStatus::Normal;
        pessimistic_locks.term = term;
        pessimistic_locks.version = region.get_region_epoch().get_version();
    }

    #[inline]
    pub fn after_commit_merge<EK: KvEngine, ER: RaftEngine, T>(
        &self,
        ctx: &StoreContext<EK, ER, T>,
        term: u64,
        region: &Region,
        logger: &Logger,
    ) {
        // If a follower merges into a leader, a more recent read may happen
        // on the leader of the follower. So max ts should be updated after
        // a region merge.
        self.require_updating_max_ts(ctx, term, region, logger);
    }

    #[inline]
    pub fn on_became_follower(&self, term: u64, region: &Region) {
        let mut pessimistic_locks = self.ext.pessimistic_locks.write();
        pessimistic_locks.status = LocksStatus::NotLeader;
        pessimistic_locks.clear();
        pessimistic_locks.term = term;
        pessimistic_locks.version = region.get_region_epoch().get_version();
    }

    #[inline]
    pub fn ext(&self) -> &Arc<TxnExt> {
        &self.ext
    }

    #[inline]
    pub fn extra_op(&self) -> &Arc<AtomicCell<ExtraOp>> {
        &self.extra_op
    }

    fn require_updating_max_ts<EK, ER, T>(
        &self,
        ctx: &StoreContext<EK, ER, T>,
        term: u64,
        region: &Region,
        logger: &Logger,
    ) where
        EK: KvEngine,
        ER: RaftEngine,
    {
        let epoch = region.get_region_epoch();
        let term_low_bits = term & ((1 << 32) - 1); // 32 bits
        let version_lot_bits = epoch.get_version() & ((1 << 31) - 1); // 31 bits
        let initial_status = (term_low_bits << 32) | (version_lot_bits << 1);
        self.ext
            .max_ts_sync_status
            .store(initial_status, Ordering::SeqCst);
        info!(
            logger,
            "require updating max ts";
            "initial_status" => initial_status,
        );
        let task = pd::Task::UpdateMaxTimestamp {
            region_id: region.get_id(),
            initial_status,
            txn_ext: self.ext.clone(),
        };
        if let Err(e) = ctx.schedulers.pd.schedule(task) {
            error!(logger, "failed to notify pd with UpdateMaxTimestamp"; "err" => ?e);
        }
    }

    pub fn split(&self, regions: &[Region], derived: &Region) -> Vec<PeerPessimisticLocks> {
        // Group in-memory pessimistic locks in the original region into new regions.
        // The locks of new regions will be put into the corresponding new regions
        // later. And the locks belonging to the old region will stay in the original
        // map.
        let mut pessimistic_locks = self.ext.pessimistic_locks.write();
        // Update the version so the concurrent reader will fail due to EpochNotMatch
        // instead of PessimisticLockNotFound.
        pessimistic_locks.version = derived.get_region_epoch().get_version();
        pessimistic_locks.group_by_regions(regions, derived)
    }

    pub fn init_with_lock(&self, locks: PeerPessimisticLocks) {
        let mut pessimistic_locks = self.ext.pessimistic_locks.write();
        *pessimistic_locks = locks;
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Returns True means the tick is consumed, otherwise the tick should be
    /// rescheduled.
    pub fn on_reactivate_memory_lock_tick<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        // If it is not leader, we needn't reactivate by tick. In-memory pessimistic
        // lock will be enabled when this region becomes leader again.
        if !self.is_leader() {
            return;
        }

        let transferring_leader = self.raft_group().raft.lead_transferee.is_some();
        let txn_context = self.txn_context_mut();
        let mut pessimistic_locks = txn_context.ext.pessimistic_locks.write();

        // And this tick is currently only used for the leader transfer failure case.
        if pessimistic_locks.status != LocksStatus::TransferringLeader {
            return;
        }

        txn_context.reactivate_memory_lock_ticks += 1;
        // `lead_transferee` is not set immediately after the lock status changes. So,
        // we need the tick count condition to avoid reactivating too early.
        if !transferring_leader
            && txn_context.reactivate_memory_lock_ticks >= ctx.cfg.reactive_memory_lock_timeout_tick
        {
            pessimistic_locks.status = LocksStatus::Normal;
            txn_context.reactivate_memory_lock_ticks = 0;
        } else {
            drop(pessimistic_locks);
            self.add_pending_tick(PeerTick::ReactivateMemoryLock);
        }
    }

    // Returns whether we should propose another TransferLeader command. This is
    // for:
    // - Considering the amount of pessimistic locks can be big, it can reduce
    //   unavailable time caused by waiting for the transferee catching up logs.
    // - Make transferring leader strictly after write commands that executes before
    //   proposing the locks, preventing unexpected lock loss.
    pub fn propose_locks_before_transfer_leader<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        msg: &eraftpb::Message,
    ) -> bool {
        // 1. Disable in-memory pessimistic locks.

        // Clone to make borrow checker happy when registering ticks.
        let txn_ext = self.txn_context().ext.clone();
        let mut pessimistic_locks = txn_ext.pessimistic_locks.write();

        // If the message context == TRANSFER_LEADER_COMMAND_REPLY_CTX, the message
        // is a reply to a transfer leader command before. If the locks status remain
        // in the TransferringLeader status, we can safely initiate transferring leader
        // now.
        // If it's not in TransferringLeader status now, it is probably because several
        // ticks have passed after proposing the locks in the last time and we
        // reactivate the memory locks. Then, we should propose the locks again.
        if msg.get_context() == TRANSFER_LEADER_COMMAND_REPLY_CTX
            && pessimistic_locks.status == LocksStatus::TransferringLeader
        {
            return false;
        }

        // If it is not writable, it's probably because it's a retried TransferLeader
        // and the locks have been proposed. But we still need to return true to
        // propose another TransferLeader command. Otherwise, some write requests that
        // have marked some locks as deleted will fail because raft rejects more
        // proposals.
        // It is OK to return true here if it's in other states like MergingRegion or
        // NotLeader. In those cases, the locks will fail to propose and nothing will
        // happen.
        if !pessimistic_locks.is_writable() {
            return true;
        }
        pessimistic_locks.status = LocksStatus::TransferringLeader;
        self.txn_context_mut().reactivate_memory_lock_ticks = 0;
        self.add_pending_tick(PeerTick::ReactivateMemoryLock);

        // 2. Propose pessimistic locks
        if pessimistic_locks.is_empty() {
            return false;
        }
        // FIXME: Raft command has size limit. Either limit the total size of
        // pessimistic locks in a region, or split commands here.
        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        let mut lock_count = 0;
        {
            // Downgrade to a read guard, do not block readers in the scheduler as far as
            // possible.
            let pessimistic_locks = RwLockWriteGuard::downgrade(pessimistic_locks);
            fail::fail_point!("invalidate_locks_before_transfer_leader");
            for (key, (lock, deleted)) in &*pessimistic_locks {
                if *deleted {
                    continue;
                }
                lock_count += 1;
                encoder.put(CF_LOCK, key.as_encoded(), &lock.to_lock().to_bytes());
            }
        }
        if lock_count == 0 {
            // If the map is not empty but all locks are deleted, it is possible that a
            // write command has just marked locks deleted but not proposed yet.
            // It might cause that command to fail if we skip proposing the
            // extra TransferLeader command here.
            return true;
        }
        let mut header = Box::<RaftRequestHeader>::default();
        header.set_region_id(self.region_id());
        header.set_region_epoch(self.region().get_region_epoch().clone());
        header.set_peer(self.peer().clone());
        info!(
            self.logger,
            "propose {} locks before transferring leader", lock_count;
        );
        let PeerMsg::SimpleWrite(write) = PeerMsg::simple_write_with_opt(header, encoder.encode(), RaftCmdExtraOpts {
            disk_full_opt: DiskFullOpt::AllowedOnAlmostFull,
            ..Default::default()
        }).0 else {unreachable!()};
        self.on_simple_write(
            ctx,
            write.header,
            write.data,
            write.ch,
            Some(write.extra_opts),
        );
        true
    }
}

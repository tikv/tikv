// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use crossbeam::channel::TrySendError;
use engine_traits::{data_cf_offset, KvEngine, RaftEngine};
use kvproto::import_sstpb::SstMeta;
use raftstore::{
    store::{check_sst_for_ingestion, metrics::PEER_WRITE_CMD_COUNTER, util},
    Result,
};
use slog::error;
use tikv_util::{box_try, slog_panic};

use crate::{
    batch::StoreContext,
    fsm::{ApplyResReporter, Store, StoreFsmDelegate},
    raft::{Apply, Peer},
    router::{PeerMsg, StoreTick},
    worker::tablet,
};

impl<'a, EK: KvEngine, ER: RaftEngine, T> StoreFsmDelegate<'a, EK, ER, T> {
    #[inline]
    pub fn on_cleanup_import_sst(&mut self) {
        if let Err(e) = self.fsm.store.on_cleanup_import_sst(self.store_ctx) {
            error!(self.fsm.store.logger(), "cleanup import sst failed"; "error" => ?e);
        }
        self.schedule_tick(
            StoreTick::CleanupImportSst,
            self.store_ctx.cfg.cleanup_import_sst_interval.0,
        );
    }
}

impl Store {
    #[inline]
    fn on_cleanup_import_sst<EK: KvEngine, ER: RaftEngine, T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
    ) -> Result<()> {
        let ssts = box_try!(ctx.sst_importer.list_ssts());
        if ssts.is_empty() {
            return Ok(());
        }
        let mut region_ssts: HashMap<_, Vec<_>> = HashMap::default();
        for sst in ssts {
            region_ssts
                .entry(sst.0.get_region_id())
                .or_default()
                .push(sst.0);
        }
        for (region_id, ssts) in region_ssts {
            if let Err(TrySendError::Disconnected(msg)) = ctx.router.send(region_id, PeerMsg::CleanupImportSst(ssts.into()))
                && !ctx.router.is_shutdown() {
                let PeerMsg::CleanupImportSst(ssts) = msg else { unreachable!() };
                let _ = ctx.schedulers.tablet.schedule(tablet::Task::CleanupImportSst(ssts));
            }
        }

        Ok(())
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_cleanup_import_sst<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        ssts: Box<[SstMeta]>,
    ) {
        let mut stale_ssts = Vec::from(ssts);
        let epoch = self.region().get_region_epoch();
        stale_ssts.retain(|sst| {
            fail::fail_point!("on_cleanup_import_sst", |_| true);
            util::is_epoch_stale(sst.get_region_epoch(), epoch)
        });

        // some sst needs to be kept if the log didn't flush the disk.
        let flushed_indexes = self.storage().apply_trace().flushed_indexes();
        stale_ssts.retain(|sst| {
            let off = data_cf_offset(sst.get_cf_name());
            let uuid = sst.get_uuid().to_vec();
            let sst_index = self.sst_apply_state().sst_applied_index(&uuid);
            if let Some(index) = sst_index {
                return flushed_indexes.as_ref()[off] >= index;
            }
            true
        });

        fail::fail_point!("on_cleanup_import_sst_schedule");
        if stale_ssts.is_empty() {
            return;
        }
        let uuids = stale_ssts
            .iter()
            .map(|sst| sst.get_uuid().to_vec())
            .collect();
        self.sst_apply_state().delete_ssts(uuids);
        let _ = ctx
            .schedulers
            .tablet
            .schedule(tablet::Task::CleanupImportSst(stale_ssts.into()));
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    #[inline]
    pub fn apply_ingest(&mut self, index: u64, ssts: Vec<SstMeta>) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.ingest_sst.inc();
        let mut infos = Vec::with_capacity(ssts.len());
        for sst in &ssts {
            // This may not be enough as ingest sst may not trigger flush at all.
            let off = data_cf_offset(sst.get_cf_name());
            if self.should_skip(off, index) {
                continue;
            }
            if let Err(e) = check_sst_for_ingestion(sst, self.region()) {
                error!(
                    self.logger,
                    "ingest fail";
                    "sst" => ?sst,
                    "region" => ?self.region(),
                    "error" => ?e
                );
                let _ = self.sst_importer().delete(sst);
                return Err(e);
            }
            match self.sst_importer().validate(sst) {
                Ok(meta_info) => infos.push(meta_info),
                Err(e) => {
                    slog_panic!(self.logger, "corrupted sst"; "sst" => ?sst, "error" => ?e);
                }
            }
        }
        if !infos.is_empty() {
            // Unlike v1, we can't batch ssts accross regions.
            self.flush();
            if let Err(e) = self.sst_importer().ingest(&infos, self.tablet()) {
                slog_panic!(self.logger, "ingest fail"; "ssts" => ?ssts, "error" => ?e);
            }
        }
        let uuids = infos
            .iter()
            .map(|info| info.meta.get_uuid().to_vec())
            .collect::<Vec<_>>();
        self.set_sst_applied_index(uuids, index);
        Ok(())
    }
}

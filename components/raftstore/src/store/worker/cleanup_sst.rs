// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::sync::Arc;

use kvproto::import_sstpb::SstMeta;

use crate::store::util::is_epoch_stale;
use crate::store::{StoreMsg, StoreRouter};
use engine_traits::KvEngine;
use pd_client::PdClient;
use sst_importer::SstImporter;
use std::marker::PhantomData;
use tikv_util::error;
use tikv_util::worker::Runnable;

pub enum Task {
    DeleteSst { ssts: Vec<SstMeta> },
    ValidateSst { ssts: Vec<SstMeta> },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Task::DeleteSst { ref ssts } => write!(f, "Delete {} ssts", ssts.len()),
            Task::ValidateSst { ref ssts } => write!(f, "Validate {} ssts", ssts.len()),
        }
    }
}

pub struct Runner<EK, C, S>
where
    EK: KvEngine,
    S: StoreRouter<EK>,
{
    store_id: u64,
    store_router: S,
    importer: Arc<SstImporter>,
    pd_client: Arc<C>,
    _engine: PhantomData<EK>,
}

impl<EK, C, S> Runner<EK, C, S>
where
    EK: KvEngine,
    C: PdClient,
    S: StoreRouter<EK>,
{
    pub fn new(
        store_id: u64,
        store_router: S,
        importer: Arc<SstImporter>,
        pd_client: Arc<C>,
    ) -> Runner<EK, C, S> {
        Runner {
            store_id,
            store_router,
            importer,
            pd_client,
            _engine: PhantomData,
        }
    }

    /// Deletes SST files from the importer.
    fn handle_delete_sst(&self, ssts: Vec<SstMeta>) {
        for sst in &ssts {
            let _ = self.importer.delete(sst);
        }
    }

    /// Validates whether the SST is stale or not.
    fn handle_validate_sst(&self, ssts: Vec<SstMeta>) {
        let store_id = self.store_id;
        let mut invalid_ssts = Vec::new();
        for sst in ssts {
            match self.pd_client.get_region(sst.get_range().get_start()) {
                Ok(r) => {
                    // The region id may or may not be the same as the
                    // SST file, but it doesn't matter, because the
                    // epoch of a range will not decrease anyway.
                    if is_epoch_stale(r.get_region_epoch(), sst.get_region_epoch()) {
                        // Region has not been updated.
                        continue;
                    }
                    if r.get_id() == sst.get_region_id()
                        && r.get_peers().iter().any(|p| p.get_store_id() == store_id)
                    {
                        // The SST still belongs to this store.
                        continue;
                    }
                    invalid_ssts.push(sst);
                }
                Err(e) => {
                    error!(%e; "get region failed");
                }
            }
        }

        // We need to send back the result to check for the stale
        // peer, which may ingest the stale SST before it is
        // destroyed.
        let msg = StoreMsg::ValidateSstResult { invalid_ssts };
        if let Err(e) = self.store_router.send(msg) {
            error!(%e; "send validate sst result failed");
        }
    }
}

impl<EK, C, S> Runnable for Runner<EK, C, S>
where
    EK: KvEngine,
    C: PdClient,
    S: StoreRouter<EK>,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::DeleteSst { ssts } => {
                self.handle_delete_sst(ssts);
            }
            Task::ValidateSst { ssts } => {
                self.handle_validate_sst(ssts);
            }
        }
    }
}

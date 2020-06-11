// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::sync::Arc;

use kvproto::import_sstpb::SstMeta;

use crate::store::util::is_epoch_stale;
use crate::store::{StoreMsg, StoreRouter};
use pd_client::PdClient;
use sst_importer::SSTImporter;
use tikv_util::worker::Runnable;

pub enum Task {
    DeleteSST { ssts: Vec<SstMeta> },
    ValidateSST { ssts: Vec<SstMeta> },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Task::DeleteSST { ref ssts } => write!(f, "Delete {} ssts", ssts.len()),
            Task::ValidateSST { ref ssts } => write!(f, "Validate {} ssts", ssts.len()),
        }
    }
}

pub struct Runner<C, S> {
    store_id: u64,
    store_router: S,
    importer: Arc<SSTImporter>,
    pd_client: Arc<C>,
}

impl<C: PdClient, S: StoreRouter> Runner<C, S> {
    pub fn new(
        store_id: u64,
        store_router: S,
        importer: Arc<SSTImporter>,
        pd_client: Arc<C>,
    ) -> Runner<C, S> {
        Runner {
            store_id,
            store_router,
            importer,
            pd_client,
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
                    error!("get region failed"; "err" => %e);
                }
            }
        }

        // We need to send back the result to check for the stale
        // peer, which may ingest the stale SST before it is
        // destroyed.
        let msg = StoreMsg::ValidateSSTResult { invalid_ssts };
        if let Err(e) = self.store_router.send(msg) {
            error!("send validate sst result failed"; "err" => %e);
        }
    }
}

impl<C: PdClient, S: StoreRouter> Runnable<Task> for Runner<C, S> {
    fn run(&mut self, task: Task) {
        match task {
            Task::DeleteSST { ssts } => {
                self.handle_delete_sst(ssts);
            }
            Task::ValidateSST { ssts } => {
                self.handle_validate_sst(ssts);
            }
        }
    }
}

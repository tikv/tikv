// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt, marker::PhantomData, sync::Arc};

use engine_traits::KvEngine;
use kvproto::import_sstpb::SstMeta;
use pd_client::PdClient;
use sst_importer::SstImporter;
use tikv_util::worker::Runnable;

use crate::store::StoreRouter;

pub enum Task {
    DeleteSst { ssts: Vec<SstMeta> },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Task::DeleteSst { ref ssts } => write!(f, "Delete {} ssts", ssts.len()),
        }
    }
}

pub struct Runner<EK, C, S>
where
    EK: KvEngine,
    S: StoreRouter<EK>,
{
    _store_id: u64,
    _store_router: S,
    importer: Arc<SstImporter>,
    _pd_client: Arc<C>,
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
            _store_id: store_id,
            _store_router: store_router,
            importer,
            _pd_client: pd_client,
            _engine: PhantomData,
        }
    }

    /// Deletes SST files from the importer.
    fn handle_delete_sst(&self, ssts: Vec<SstMeta>) {
        for sst in &ssts {
            let _ = self.importer.delete(sst);
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
        }
    }
}

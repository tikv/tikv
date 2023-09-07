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

pub struct Runner {
    importer: Arc<SstImporter>,
}

impl Runner{
    pub fn new(
        importer: Arc<SstImporter>,
    ) -> Runner {
        Runner {
            importer,
        }
    }

    /// Deletes SST files from the importer.
    fn handle_delete_sst(&self, ssts: Vec<SstMeta>) {
        for sst in &ssts {
            let _ = self.importer.delete(sst);
        }
    }
}

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::DeleteSst { ssts } => {
                self.handle_delete_sst(ssts);
            }
        }
    }
}

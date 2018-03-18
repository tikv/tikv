// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::sync::Arc;

use futures::Future;
use kvproto::metapb::Region;
use kvproto::importpb::SSTMeta;

use pd::PdClient;
use import::SSTImporter;
use raftstore::store::Msg;
use raftstore::store::util::is_epoch_stale;
use util::transport::SendCh;
use util::worker::Runnable;

pub enum Task {
    DeleteSST { ssts: Vec<SSTMeta> },
    ValidateSST { ssts: Vec<SSTMeta> },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Task::DeleteSST { ref ssts } => write!(f, "Delete {} ssts", ssts.len()),
            Task::ValidateSST { ref ssts } => write!(f, "Validate {} ssts", ssts.len()),
        }
    }
}

pub struct Runner<C> {
    store_id: u64,
    store_ch: SendCh<Msg>,
    importer: Arc<SSTImporter>,
    pd_client: Arc<C>,
}

impl<C: PdClient> Runner<C> {
    pub fn new(
        store_id: u64,
        store_ch: SendCh<Msg>,
        importer: Arc<SSTImporter>,
        pd_client: Arc<C>,
    ) -> Runner<C> {
        Runner {
            store_id: store_id,
            store_ch: store_ch,
            importer: importer,
            pd_client: pd_client,
        }
    }

    fn handle_delete_sst(&self, ssts: Vec<SSTMeta>) {
        for sst in &ssts {
            let _ = self.importer.delete(sst);
        }
    }

    fn handle_validate_sst(&self, ssts: Vec<SSTMeta>) {
        let store_id = self.store_id;
        // Get region info from PD, then check if the SST still belongs to the store.
        let f = &|sst: SSTMeta, res: Result<Option<Region>, _>| {
            match res {
                Ok(Some(region)) => {
                    if is_epoch_stale(region.get_region_epoch(), sst.get_region_epoch()) {
                        // Region has not been updated.
                        return Ok(None);
                    }
                    if region
                        .get_peers()
                        .into_iter()
                        .any(|p| p.get_store_id() == store_id)
                    {
                        // The SST still belongs to the store.
                        return Ok(None);
                    }
                    Ok(Some(sst))
                }
                Ok(None) => {
                    // TODO: handle merge
                    Ok(None)
                }
                Err(e) => {
                    error!("get region {} failed {:?}", sst.get_region_id(), e);
                    Err(e)
                }
            }
        };

        let mut results = Vec::new();
        for sst in ssts {
            let sst = self.pd_client
                .get_region_by_id(sst.get_region_id())
                .then(move |res| f(sst, res))
                .wait();
            if let Ok(Some(sst)) = sst {
                results.push(sst);
            }
        }

        let msg = Msg::ValidateSSTResult(results);
        if let Err(e) = self.store_ch.try_send(msg) {
            error!("send validate sst result: {:?}", e);
        }
    }
}

impl<C: PdClient> Runnable<Task> for Runner<C> {
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

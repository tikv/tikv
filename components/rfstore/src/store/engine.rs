// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::{path::PathBuf, sync::Mutex};

use crate::store::StoreMsg;
use kvengine::Engine;
use kvenginepb::ChangeSet;
use rfengine::RFEngine;
use tikv_util::mpsc;
use tikv_util::mpsc::{Receiver, Sender};

#[derive(Clone)]
pub struct Engines {
    pub kv: kvengine::Engine,
    kv_path: PathBuf,
    pub raft: rfengine::RFEngine,
    raft_path: PathBuf,
    pub meta_change_channel: Arc<Mutex<Option<(Sender<StoreMsg>, Receiver<StoreMsg>)>>>,
}

impl Engines {
    pub fn new(
        kv: kvengine::Engine,
        raft: rfengine::RFEngine,
        meta_change_channel: (Sender<StoreMsg>, Receiver<StoreMsg>),
    ) -> Self {
        let kv_path = kv.opts.dir.clone();
        let raft_path = raft.dir.clone();
        Self {
            kv,
            kv_path,
            raft,
            raft_path,
            meta_change_channel: Arc::new(Mutex::new(Some(meta_change_channel))),
        }
    }

    pub fn write_kv(&self, wb: &mut KVWriteBatch) {
        for (_, batch) in &mut wb.batches {
            self.kv.write(batch)
        }
    }
}

impl Into<engine_traits::Engines<kvengine::Engine, rfengine::RFEngine>> for Engines {
    fn into(self) -> engine_traits::Engines<Engine, RFEngine> {
        engine_traits::Engines {
            kv: self.kv.clone(),
            raft: self.raft.clone(),
        }
    }
}

pub struct KVWriteBatch {
    kv: kvengine::Engine,
    batches: HashMap<u64, kvengine::WriteBatch>,
}

impl KVWriteBatch {
    pub(crate) fn new(kv: kvengine::Engine) -> Self {
        Self {
            kv,
            batches: HashMap::new(),
        }
    }

    pub(crate) fn get_engine_wb(&mut self, region_id: u64) -> &mut kvengine::WriteBatch {
        match self.batches.entry(region_id) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(kvengine::WriteBatch::new(
                region_id,
                self.kv.opts.cfs.clone(),
            )),
        }
    }
}

#[derive(Clone)]
pub struct MetaChangeListener {
    pub sender: mpsc::Sender<StoreMsg>,
}

impl kvengine::MetaChangeListener for MetaChangeListener {
    fn on_change_set(&self, cs: ChangeSet) {
        let msg = StoreMsg::GenerateEngineChangeSet(cs);
        self.sender.send(msg).unwrap();
    }
}

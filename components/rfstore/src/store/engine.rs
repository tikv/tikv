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
    pub raft: rfengine::RFEngine,
    raft_path: PathBuf,
    #[allow(clippy::type_complexity)]
    pub meta_change_channel: Arc<Mutex<Option<(Sender<StoreMsg>, Receiver<StoreMsg>)>>>,
}

impl Engines {
    pub fn new(
        kv: kvengine::Engine,
        raft: rfengine::RFEngine,
        meta_change_channel: (Sender<StoreMsg>, Receiver<StoreMsg>),
    ) -> Self {
        let raft_path = raft.dir.clone();
        Self {
            kv,
            raft,
            raft_path,
            meta_change_channel: Arc::new(Mutex::new(Some(meta_change_channel))),
        }
    }

    pub fn write_kv(&self, wb: &mut KVWriteBatch) {
        for batch in &mut wb.batches.values_mut() {
            self.kv.write(batch)
        }
    }
}

impl From<Engines> for engine_traits::Engines<kvengine::Engine, rfengine::RFEngine> {
    fn from(engines: Engines) -> Self {
        Self {
            kv: engines.kv.clone(),
            raft: engines.raft,
        }
    }
}

pub struct KVWriteBatch {
    batches: HashMap<u64, kvengine::WriteBatch>,
}

impl KVWriteBatch {
    pub(crate) fn new() -> Self {
        Self {
            batches: HashMap::new(),
        }
    }

    pub(crate) fn get_engine_wb(&mut self, region_id: u64) -> &mut kvengine::WriteBatch {
        match self.batches.entry(region_id) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(kvengine::WriteBatch::new(region_id)),
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

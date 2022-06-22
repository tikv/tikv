// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

use kvenginepb::ChangeSet;
use tikv_util::{
    mpsc,
    mpsc::{Receiver, Sender},
};

use crate::store::StoreMsg;

#[derive(Clone)]
pub struct Engines {
    pub kv: kvengine::Engine,
    pub raft: rfengine::RfEngine,
    #[allow(clippy::type_complexity)]
    pub meta_change_channel: Arc<Mutex<Option<(Sender<StoreMsg>, Receiver<StoreMsg>)>>>,
}

impl Engines {
    pub fn new(
        kv: kvengine::Engine,
        raft: rfengine::RfEngine,
        meta_change_channel: (Sender<StoreMsg>, Receiver<StoreMsg>),
    ) -> Self {
        Self {
            kv,
            raft,
            meta_change_channel: Arc::new(Mutex::new(Some(meta_change_channel))),
        }
    }
}

impl From<Engines> for engine_traits::Engines<kvengine::Engine, rfengine::RfEngine> {
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

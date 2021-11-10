// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::{path::PathBuf, sync::Mutex};

use crate::store::StoreMsg;
use crate::*;
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
}

impl Into<engine_traits::Engines<kvengine::Engine, rfengine::RFEngine>> for Engines {
    fn into(self) -> engine_traits::Engines<Engine, RFEngine> {
        engine_traits::Engines {
            kv: self.kv.clone(),
            raft: self.raft.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MetaChangeListener {
    pub sender: mpsc::Sender<StoreMsg>,
}

impl kvengine::MetaChangeListener for MetaChangeListener {
    fn on_change_set(&self, cs: ChangeSet) {
        todo!()
    }
}

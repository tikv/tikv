// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{path::PathBuf, sync::Mutex};

use crate::store::StoreMsg;
use crate::*;
use tokio::sync::mpsc;

pub struct Engines {
    pub(crate) kv: kvengine::Engine,
    kv_path: PathBuf,
    pub(crate) raft: rfengine::RFEngine,
    raft_path: PathBuf,
    pub(crate) listener: MetaChangeListener,
}

pub struct MetaChangeListener {
    queue: Mutex<Vec<StoreMsg>>,
}

impl MetaChangeListener {
    pub(crate) fn init_msg_ch(&mut self, store_sender: mpsc::Sender<StoreMsg>) {
        todo!()
    }
}

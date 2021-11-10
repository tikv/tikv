// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::Result;
use kvengine::{Engine, Shard, ShardMeta};
use kvenginepb::ChangeSet;
use std::sync::Arc;

pub struct RecoverHandler {}

impl RecoverHandler {
    pub fn new(rf_engine: rfengine::RFEngine) -> Result<Self> {
        todo!()
    }
}

impl kvengine::RecoverHandler for RecoverHandler {
    fn recover(&self, engine: &Engine, shard: &Shard, info: &ShardMeta) -> kvengine::Result<()> {
        todo!()
    }
}

pub struct MetaIterator {}

impl MetaIterator {
    pub fn new(rf_engine: rfengine::RFEngine) -> Result<Self> {
        todo!()
    }
}

impl kvengine::MetaIterator for MetaIterator {
    fn iterate<F>(&self, f: F) -> kvengine::Result<()>
    where
        F: FnMut(ChangeSet),
    {
        todo!()
    }
}

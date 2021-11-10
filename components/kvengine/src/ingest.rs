// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::*;

pub struct IngestTree {
    pub change_set: kvenginepb::ChangeSet,
    pub passive: bool,
}

impl Engine {
    pub fn ingest(&self, tree: IngestTree) -> Result<()> {
        todo!()
    }
}

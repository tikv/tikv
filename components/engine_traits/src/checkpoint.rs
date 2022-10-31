// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use crate::Result;

pub trait Checkpointable {
    type Checkpointer: Checkpointer;

    fn new_checkpointer(&self) -> Result<Self::Checkpointer>;
}

pub trait Checkpointer {
    fn create_at(
        &mut self,
        db_out_dir: &Path,
        titan_out_dir: Option<&Path>,
        log_size_for_flush: u64,
    ) -> Result<()>;
}

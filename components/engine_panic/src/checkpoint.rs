// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use core::panic;
use std::path::Path;

use engine_traits::{Checkpointable, Checkpointer, Result};

use crate::PanicEngine;

pub struct PanicCheckpointer {}

impl Checkpointable for PanicEngine {
    type Checkpointer = PanicCheckpointer;

    fn new_checkpointer(&self) -> Result<Self::Checkpointer> {
        panic!()
    }
}

impl Checkpointer for PanicCheckpointer {
    fn create_at(
        &mut self,
        db_out_dir: &Path,
        titan_out_dir: Option<&Path>,
        log_size_for_flush: u64,
    ) -> Result<()> {
        panic!()
    }
}

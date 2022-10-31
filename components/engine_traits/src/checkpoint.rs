// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::Result;

pub trait Checkpoint {
    type Checkpointer;

    fn new_checkpointer(&self) -> Result<Self::Checkpointer>;
}

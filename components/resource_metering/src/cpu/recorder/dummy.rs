// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::recorder::RecorderHandle;
use crate::ResourceMeteringTag;

use std::marker::PhantomData;

impl ResourceMeteringTag {
    pub fn attach(&self) -> Guard {
        Guard::default()
    }
}

#[derive(Default)]
pub struct Guard {
    // A trick to impl !Send, !Sync
    _p: PhantomData<*const ()>,
}

pub fn init_recorder(_: bool) -> RecorderHandle {
    RecorderHandle::default()
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{Builder, ReqCpuConfig, RequestTag};

use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

impl RequestTag {
    pub fn attach(self: &Arc<Self>) -> Guard {
        Guard::default()
    }
}

#[derive(Default)]
pub struct Guard {
    // A trick to impl !Send, !Sync
    _p: PhantomData<*const ()>,
}

impl Builder {
    pub fn build(self) {}
}

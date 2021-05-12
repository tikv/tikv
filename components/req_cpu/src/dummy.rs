// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{ReqCpuConfig, RequestCpuReporter, RequestTags};

use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

pub fn build(_config: ReqCpuConfig) -> Arc<Mutex<RequestCpuReporter>> {
    Arc::new(Mutex::new(RequestCpuReporter::with_capacity(1)))
}

impl RequestTags {
    pub fn attach(self: &Arc<Self>) -> Guard {
        Guard::default()
    }
}

#[derive(Default)]
pub struct Guard {
    // A trick to impl !Send, !Sync
    _p: PhantomData<*const ()>,
}

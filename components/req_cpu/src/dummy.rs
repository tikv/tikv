// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collector::{Collector, CollectorId};
use crate::{Builder, RecorderConfig, RequestTag};

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

pub fn install_recorder() {}

pub struct CollectorHandle;
pub fn register_collector(collector: Box<dyn Collector>) -> CollectorHandle {
    CollectorHandle
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::collector::Collector;

pub struct CollectorHandle;
pub fn register_collector(_collector: Box<dyn Collector>) -> CollectorHandle {
    CollectorHandle
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::CpuRecords;

use std::sync::Arc;

pub trait Collector: Send {
    fn collect(&self, records: Arc<CpuRecords>);
}

#[derive(Copy, Clone, Default, Debug, Eq, PartialEq, Hash)]
pub struct CollectorId(pub(crate) u64);

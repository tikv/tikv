// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collector::Collector;
use crate::cpu::RawCpuRecords;
use crate::recorder::SubRecorder;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// A dummy `CpuRecorder` for non-Linux systems.
#[allow(dead_code)]
pub struct CpuRecorder<C> {
    collector: C,
}

impl<C> SubRecorder for CpuRecorder<C> where C: Collector<Arc<RawCpuRecords>> {}

impl<C> CpuRecorder<C>
where
    C: Collector<Arc<RawCpuRecords>>,
{
    pub fn new(collector: C, _precision_ms: Arc<AtomicU64>) -> Self {
        Self { collector }
    }
}

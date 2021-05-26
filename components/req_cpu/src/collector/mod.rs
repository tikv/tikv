// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::RequestCpuRecords;

pub trait Collector: Send {
    fn collect(&self, records: &RequestCpuRecords);
}

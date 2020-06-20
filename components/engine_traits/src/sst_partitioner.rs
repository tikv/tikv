// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SstPartitionerState<'a> {
    pub next_key: &'a [u8],
    pub current_output_file_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SstPartitionerContext<'a> {
    pub is_full_compaction: bool,
    pub is_manual_compaction: bool,
    pub output_level: i32,
    pub smallest_key: &'a [u8],
    pub largest_key: &'a [u8],
}

pub trait SstPartitioner {
    fn should_partition(&self, state: &SstPartitionerState) -> bool;
    fn reset(&self, key: &[u8]);
}

pub trait SstPartitionerFactory: Sync + Send {
    // Lifetime of the partitioner can be changed to be bounded by the factory's lifetime once
    // generic associated types is supported.
    // https://github.com/rust-lang/rfcs/blob/master/text/1598-generic_associated_types.md
    type Partitioner: SstPartitioner + 'static;

    fn name(&self) -> &CString;
    fn create_partitioner(&self, context: &SstPartitionerContext) -> Option<Self::Partitioner>;
}

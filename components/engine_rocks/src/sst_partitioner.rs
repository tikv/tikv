// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;

pub struct RocksSstPartitionerFactory<F: engine_traits::SstPartitionerFactory>(pub F);

impl<F: engine_traits::SstPartitionerFactory> rocksdb::SstPartitionerFactory
    for RocksSstPartitionerFactory<F>
{
    fn name(&self) -> &CString {
        self.0.name()
    }

    fn create_partitioner(
        &self,
        context: &rocksdb::SstPartitionerContext,
    ) -> Box<dyn rocksdb::SstPartitioner> {
        let ctx = engine_traits::SstPartitionerContext {
            is_full_compaction: context.is_full_compaction,
            is_manual_compaction: context.is_manual_compaction,
            output_level: context.output_level,
            smallest_key: context.smallest_key,
            largest_key: context.largest_key,
        };
        self.0.create_partitioner(&ctx) as _
    }
}

trait RocksSstPartitionerExt {
    fn should_partition(&self, state: &rocksdb::SstPartitionerState) -> bool;
    fn reset(&self, key: &[u8]);
}

impl RocksSstPartitionerExt for dyn engine_traits::SstPartitioner {
    fn should_partition(&self, state: &rocksdb::SstPartitionerState) -> bool {
        let st = engine_traits::SstPartitionerState {
            next_key: state.next_key,
            current_output_file_size: state.current_output_file_size,
        };
        self.should_partition(&st)
    }
    fn reset(&self, key: &[u8]) {
        self.reset(key);
    }
}

impl rocksdb::SstPartitioner for dyn RocksSstPartitionerExt {
    fn should_partition(&self, state: &rocksdb::SstPartitionerState) -> bool {
        self.should_partition(state)
    }
    fn reset(&self, key: &[u8]) {
        self.reset(key);
    }
}

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;

pub struct RocksSstPartitionerFactory<F: engine_traits::SstPartitionerFactory>(pub F);

impl<F: engine_traits::SstPartitionerFactory> rocksdb::SstPartitionerFactory
    for RocksSstPartitionerFactory<F>
{
    type Partitioner = RocksSstPartitioner<F::Partitioner>;

    fn name(&self) -> &CString {
        self.0.name()
    }

    fn create_partitioner(
        &self,
        context: &rocksdb::SstPartitionerContext<'_>,
    ) -> Option<Self::Partitioner> {
        let ctx = engine_traits::SstPartitionerContext {
            is_full_compaction: context.is_full_compaction,
            is_manual_compaction: context.is_manual_compaction,
            output_level: context.output_level,
            smallest_key: context.smallest_key,
            largest_key: context.largest_key,
        };
        self.0.create_partitioner(&ctx).map(RocksSstPartitioner)
    }
}

pub struct RocksSstPartitioner<P: engine_traits::SstPartitioner>(P);

impl<P: engine_traits::SstPartitioner> rocksdb::SstPartitioner for RocksSstPartitioner<P> {
    fn should_partition(
        &mut self,
        request: &rocksdb::SstPartitionerRequest<'_>,
    ) -> rocksdb::SstPartitionerResult {
        let req = engine_traits::SstPartitionerRequest {
            prev_user_key: request.prev_user_key,
            current_user_key: request.current_user_key,
            current_output_file_size: request.current_output_file_size,
        };
        match self.0.should_partition(&req) {
            engine_traits::SstPartitionerResult::NotRequired => {
                rocksdb::SstPartitionerResult::NotRequired
            }
            engine_traits::SstPartitionerResult::Required => {
                rocksdb::SstPartitionerResult::Required
            }
        }
    }

    fn can_do_trivial_move(&mut self, smallest_key: &[u8], largest_key: &[u8]) -> bool {
        self.0.can_do_trivial_move(smallest_key, largest_key)
    }
}

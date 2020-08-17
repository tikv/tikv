// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::Cell, ffi::CString};

use crate::coprocessor::{RegionInfoAccessor, RegionInfoProvider};
use engine_traits::{
    SstPartitioner, SstPartitionerContext, SstPartitionerFactory, SstPartitionerRequest,
    SstPartitionerResult,
};
use keys::data_end_key;

lazy_static! {
    static ref COMPACTION_GUARD: CString = CString::new(b"CompactionGuard".to_vec()).unwrap();
}

pub struct CompactionGuardGeneratorFactory {
    accessor: RegionInfoAccessor,
    min_output_file_size: u64,
    max_output_file_size: u64,
}

impl CompactionGuardGeneratorFactory {
    pub fn new(
        accessor: RegionInfoAccessor,
        min_output_file_size: u64,
        max_output_file_size: u64,
    ) -> Self {
        CompactionGuardGeneratorFactory {
            accessor,
            min_output_file_size,
            max_output_file_size,
        }
    }
}

// Update to implement engine_traits::SstPartitionerFactory instead once we move to use abstracted
// ColumnFamilyOptions in src/config.rs.
impl SstPartitionerFactory for CompactionGuardGeneratorFactory {
    type Partitioner = CompactionGuardGenerator;

    fn name(&self) -> &CString {
        &COMPACTION_GUARD
    }

    fn create_partitioner(&self, context: &SstPartitionerContext) -> Option<Self::Partitioner> {
        match self
            .accessor
            .get_regions_in_range(context.smallest_key, context.largest_key)
        {
            Ok(regions) => {
                // The regions returned from region_info_accessor should have been sorted,
                // but we sort it again just in case.
                let mut boundaries = regions
                    .iter()
                    .map(|region| data_end_key(&region.end_key))
                    .collect::<Vec<Vec<u8>>>();
                boundaries.sort();
                Some(CompactionGuardGenerator {
                    boundaries,
                    min_output_file_size: self.min_output_file_size,
                    max_output_file_size: self.max_output_file_size,
                    pos: Cell::new(0),
                })
            }
            Err(e) => {
                warn!("failed to create compaction guard generator"; "err" => ?e);
                None
            }
        }
    }
}

pub struct CompactionGuardGenerator {
    // The boundary keys are exclusive.
    boundaries: Vec<Vec<u8>>,
    min_output_file_size: u64,
    max_output_file_size: u64,
    pos: Cell<usize>,
}

impl SstPartitioner for CompactionGuardGenerator {
    fn should_partition(&self, req: &SstPartitionerRequest) -> SstPartitionerResult {
        let mut pos = self.pos.get();
        while pos < self.boundaries.len() && self.boundaries[pos].as_slice() <= req.prev_user_key {
            pos += 1;
        }
        self.pos.set(pos);
        if (req.current_output_file_size >= self.min_output_file_size)
            && ((req.current_output_file_size >= self.max_output_file_size)
                || ((pos < self.boundaries.len())
                    && (self.boundaries[pos].as_slice() <= req.current_user_key)))
        {
            SstPartitionerResult::Required
        } else {
            SstPartitionerResult::NotRequired
        }
    }

    fn can_do_trivial_move(&self, _smallest_key: &[u8], _largest_key: &[u8]) -> bool {
        // Always allow trivial move
        true
    }
}

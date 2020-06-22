// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::Cell, ffi::CString};

use crate::coprocessor::{RegionInfoAccessor, RegionInfoProvider};
use engine_rocks::raw::{
    SstPartitioner, SstPartitionerContext, SstPartitionerFactory, SstPartitionerState,
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
    fn should_partition(&self, state: &SstPartitionerState) -> bool {
        let pos = self.pos.get();
        (state.current_output_file_size >= self.min_output_file_size)
            && ((state.current_output_file_size >= self.max_output_file_size)
                || ((pos < self.boundaries.len())
                    && (self.boundaries[pos].as_slice() <= state.next_key)))
    }

    fn reset(&self, key: &[u8]) {
        let mut pos = self.pos.get();
        while pos < self.boundaries.len() && self.boundaries[pos].as_slice() <= key {
            pos += 1;
        }
        self.pos.set(pos);
    }
}

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::Cell, ffi::CString};

use crate::coprocessor::{RegionInfoAccessor, RegionInfoProvider};
use engine_traits::{
    SstPartitioner, SstPartitionerContext, SstPartitionerFactory, SstPartitionerRequest,
    SstPartitionerResult,
};
use keys::data_end_key;

const COMPACTION_GUARD_MAX_POS_SKIP: u32 = 10;

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
        let mut skip_count = 0;
        while pos < self.boundaries.len() && self.boundaries[pos].as_slice() <= req.prev_user_key {
            pos += 1;
            skip_count += 1;
            if skip_count >= COMPACTION_GUARD_MAX_POS_SKIP {
                let prev_user_key = req.prev_user_key.to_vec();
                pos = match self.boundaries.binary_search(&prev_user_key) {
                    Ok(search_pos) => search_pos + 1,
                    Err(search_pos) => search_pos,
                };
                break;
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_guard_should_partition() {
        let guard = CompactionGuardGenerator {
            boundaries: vec![b"bbb".to_vec(), b"ccc".to_vec()],
            min_output_file_size: 8 << 20,   // 8MB
            max_output_file_size: 128 << 20, // 128MB
            pos: Cell::new(0),
        };
        // Crossing region boundary.
        let mut req = SstPartitionerRequest {
            prev_user_key: b"bba",
            current_user_key: b"bbz",
            current_output_file_size: 32 << 20,
        };
        assert_eq!(guard.should_partition(&req), SstPartitionerResult::Required);
        assert_eq!(guard.pos.get(), 0);
        // Output file size too small.
        req = SstPartitionerRequest {
            prev_user_key: b"bba",
            current_user_key: b"bbz",
            current_output_file_size: 4 << 20,
        };
        assert_eq!(
            guard.should_partition(&req),
            SstPartitionerResult::NotRequired
        );
        assert_eq!(guard.pos.get(), 0);
        // Not crossing boundary.
        req = SstPartitionerRequest {
            prev_user_key: b"aaa",
            current_user_key: b"aaz",
            current_output_file_size: 32 << 20,
        };
        assert_eq!(
            guard.should_partition(&req),
            SstPartitionerResult::NotRequired
        );
        assert_eq!(guard.pos.get(), 0);
        // Output file size too large.
        req = SstPartitionerRequest {
            prev_user_key: b"bba",
            current_user_key: b"bbz",
            current_output_file_size: 256 << 20,
        };
        assert_eq!(guard.should_partition(&req), SstPartitionerResult::Required);
        assert_eq!(guard.pos.get(), 0);
        // Move position
        req = SstPartitionerRequest {
            prev_user_key: b"cca",
            current_user_key: b"ccz",
            current_output_file_size: 32 << 20,
        };
        assert_eq!(guard.should_partition(&req), SstPartitionerResult::Required);
        assert_eq!(guard.pos.get(), 1);
    }

    #[test]
    fn test_compaction_guard_should_partition_binary_search() {
        let guard = CompactionGuardGenerator {
            boundaries: vec![
                b"aaa00".to_vec(),
                b"aaa01".to_vec(),
                b"aaa02".to_vec(),
                b"aaa03".to_vec(),
                b"aaa04".to_vec(),
                b"aaa05".to_vec(),
                b"aaa06".to_vec(),
                b"aaa07".to_vec(),
                b"aaa08".to_vec(),
                b"aaa09".to_vec(),
                b"aaa10".to_vec(),
                b"aaa11".to_vec(),
                b"aaa12".to_vec(),
                b"aaa13".to_vec(),
                b"aaa14".to_vec(),
                b"aaa15".to_vec(),
            ],
            min_output_file_size: 8 << 20,   // 8MB
            max_output_file_size: 128 << 20, // 128MB
            pos: Cell::new(0),
        };
        // Binary search meet exact match.
        guard.pos.set(0);
        let mut req = SstPartitionerRequest {
            prev_user_key: b"aaa12",
            current_user_key: b"aaa131",
            current_output_file_size: 32 << 20,
        };
        assert_eq!(guard.should_partition(&req), SstPartitionerResult::Required);
        assert_eq!(guard.pos.get(), 13);
        // Binary search doesn't find exact match.
        guard.pos.set(0);
        req = SstPartitionerRequest {
            prev_user_key: b"aaa121",
            current_user_key: b"aaa122",
            current_output_file_size: 32 << 20,
        };
        assert_eq!(
            guard.should_partition(&req),
            SstPartitionerResult::NotRequired
        );
        assert_eq!(guard.pos.get(), 13);
    }
}

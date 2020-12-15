// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::Cell, ffi::CString};

use crate::coprocessor::RegionInfoProvider;
use engine_traits::{
    SstPartitioner, SstPartitionerContext, SstPartitionerFactory, SstPartitionerRequest,
    SstPartitionerResult,
};
use keys::data_end_key;

use super::metrics::*;

const COMPACTION_GUARD_MAX_POS_SKIP: u32 = 10;

lazy_static! {
    static ref COMPACTION_GUARD: CString = CString::new(b"CompactionGuard".to_vec()).unwrap();
}

pub struct CompactionGuardGeneratorFactory<P: RegionInfoProvider> {
    provider: P,
    min_output_file_size: u64,
}

impl<P: RegionInfoProvider> CompactionGuardGeneratorFactory<P> {
    pub fn new(provider: P, min_output_file_size: u64) -> Self {
        CompactionGuardGeneratorFactory {
            provider,
            min_output_file_size,
        }
    }
}

// Update to implement engine_traits::SstPartitionerFactory instead once we move to use abstracted
// ColumnFamilyOptions in src/config.rs.
impl<P: RegionInfoProvider + Sync> SstPartitionerFactory for CompactionGuardGeneratorFactory<P> {
    type Partitioner = CompactionGuardGenerator;

    fn name(&self) -> &CString {
        &COMPACTION_GUARD
    }

    fn create_partitioner(&self, context: &SstPartitionerContext) -> Option<Self::Partitioner> {
        match self
            .provider
            .get_regions_in_range(context.smallest_key, context.largest_key)
        {
            Ok(regions) => {
                // The regions returned from region_info_provider should have been sorted,
                // but we sort it again just in case.
                COMPACTION_GUARD_ACTION_COUNTER.create.inc();
                let mut boundaries = regions
                    .iter()
                    .map(|region| data_end_key(&region.end_key))
                    .collect::<Vec<Vec<u8>>>();
                boundaries.sort();
                Some(CompactionGuardGenerator {
                    boundaries,
                    min_output_file_size: self.min_output_file_size,
                    pos: Cell::new(0),
                })
            }
            Err(e) => {
                COMPACTION_GUARD_ACTION_COUNTER.create_failure.inc();
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
        if pos < self.boundaries.len() && self.boundaries[pos].as_slice() <= req.current_user_key {
            if req.current_output_file_size >= self.min_output_file_size {
                COMPACTION_GUARD_ACTION_COUNTER.partition.inc();
                SstPartitionerResult::Required
            } else {
                COMPACTION_GUARD_ACTION_COUNTER.skip_partition.inc();
                SstPartitionerResult::NotRequired
            }
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
    use crate::coprocessor::Result as CoprocessorResult;
    use engine_rocks::{
        raw::{BlockBasedOptions, ColumnFamilyOptions, DBCompressionType, DBOptions},
        raw_util::{new_engine_opt, CFOptions},
        RocksEngine, RocksSstPartitionerFactory, RocksSstReader,
    };
    use engine_traits::{
        CompactExt, Iterator, MiscExt, SeekKey, SstReader, SyncMutable, CF_DEFAULT,
    };
    use keys::DATA_PREFIX_KEY;
    use kvproto::metapb::Region;
    use std::{
        str,
        sync::{Arc, Mutex},
    };
    use tempfile::TempDir;

    #[test]
    fn test_compaction_guard_should_partition() {
        let guard = CompactionGuardGenerator {
            boundaries: vec![b"bbb".to_vec(), b"ccc".to_vec()],
            min_output_file_size: 8 << 20, // 8MB
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
            min_output_file_size: 8 << 20, // 8MB
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

    struct MockProvider(Mutex<Vec<Region>>);

    impl Clone for MockProvider {
        fn clone(&self) -> Self {
            MockProvider(Mutex::new(self.0.lock().unwrap().clone()))
        }
    }

    impl RegionInfoProvider for MockProvider {
        fn get_regions_in_range(
            &self,
            _start_key: &[u8],
            _end_key: &[u8],
        ) -> CoprocessorResult<Vec<Region>> {
            Ok(self.0.lock().unwrap().clone())
        }
    }

    const MIN_OUTPUT_FILE_SIZE: u64 = 1024;
    const MAX_OUTPUT_FILE_SIZE: u64 = 4096;

    fn new_test_db(provider: MockProvider) -> (RocksEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_target_file_size_base(MAX_OUTPUT_FILE_SIZE);
        cf_opts.set_sst_partitioner_factory(RocksSstPartitionerFactory(
            CompactionGuardGeneratorFactory::new(provider, MIN_OUTPUT_FILE_SIZE),
        ));
        cf_opts.set_disable_auto_compactions(true);
        cf_opts.compression_per_level(&[
            DBCompressionType::No,
            DBCompressionType::No,
            DBCompressionType::No,
            DBCompressionType::No,
            DBCompressionType::No,
            DBCompressionType::No,
            DBCompressionType::No,
        ]);
        // Make block size small to make sure current_output_file_size passed to SstPartitioner
        // is accurate.
        let mut block_based_opts = BlockBasedOptions::new();
        block_based_opts.set_block_size(100);
        cf_opts.set_block_based_table_factory(&block_based_opts);

        let db = RocksEngine::from_db(Arc::new(
            new_engine_opt(
                temp_dir.path().to_str().unwrap(),
                DBOptions::new(),
                vec![CFOptions::new(CF_DEFAULT, cf_opts)],
            )
            .unwrap(),
        ));
        (db, temp_dir)
    }

    fn collect_keys(path: &str) -> Vec<Vec<u8>> {
        let mut sst_reader = RocksSstReader::open(path).unwrap().iter();
        let mut valid = sst_reader.seek(SeekKey::Start).unwrap();
        let mut ret = vec![];
        while valid {
            ret.push(sst_reader.key().to_owned());
            valid = sst_reader.next().unwrap();
        }
        ret
    }

    #[test]
    fn test_compaction_guard_with_rocks() {
        let provider = MockProvider(Mutex::new(vec![
            Region {
                id: 1,
                start_key: b"a".to_vec(),
                end_key: b"b".to_vec(),
                ..Default::default()
            },
            Region {
                id: 1,
                start_key: b"b".to_vec(),
                end_key: b"c".to_vec(),
                ..Default::default()
            },
            Region {
                id: 1,
                start_key: b"c".to_vec(),
                end_key: b"d".to_vec(),
                ..Default::default()
            },
        ]));
        let (db, dir) = new_test_db(provider);

        // The following test assume data key starts with "z".
        assert_eq!(b"z", DATA_PREFIX_KEY);

        // Create two overlapping SST files then force compaction.
        // Region "a" will share a SST file with region "b", since region "a" is too small.
        // Region "c" will be splitted into two SSTs, since its size is larger than
        // target_file_size_base.
        let value = vec![b'v'; 1024];
        db.put(b"za1", b"").unwrap();
        db.put(b"zb1", &value).unwrap();
        db.put(b"zc1", &value).unwrap();
        db.flush(true /*sync*/).unwrap();
        db.put(b"zb2", &value).unwrap();
        db.put(b"zc2", &value).unwrap();
        db.put(b"zc3", &value).unwrap();
        db.put(b"zc4", &value).unwrap();
        db.put(b"zc5", &value).unwrap();
        db.put(b"zc6", &value).unwrap();
        db.flush(true /*sync*/).unwrap();
        db.compact_range(
            CF_DEFAULT, None,  /*start_key*/
            None,  /*end_key*/
            false, /*exclusive_manual*/
            1,     /*max_subcompactions*/
        )
        .unwrap();

        let files = dir.path().read_dir().unwrap();
        let mut sst_files = files
            .map(|entry| entry.unwrap().path().to_str().unwrap().to_owned())
            .filter(|entry| entry.ends_with(".sst"))
            .collect::<Vec<String>>();
        sst_files.sort();
        assert_eq!(3, sst_files.len());
        assert_eq!(collect_keys(&sst_files[0]), [b"za1", b"zb1", b"zb2"]);
        assert_eq!(
            collect_keys(&sst_files[1]),
            [b"zc1", b"zc2", b"zc3", b"zc4", b"zc5"]
        );
        assert_eq!(collect_keys(&sst_files[2]), [b"zc6"]);
    }
}

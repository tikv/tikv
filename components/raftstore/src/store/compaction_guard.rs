// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;

use engine_traits::{
    CfName, SstPartitioner, SstPartitionerContext, SstPartitionerFactory, SstPartitionerRequest,
    SstPartitionerResult, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE,
};
use keys::{data_end_key, origin_key};
use lazy_static::lazy_static;
use tikv_util::warn;

use super::metrics::*;
use crate::{coprocessor::RegionInfoProvider, Error, Result};

const COMPACTION_GUARD_MAX_POS_SKIP: u32 = 10;

lazy_static! {
    static ref COMPACTION_GUARD: CString = CString::new(b"CompactionGuard".to_vec()).unwrap();
}

pub struct CompactionGuardGeneratorFactory<P: RegionInfoProvider> {
    cf_name: CfNames,
    provider: P,
    min_output_file_size: u64,
}

impl<P: RegionInfoProvider> CompactionGuardGeneratorFactory<P> {
    pub fn new(cf: CfName, provider: P, min_output_file_size: u64) -> Result<Self> {
        let cf_name = match cf {
            CF_DEFAULT => CfNames::default,
            CF_LOCK => CfNames::lock,
            CF_WRITE => CfNames::write,
            CF_RAFT => CfNames::raft,
            _ => {
                return Err(Error::Other(From::from(format!(
                    "fail to enable compaction guard, unrecognized cf name: {}",
                    cf
                ))));
            }
        };
        Ok(CompactionGuardGeneratorFactory {
            cf_name,
            provider,
            min_output_file_size,
        })
    }
}

// Update to implement engine_traits::SstPartitionerFactory instead once we move to use abstracted
// ColumnFamilyOptions in src/config.rs.
impl<P: RegionInfoProvider + Clone + 'static> SstPartitionerFactory
    for CompactionGuardGeneratorFactory<P>
{
    type Partitioner = CompactionGuardGenerator<P>;

    fn name(&self) -> &CString {
        &COMPACTION_GUARD
    }

    fn create_partitioner(&self, context: &SstPartitionerContext<'_>) -> Option<Self::Partitioner> {
        // create_partitioner can be called in RocksDB while holding db_mutex. It can block
        // other operations on RocksDB. To avoid such caces, we defer region info query to
        // the first time should_partition is called.
        Some(CompactionGuardGenerator {
            cf_name: self.cf_name,
            smallest_key: context.smallest_key.to_vec(),
            largest_key: context.largest_key.to_vec(),
            min_output_file_size: self.min_output_file_size,
            provider: self.provider.clone(),
            initialized: false,
            use_guard: false,
            boundaries: vec![],
            pos: 0,
        })
    }
}

pub struct CompactionGuardGenerator<P: RegionInfoProvider> {
    cf_name: CfNames,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
    min_output_file_size: u64,
    provider: P,
    initialized: bool,
    use_guard: bool,
    // The boundary keys are exclusive.
    boundaries: Vec<Vec<u8>>,
    pos: usize,
}

impl<P: RegionInfoProvider> CompactionGuardGenerator<P> {
    fn initialize(&mut self) {
        // The range may include non-data keys which are not included in any region,
        // such as `STORE_IDENT_KEY`, `REGION_RAFT_KEY` and `REGION_META_KEY`,
        // so check them and get covered regions only for the range of data keys.
        let res = match (
            self.smallest_key.starts_with(keys::DATA_PREFIX_KEY),
            self.largest_key.starts_with(keys::DATA_PREFIX_KEY),
        ) {
            (true, true) => Some((
                origin_key(&self.smallest_key),
                origin_key(&self.largest_key),
            )),
            (true, false) => Some((origin_key(&self.smallest_key), "".as_bytes())),
            (false, true) => Some(("".as_bytes(), origin_key(&self.largest_key))),
            (false, false) => {
                if self.smallest_key.as_slice() < keys::DATA_MIN_KEY
                    && self.largest_key.as_slice() >= keys::DATA_MAX_KEY
                {
                    Some(("".as_bytes(), "".as_bytes()))
                } else {
                    None
                }
            }
        };
        self.use_guard = if let Some((start, end)) = res {
            match self.provider.get_regions_in_range(start, end) {
                Ok(regions) => {
                    // The regions returned from region_info_provider should have been sorted,
                    // but we sort it again just in case.
                    COMPACTION_GUARD_ACTION_COUNTER.get(self.cf_name).init.inc();
                    let mut boundaries = regions
                        .iter()
                        .map(|region| data_end_key(&region.end_key))
                        .collect::<Vec<Vec<u8>>>();
                    boundaries.sort();
                    self.boundaries = boundaries;
                    true
                }
                Err(e) => {
                    COMPACTION_GUARD_ACTION_COUNTER
                        .get(self.cf_name)
                        .init_failure
                        .inc();
                    warn!("failed to initialize compaction guard generator"; "err" => ?e);
                    false
                }
            }
        } else {
            false
        };
        self.pos = 0;
        self.initialized = true;
    }
}

impl<P: RegionInfoProvider> SstPartitioner for CompactionGuardGenerator<P> {
    fn should_partition(&mut self, req: &SstPartitionerRequest<'_>) -> SstPartitionerResult {
        if !self.initialized {
            self.initialize();
        }
        if !self.use_guard {
            return SstPartitionerResult::NotRequired;
        }
        let mut pos = self.pos;
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
        self.pos = pos;
        if pos < self.boundaries.len() && self.boundaries[pos].as_slice() <= req.current_user_key {
            if req.current_output_file_size >= self.min_output_file_size {
                COMPACTION_GUARD_ACTION_COUNTER
                    .get(self.cf_name)
                    .partition
                    .inc();
                SstPartitionerResult::Required
            } else {
                COMPACTION_GUARD_ACTION_COUNTER
                    .get(self.cf_name)
                    .skip_partition
                    .inc();
                SstPartitionerResult::NotRequired
            }
        } else {
            SstPartitionerResult::NotRequired
        }
    }

    fn can_do_trivial_move(&mut self, _smallest_key: &[u8], _largest_key: &[u8]) -> bool {
        // Always allow trivial move
        true
    }
}

#[cfg(test)]
mod tests {
    use std::{str, sync::Arc};

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
    use tempfile::TempDir;

    use super::*;
    use crate::coprocessor::region_info_accessor::MockRegionInfoProvider;

    #[test]
    fn test_compaction_guard_non_data() {
        let mut guard = CompactionGuardGenerator {
            cf_name: CfNames::default,
            smallest_key: vec![],
            largest_key: vec![],
            min_output_file_size: 8 << 20, // 8MB
            provider: MockRegionInfoProvider::new(vec![]),
            initialized: false,
            use_guard: false,
            boundaries: vec![],
            pos: 0,
        };

        guard.smallest_key = keys::LOCAL_MIN_KEY.to_vec();
        guard.largest_key = keys::LOCAL_MAX_KEY.to_vec();
        guard.initialize();
        assert_eq!(guard.use_guard, false);

        guard.smallest_key = keys::LOCAL_MIN_KEY.to_vec();
        guard.largest_key = keys::DATA_MIN_KEY.to_vec();
        guard.initialize();
        assert_eq!(guard.use_guard, true);

        guard.smallest_key = keys::LOCAL_MIN_KEY.to_vec();
        guard.largest_key = keys::DATA_MAX_KEY.to_vec();
        guard.initialize();
        assert_eq!(guard.use_guard, true);

        guard.smallest_key = keys::DATA_MIN_KEY.to_vec();
        guard.largest_key = keys::DATA_MAX_KEY.to_vec();
        guard.initialize();
        assert_eq!(guard.use_guard, true);

        guard.smallest_key = keys::DATA_MIN_KEY.to_vec();
        guard.largest_key = vec![keys::DATA_PREFIX + 10];
        guard.initialize();
        assert_eq!(guard.use_guard, true);

        guard.smallest_key = keys::DATA_MAX_KEY.to_vec();
        guard.largest_key = vec![keys::DATA_PREFIX + 10];
        guard.initialize();
        assert_eq!(guard.use_guard, false);
    }

    #[test]
    fn test_compaction_guard_should_partition() {
        let mut guard = CompactionGuardGenerator {
            cf_name: CfNames::default,
            smallest_key: vec![],
            largest_key: vec![],
            min_output_file_size: 8 << 20, // 8MB
            provider: MockRegionInfoProvider::new(vec![]),
            initialized: true,
            use_guard: true,
            boundaries: vec![b"bbb".to_vec(), b"ccc".to_vec()],
            pos: 0,
        };
        // Crossing region boundary.
        let mut req = SstPartitionerRequest {
            prev_user_key: b"bba",
            current_user_key: b"bbz",
            current_output_file_size: 32 << 20,
        };
        assert_eq!(guard.should_partition(&req), SstPartitionerResult::Required);
        assert_eq!(guard.pos, 0);
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
        assert_eq!(guard.pos, 0);
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
        assert_eq!(guard.pos, 0);
        // Move position
        req = SstPartitionerRequest {
            prev_user_key: b"cca",
            current_user_key: b"ccz",
            current_output_file_size: 32 << 20,
        };
        assert_eq!(guard.should_partition(&req), SstPartitionerResult::Required);
        assert_eq!(guard.pos, 1);
    }

    #[test]
    fn test_compaction_guard_should_partition_binary_search() {
        let mut guard = CompactionGuardGenerator {
            cf_name: CfNames::default,
            smallest_key: vec![],
            largest_key: vec![],
            min_output_file_size: 8 << 20, // 8MB
            provider: MockRegionInfoProvider::new(vec![]),
            initialized: true,
            use_guard: true,
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
            pos: 0,
        };
        // Binary search meet exact match.
        guard.pos = 0;
        let mut req = SstPartitionerRequest {
            prev_user_key: b"aaa12",
            current_user_key: b"aaa131",
            current_output_file_size: 32 << 20,
        };
        assert_eq!(guard.should_partition(&req), SstPartitionerResult::Required);
        assert_eq!(guard.pos, 13);
        // Binary search doesn't find exact match.
        guard.pos = 0;
        req = SstPartitionerRequest {
            prev_user_key: b"aaa121",
            current_user_key: b"aaa122",
            current_output_file_size: 32 << 20,
        };
        assert_eq!(
            guard.should_partition(&req),
            SstPartitionerResult::NotRequired
        );
        assert_eq!(guard.pos, 13);
    }

    const MIN_OUTPUT_FILE_SIZE: u64 = 1024;
    const MAX_OUTPUT_FILE_SIZE: u64 = 4096;

    fn new_test_db(provider: MockRegionInfoProvider) -> (RocksEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_target_file_size_base(MAX_OUTPUT_FILE_SIZE);
        cf_opts.set_sst_partitioner_factory(RocksSstPartitionerFactory(
            CompactionGuardGeneratorFactory::new(CF_DEFAULT, provider, MIN_OUTPUT_FILE_SIZE)
                .unwrap(),
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
        let provider = MockRegionInfoProvider::new(vec![
            Region {
                id: 1,
                start_key: b"a".to_vec(),
                end_key: b"b".to_vec(),
                ..Default::default()
            },
            Region {
                id: 2,
                start_key: b"b".to_vec(),
                end_key: b"c".to_vec(),
                ..Default::default()
            },
            Region {
                id: 3,
                start_key: b"c".to_vec(),
                end_key: b"d".to_vec(),
                ..Default::default()
            },
        ]);
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

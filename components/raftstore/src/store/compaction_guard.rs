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
    max_compaction_size: u64,
}

impl<P: RegionInfoProvider> CompactionGuardGeneratorFactory<P> {
    pub fn new(
        cf: CfName,
        provider: P,
        min_output_file_size: u64,
        max_compaction_size: u64,
    ) -> Result<Self> {
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
            max_compaction_size,
        })
    }
}

// Update to implement engine_traits::SstPartitionerFactory instead once we move
// to use abstracted CfOptions in src/config.rs.
impl<P: RegionInfoProvider + Clone + 'static> SstPartitionerFactory
    for CompactionGuardGeneratorFactory<P>
{
    type Partitioner = CompactionGuardGenerator<P>;

    fn name(&self) -> &CString {
        &COMPACTION_GUARD
    }

    fn create_partitioner(&self, context: &SstPartitionerContext<'_>) -> Option<Self::Partitioner> {
        // create_partitioner can be called in RocksDB while holding db_mutex. It can
        // block other operations on RocksDB. To avoid such cases, we defer
        // region info query to the first time should_partition is called.
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
            next_level_pos: 0,
            next_level_boundaries: context
                .next_level_boundaries
                .iter()
                .map(|v| v.to_vec())
                .collect(),
            next_level_size: context.next_level_sizes.clone(),
            current_next_level_size: 0,
            max_compaction_size: self.max_compaction_size,
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
    /// The SST boundaries overlapped with the compaction input at the next
    /// level of output level (let we call it L+2). When the output level is the
    /// bottom-most level(usually L6), this will be empty. The boundaries
    /// are the first key of the first sst concatenating with all ssts' end key.
    next_level_boundaries: Vec<Vec<u8>>,
    /// The size of each "segment" of L+2. If the `next_level_boundaries`(let we
    /// call it NLB) isn't empty, `next_level_size` will have length
    /// `NLB.len() - 1`, and at the position `N` stores the size of range
    /// `[NLB[N], NLB[N+1]]` in L+2.
    next_level_size: Vec<usize>,
    pos: usize,
    next_level_pos: usize,
    current_next_level_size: u64,
    max_compaction_size: u64,
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
            match self.provider.get_regions_in_range(start, end, false) {
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
        self.pos = seek_to(&self.boundaries, req.prev_user_key, self.pos);
        // Generally this shall be a noop... because each time we are moving the cursor
        // to the previous key.
        let left_next_level_pos = seek_to(
            &self.next_level_boundaries,
            req.prev_user_key,
            self.next_level_pos,
        );
        let right_next_level_pos = seek_to(
            &self.next_level_boundaries,
            req.current_user_key,
            left_next_level_pos,
        );
        // The cursor has been moved.
        if right_next_level_pos > left_next_level_pos {
            self.current_next_level_size += self.next_level_size
                [left_next_level_pos..right_next_level_pos - 1]
                .iter()
                .map(|x| *x as u64)
                .sum::<u64>();
        }
        self.next_level_pos = right_next_level_pos;

        if self.pos < self.boundaries.len()
            && self.boundaries[self.pos].as_slice() <= req.current_user_key
        {
            if req.current_output_file_size >= self.min_output_file_size
                // Or, the output file may make a huge compaction even greater than the max compaction size.
                || self.current_next_level_size >= self.max_compaction_size
            {
                COMPACTION_GUARD_ACTION_COUNTER
                    .get(self.cf_name)
                    .partition
                    .inc();
                // The current pointer status should be like (let * be the current pos, ^ be
                // where the previous user key is):
                // boundaries: A   B   C   D
                // size:           1   3   2
                //                   ^ *
                // You will notice that the previous user key is between B and C, which indices
                // that there must still be something between previous user key and C.
                // We still set `current_next_level_size` to zero here, so the segment will be
                // forgotten. I think that will be acceptable given generally a segment won't be
                // greater than the `max-sst-size`, which is tiny comparing to the
                // `max-compaction-size` usually.
                self.current_next_level_size = 0;
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

fn seek_to(all_data: &[Vec<u8>], target_key: &[u8], from_pos: usize) -> usize {
    let mut pos = from_pos;
    let mut skip_count = 0;
    while pos < all_data.len() && all_data[pos].as_slice() <= target_key {
        pos += 1;
        skip_count += 1;
        if skip_count >= COMPACTION_GUARD_MAX_POS_SKIP {
            pos = match all_data.binary_search_by(|probe| probe.as_slice().cmp(target_key)) {
                Ok(search_pos) => search_pos + 1,
                Err(search_pos) => search_pos,
            };
            break;
        }
    }
    pos
}

#[cfg(test)]
mod tests {
    use std::{path::Path, str};

    use collections::HashMap;
    use engine_rocks::{
        raw::{BlockBasedOptions, DBCompressionType},
        util::new_engine_opt,
        RocksCfOptions, RocksDbOptions, RocksEngine, RocksSstPartitionerFactory, RocksSstReader,
    };
    use engine_traits::{
        CompactExt, IterOptions, Iterator, ManualCompactionOptions, MiscExt, RefIterable,
        SstReader, SyncMutable, CF_DEFAULT,
    };
    use keys::DATA_PREFIX_KEY;
    use kvproto::metapb::Region;
    use tempfile::TempDir;

    use super::*;
    use crate::coprocessor::region_info_accessor::MockRegionInfoProvider;

    impl<G: RegionInfoProvider> CompactionGuardGenerator<G> {
        fn reset_next_level_size_state(&mut self) {
            self.current_next_level_size = 0;
            self.next_level_pos = 0;
        }
    }

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
            current_next_level_size: 0,
            next_level_pos: 0,
            next_level_boundaries: vec![],
            next_level_size: vec![],
            max_compaction_size: 1 << 30,
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
            boundaries: vec![b"bbb".to_vec(), b"ccc".to_vec(), b"ddd".to_vec()],
            pos: 0,
            current_next_level_size: 0,
            next_level_pos: 0,
            next_level_boundaries: (0..10)
                .map(|x| format!("bbb{:02}", x).into_bytes())
                .chain((0..100).map(|x| format!("cccz{:03}", x).into_bytes()))
                .collect(),
            next_level_size: [&[1 << 18; 99][..], &[1 << 28; 10][..]].concat(),
            max_compaction_size: 1 << 30, // 1GB
        };
        // Crossing region boundary.
        let mut req = SstPartitionerRequest {
            prev_user_key: b"bba",
            current_user_key: b"bbz",
            current_output_file_size: 32 << 20,
        };
        assert_eq!(guard.should_partition(&req), SstPartitionerResult::Required);
        assert_eq!(guard.next_level_pos, 10);
        assert_eq!(guard.pos, 0);
        assert_eq!(guard.current_next_level_size, 0);
        guard.reset_next_level_size_state();

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
        assert_eq!(guard.next_level_pos, 10);
        assert_eq!(guard.current_next_level_size, 9 << 18);
        guard.reset_next_level_size_state();

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
        assert_eq!(guard.next_level_pos, 0);
        guard.reset_next_level_size_state();

        // Move position
        req = SstPartitionerRequest {
            prev_user_key: b"cca",
            current_user_key: b"ccz",
            current_output_file_size: 32 << 20,
        };
        assert_eq!(guard.should_partition(&req), SstPartitionerResult::Required);
        assert_eq!(guard.pos, 1);
        assert_eq!(guard.next_level_pos, 110);
        guard.reset_next_level_size_state();

        // Move next level posistion
        req = SstPartitionerRequest {
            prev_user_key: b"cccz000",
            current_user_key: b"cccz042",
            current_output_file_size: 1 << 20,
        };
        assert_eq!(
            guard.should_partition(&req),
            SstPartitionerResult::NotRequired
        );
        assert_eq!(guard.pos, 2);
        assert_eq!(guard.next_level_pos, 53);

        req = SstPartitionerRequest {
            prev_user_key: b"cccz090",
            current_user_key: b"dde",
            current_output_file_size: 1 << 20,
        };
        assert_eq!(guard.should_partition(&req), SstPartitionerResult::Required);
        assert_eq!(guard.pos, 2);
        assert_eq!(guard.next_level_pos, 110);
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
            current_next_level_size: 0,
            next_level_pos: 0,
            next_level_boundaries: vec![],
            next_level_size: vec![],
            max_compaction_size: 1 << 30,
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
    const MAX_COMPACTION_SIZE: u64 = 10240;

    fn new_test_db(provider: MockRegionInfoProvider) -> (RocksEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        let mut cf_opts = RocksCfOptions::default();
        cf_opts.set_max_bytes_for_level_base(MAX_OUTPUT_FILE_SIZE);
        cf_opts.set_max_bytes_for_level_multiplier(5);
        cf_opts.set_target_file_size_base(MAX_OUTPUT_FILE_SIZE);
        cf_opts.set_sst_partitioner_factory(RocksSstPartitionerFactory(
            CompactionGuardGeneratorFactory::new(
                CF_DEFAULT,
                provider,
                MIN_OUTPUT_FILE_SIZE,
                MAX_COMPACTION_SIZE,
            )
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
        // Make block size small to make sure current_output_file_size passed to
        // SstPartitioner is accurate.
        let mut block_based_opts = BlockBasedOptions::new();
        block_based_opts.set_block_size(100);
        cf_opts.set_block_based_table_factory(&block_based_opts);

        let db = new_engine_opt(
            temp_dir.path().to_str().unwrap(),
            RocksDbOptions::default(),
            vec![(CF_DEFAULT, cf_opts)],
        )
        .unwrap();
        (db, temp_dir)
    }

    fn collect_keys(path: &str) -> Vec<Vec<u8>> {
        let reader = RocksSstReader::open(path, None).unwrap();
        let mut sst_reader = reader.iter(IterOptions::default()).unwrap();
        let mut valid = sst_reader.seek_to_first().unwrap();
        let mut ret = vec![];
        while valid {
            ret.push(sst_reader.key().to_owned());
            valid = sst_reader.next().unwrap();
        }
        ret
    }

    fn get_sst_files(dir: &Path) -> Vec<String> {
        let files = dir.read_dir().unwrap();
        let mut sst_files = files
            .map(|entry| entry.unwrap().path().to_str().unwrap().to_owned())
            .filter(|entry| entry.ends_with(".sst"))
            .collect::<Vec<String>>();
        sst_files.sort();
        sst_files
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
        // Region "a" will share a SST file with region "b", since region "a" is too
        // small. Region "c" will be splitted into two SSTs, since its size is
        // larger than target_file_size_base.
        let value = vec![b'v'; 1024];
        db.put(b"za1", b"").unwrap();
        db.put(b"zb1", &value).unwrap();
        db.put(b"zc1", &value).unwrap();
        db.flush_cfs(&[], true /* wait */).unwrap();
        db.put(b"zb2", &value).unwrap();
        db.put(b"zc2", &value).unwrap();
        db.put(b"zc3", &value).unwrap();
        db.put(b"zc4", &value).unwrap();
        db.put(b"zc5", &value).unwrap();
        db.put(b"zc6", &value).unwrap();
        db.flush_cfs(&[], true /* wait */).unwrap();
        db.compact_range_cf(
            CF_DEFAULT,
            None, // start_key
            None, // end_key
            ManualCompactionOptions::new(false, 1, false),
        )
        .unwrap();

        let mut sst_files = get_sst_files(dir.path());
        sst_files.sort();
        assert_eq!(3, sst_files.len());
        assert_eq!(collect_keys(&sst_files[0]), [b"za1", b"zb1", b"zb2"]);
        assert_eq!(
            collect_keys(&sst_files[1]),
            [b"zc1", b"zc2", b"zc3", b"zc4", b"zc5"]
        );
        assert_eq!(collect_keys(&sst_files[2]), [b"zc6"]);
    }

    fn simple_regions() -> MockRegionInfoProvider {
        MockRegionInfoProvider::new(vec![
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
        ])
    }

    #[test]
    fn test_next_level_compaction() {
        let provider = simple_regions();
        let (db, _dir) = new_test_db(provider);
        assert_eq!(b"z", DATA_PREFIX_KEY);
        let tiny_value = [b'v'; 1];
        let value = vec![b'v'; 1024 * 10];
        ['a', 'b', 'c']
            .into_iter()
            .flat_map(|x| (1..10).map(move |n| format!("z{x}{n}").into_bytes()))
            .for_each(|key| db.put(&key, &value).unwrap());
        db.flush_cfs(&[], true).unwrap();
        db.compact_files_in_range(None, None, Some(2)).unwrap();
        db.put(b"za0", &tiny_value).unwrap();
        db.put(b"zd0", &tiny_value).unwrap();
        db.flush_cfs(&[], true).unwrap();
        db.compact_files_in_range(None, None, Some(1)).unwrap();

        let level_1 = &level_files(&db)[&1];
        assert_eq!(level_1.len(), 2, "{:?}", level_1);
        assert_eq!(level_1[0].smallestkey, b"za0", "{:?}", level_1);
        assert_eq!(level_1[0].largestkey, b"za0", "{:?}", level_1);
        assert_eq!(level_1[1].smallestkey, b"zd0", "{:?}", level_1);
        assert_eq!(level_1[1].largestkey, b"zd0", "{:?}", level_1);
    }

    #[test]
    fn test_next_level_compaction_no_split() {
        let provider = simple_regions();
        let (db, _dir) = new_test_db(provider);
        assert_eq!(b"z", DATA_PREFIX_KEY);
        let tiny_value = [b'v'; 1];
        let value = vec![b'v'; 1024 * 10];
        ['a', 'b', 'c']
            .into_iter()
            .flat_map(|x| (1..10).map(move |n| format!("z{x}{n}").into_bytes()))
            .for_each(|key| db.put(&key, &value).unwrap());
        db.flush_cfs(&[], true).unwrap();
        db.compact_files_in_range(None, None, Some(2)).unwrap();
        // So... the next-level size will be almost 1024 * 9, which doesn't exceeds the
        // compaction size limit.
        db.put(b"za0", &tiny_value).unwrap();
        db.put(b"za9", &tiny_value).unwrap();
        db.flush_cfs(&[], true).unwrap();
        db.compact_files_in_range(None, None, Some(1)).unwrap();

        let level_1 = &level_files(&db)[&1];
        assert_eq!(level_1.len(), 1, "{:?}", level_1);
        assert_eq!(level_1[0].smallestkey, b"za0", "{:?}", level_1);
        assert_eq!(level_1[0].largestkey, b"za9", "{:?}", level_1);
        db.compact_range(None, None, ManualCompactionOptions::new(false, 1, false))
            .unwrap();

        // So... the next-level size will be almost 1024 * 15, which should reach the
        // limit.
        db.put(b"za30", &tiny_value).unwrap();
        db.put(b"zb90", &tiny_value).unwrap();
        db.flush_cfs(&[], true).unwrap();
        db.compact_files_in_range(None, None, Some(1)).unwrap();

        let level_1 = &level_files(&db)[&1];
        assert_eq!(level_1.len(), 2, "{:?}", level_1);
        assert_eq!(level_1[0].smallestkey, b"za30", "{:?}", level_1);
        assert_eq!(level_1[1].largestkey, b"zb90", "{:?}", level_1);
    }

    #[derive(Debug)]
    #[allow(dead_code)]
    struct OwnedSstFileMetadata {
        name: String,
        size: usize,
        smallestkey: Vec<u8>,
        largestkey: Vec<u8>,
    }

    #[allow(unused)]
    fn level_files(db: &RocksEngine) -> HashMap<usize, Vec<OwnedSstFileMetadata>> {
        let db = db.as_inner();
        let cf = db.cf_handle("default").unwrap();
        let md = db.get_column_family_meta_data(cf);
        let mut res: HashMap<usize, Vec<OwnedSstFileMetadata>> = HashMap::default();
        for (i, level) in md.get_levels().into_iter().enumerate() {
            for file in level.get_files() {
                res.entry(i).or_default().push(OwnedSstFileMetadata {
                    name: file.get_name(),
                    size: file.get_size(),
                    smallestkey: file.get_smallestkey().to_owned(),
                    largestkey: file.get_largestkey().to_owned(),
                });
            }
        }
        res
    }
}

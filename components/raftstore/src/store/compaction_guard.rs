// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    ffi::CString,
    sync::{Arc, RwLock},
    time::Duration,
};

use engine_traits::{
    CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE, CfName, SstPartitioner, SstPartitionerContext,
    SstPartitionerFactory, SstPartitionerRequest, SstPartitionerResult,
};
use keys::{data_end_key, origin_key};
use lazy_static::lazy_static;
use tikv_util::{time::Instant, warn};

use super::metrics::*;
use crate::{Error, Result, coprocessor::RegionInfoProvider};

const COMPACTION_GUARD_MAX_POS_SKIP: u32 = 10;

lazy_static! {
    static ref COMPACTION_GUARD: CString = CString::new(b"CompactionGuard".to_vec()).unwrap();
}

#[derive(Eq, PartialEq, PartialOrd, Clone)]
struct TtlRange {
    start: Vec<u8>,
    end: Vec<u8>,
    expired_at: Instant,
}

impl TtlRange {
    #[cfg(test)]
    fn new(start: Vec<u8>, end: Vec<u8>, expired_at: Instant) -> Self {
        Self {
            start,
            end,
            expired_at,
        }
    }
}

impl Ord for TtlRange {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl std::fmt::Debug for TtlRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Range({:?}, {:?})",
            log_wrappers::Value(&self.start),
            log_wrappers::Value(&self.end)
        )
    }
}

#[derive(Clone, Default)]
pub struct ForcePartitionRangeManager {
    force_partition_ranges: Arc<RwLock<Vec<TtlRange>>>,
}

impl ForcePartitionRangeManager {
    pub fn add_range(&self, start: Vec<u8>, end: Vec<u8>, ttl: u64) -> bool {
        let expires = Instant::now_coarse() + Duration::from_secs(ttl);
        let mut ranges = self.force_partition_ranges.write().unwrap();
        for r in &mut *ranges {
            if r.start == start && r.end == end {
                // prolong the ttl if needed.
                if r.expired_at < expires {
                    r.expired_at = expires;
                }
                return false;
            }
        }
        ranges.push(TtlRange {
            start,
            end,
            expired_at: expires,
        });
        ranges.sort();
        true
    }

    pub fn remove_range(&self, start: &[u8], end: &[u8]) -> bool {
        let mut ranges = self.force_partition_ranges.write().unwrap();
        let mut removed = false;
        ranges.retain(|r| {
            let ne = r.start != start || r.end != end;
            if !ne {
                removed = true;
            }
            ne
        });
        removed
    }

    fn get_overlapped_ranges(&self, start: &[u8], end: &[u8]) -> Vec<TtlRange> {
        let mut ranges: Vec<TtlRange> = vec![];
        let now = Instant::now_coarse();
        let mut clean = false;
        for rg in &*self.force_partition_ranges.read().unwrap() {
            if rg.expired_at < now {
                clean = true;
                continue;
            }
            if *rg.start < *end && *rg.end > *start {
                // try to merge overlapping or adjacent range into a bigger range.
                let idx = ranges.len().saturating_sub(1);
                if !ranges.is_empty() && ranges[idx].end >= rg.start {
                    if ranges[idx].end < rg.end {
                        ranges[idx].end = rg.end.clone();
                    }
                } else {
                    ranges.push(rg.clone());
                }
            }
        }
        if clean {
            self.remove_outdated_ranges();
        }
        ranges
    }

    fn remove_outdated_ranges(&self) {
        let now = Instant::now_coarse();
        self.force_partition_ranges.write().unwrap().retain(|r| {
            let keep = r.expired_at > now;
            if !keep {
                tikv_util::info!("remove outdated force partition range"; "start" => ?log_wrappers::Value(&r.start), "end" => ?log_wrappers::Value(&r.end));
            }
            keep
        });
    }

    pub fn iter_all_ranges(&self, mut func: impl FnMut(&[u8], &[u8], u64)) {
        let now = Instant::now_coarse();
        let mut need_clean = false;
        self.force_partition_ranges
            .read()
            .unwrap()
            .iter()
            .for_each(|r| {
                let expired = r.expired_at.saturating_duration_since(now);
                if expired == Duration::ZERO {
                    need_clean = true;
                    return;
                }
                func(&r.start, &r.end, expired.as_secs());
            });
        if need_clean {
            self.remove_outdated_ranges();
        }
    }
}

pub struct CompactionGuardGeneratorFactory<P: RegionInfoProvider> {
    cf_name: CfNames,
    provider: P,
    min_output_file_size: u64,
    max_compaction_size: u64,
    partition_range_mgr: ForcePartitionRangeManager,
}

impl<P: RegionInfoProvider> CompactionGuardGeneratorFactory<P> {
    pub fn new(
        cf: CfName,
        provider: P,
        min_output_file_size: u64,
        max_compaction_size: u64,
        partition_range_mgr: ForcePartitionRangeManager,
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
            partition_range_mgr,
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
        let force_partition_ranges = self
            .partition_range_mgr
            .get_overlapped_ranges(context.smallest_key, context.largest_key);

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
            force_partition_ranges,
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
    force_partition_ranges: Vec<TtlRange>,
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
                        .chain(
                            self.force_partition_ranges
                                .iter()
                                .flat_map(|r| [r.start.clone(), r.end.clone()].into_iter()),
                        )
                        .collect::<Vec<Vec<u8>>>();
                    boundaries.sort();
                    boundaries.dedup();
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

fn overlap_with(ranges: &[TtlRange], last_key: &[u8], next_key: &[u8]) -> bool {
    // do not partition at the same key.
    if last_key >= next_key {
        return false;
    }
    if ranges.is_empty() {
        return false;
    }
    // find the range who's end_key is larger than the start_key
    let check_idx = ranges.partition_point(|r| *last_key >= *r.end);
    // if the range's start key is smaller than next_key, then it should overlap.
    check_idx < ranges.len() && *next_key > *ranges[check_idx].start
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
                || overlap_with(&self.force_partition_ranges, req.prev_user_key, req.current_user_key)
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
                if req.current_output_file_size < self.min_output_file_size
                    && self.current_next_level_size < self.max_compaction_size
                {
                    let last_pos = if self.pos > 0 {
                        self.boundaries[self.pos].as_slice()
                    } else {
                        &[]
                    };
                    let next_pos = if self.pos > 0 && self.pos < self.boundaries.len() - 1 {
                        self.boundaries[self.pos + 1].as_slice()
                    } else {
                        &[]
                    };

                    tikv_util::info!("sst partition due to force partition ranges";
                        "prev_key" => ?log_wrappers::Value(req.prev_user_key),
                        "next_key" => ?log_wrappers::Value(req.current_user_key),
                        "last_pos" => ?log_wrappers::Value(last_pos),
                        "cur_pos" => ?log_wrappers::Value(self.boundaries[self.pos].as_slice()),
                        "next_pos" => ?log_wrappers::Value(next_pos),
                        "check_ranges" => ?&self.force_partition_ranges,
                    );
                }
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

    fn can_do_trivial_move(&mut self, smallest_key: &[u8], largest_key: &[u8]) -> bool {
        // do not allow trivial move if the range overlaps with force partition range.
        !overlap_with(&self.force_partition_ranges, smallest_key, largest_key)
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
        RocksCfOptions, RocksDbOptions, RocksEngine, RocksSstPartitionerFactory, RocksSstReader,
        raw::{BlockBasedOptions, DBCompressionType},
        util::new_engine_opt,
    };
    use engine_traits::{
        CF_DEFAULT, CompactExt, IterOptions, Iterator, ManualCompactionOptions, MiscExt,
        RefIterable, SstReader, SyncMutable,
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
            force_partition_ranges: vec![],
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
            boundaries: vec![
                b"bbb".to_vec(),
                b"ccc".to_vec(),
                b"ddd".to_vec(),
                b"e".to_vec(),
                b"f".to_vec(),
            ],
            pos: 0,
            current_next_level_size: 0,
            next_level_pos: 0,
            next_level_boundaries: (0..10)
                .map(|x| format!("bbb{:02}", x).into_bytes())
                .chain((0..100).map(|x| format!("cccz{:03}", x).into_bytes()))
                .collect(),
            next_level_size: [&[1 << 18; 99][..], &[1 << 28; 10][..]].concat(),
            max_compaction_size: 1 << 30, // 1GB
            force_partition_ranges: vec![TtlRange::new(
                "e".into(),
                "f".into(),
                Instant::now_coarse() + Duration::from_secs(100),
            )],
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

        // small output file, and not overlap with force partition boundary.
        let cases: [(&[u8], &[u8]); 2] = [(b"dde", b"e"), (b"f", b"g")];
        for (start, end) in cases {
            req = SstPartitionerRequest {
                prev_user_key: start,
                current_user_key: end,
                current_output_file_size: 4 << 20,
            };
            assert_eq!(
                guard.should_partition(&req),
                SstPartitionerResult::NotRequired
            );
            let mut idx = 0;
            for i in &guard.boundaries {
                if **i > *start {
                    break;
                }
                idx += 1;
            }
            assert_eq!(guard.pos, idx, "pos: {}, idx: {}", guard.pos, idx);
            assert_eq!(guard.next_level_pos, 110);
            assert_eq!(guard.current_next_level_size, 0);
            guard.reset_next_level_size_state();
        }

        // small output file, but overlap with force partition boundary.
        let cases: [(&[u8], &[u8]); 5] = [
            (b"abc", b"eee"),
            (b"dde", b"eee"),
            (b"eff", b"g"),
            (b"e", b"f"),
            (b"de", b"fg"),
        ];
        for (start, end) in cases {
            req = SstPartitionerRequest {
                prev_user_key: start,
                current_user_key: end,
                current_output_file_size: 4 << 20,
            };
            guard.reset_next_level_size_state();
            guard.pos = 0;
            guard.next_level_pos = 0;
            guard.current_next_level_size = 0;
            assert_eq!(
                guard.should_partition(&req),
                SstPartitionerResult::Required,
                "start: {:?}, end: {:?}",
                start,
                end,
            );
        }
        guard.pos = 0;
        guard.next_level_pos = 0;
        guard.current_next_level_size = 0;
        // Not crossing boundary.
        req = SstPartitionerRequest {
            prev_user_key: b"aaa",
            current_user_key: b"aaz",
            current_output_file_size: 32 << 20,
        };
        assert_eq!(
            guard.should_partition(&req),
            SstPartitionerResult::NotRequired,
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
            force_partition_ranges: vec![],
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
        cf_opts.set_level_compaction_dynamic_level_bytes(false);
        cf_opts.set_sst_partitioner_factory(RocksSstPartitionerFactory(
            CompactionGuardGeneratorFactory::new(
                CF_DEFAULT,
                provider,
                MIN_OUTPUT_FILE_SIZE,
                MAX_COMPACTION_SIZE,
                Default::default(),
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

    #[test]
    fn test_overlap_with() {
        // empty ranges
        assert!(!overlap_with(&[], b"a", b"b"));

        let ttl = Instant::now_coarse() + Duration::from_secs(10);
        let ranges = vec![
            TtlRange::new(b"b".to_vec(), b"c".to_vec(), ttl),
            TtlRange::new(b"e".to_vec(), b"g".to_vec(), ttl),
            TtlRange::new(b"j".to_vec(), b"p".to_vec(), ttl),
        ];

        let cases = [
            (b"a".as_ref(), b"aaa".as_ref(), false),
            (b"a", b"b", false),
            (b"a", b"bb", true),
            (b"a", b"c", true),
            (b"a", b"e", true),
            (b"a", b"j", true),
            (b"a", b"p", true),
            (b"a", b"z", true),
            (b"b", b"bccc", true),
            (b"b", b"c", true),
            (b"b", b"d", true),
            (b"c", b"d", false),
            (b"c", b"e", false),
            (b"c", b"f", true),
            (b"c", b"g", true),
            (b"c", b"h", true),
            (b"g", b"h", false),
            (b"j", b"p", true),
            (b"jj", b"n", true),
            (b"p", b"q", false),
            (b"q", b"r", false),
            // following are not overlap because start >= end
            (b"a", b"a", false),
            (b"b", b"a", false),
            (b"f", b"f", false),
            (b"z", b"a", false),
        ];
        for (i, (start, end, overlap)) in cases.into_iter().enumerate() {
            assert_eq!(overlap_with(&ranges, start, end), overlap, "case: {}", i);
        }
    }

    #[test]
    fn test_force_partition_range() {
        let mgr = ForcePartitionRangeManager::default();

        mgr.add_range(b"h".to_vec(), b"n".to_vec(), 10);
        mgr.add_range(b"b".to_vec(), b"f".to_vec(), 10);
        assert!(mgr.force_partition_ranges.read().unwrap().is_sorted());

        #[track_caller]
        fn check_overlap(
            mgr: &ForcePartitionRangeManager,
            start: &[u8],
            end: &[u8],
            expect: &[(&[u8], &[u8])],
        ) {
            let ranges = mgr.get_overlapped_ranges(start, end);
            assert_eq!(ranges.len(), expect.len());
            ranges
                .iter()
                .zip(expect)
                .for_each(|(got, exp)| assert!(*got.start == *exp.0 && *got.end == *exp.1));
        }

        check_overlap(&mgr, b"a", b"aa", &[]);
        check_overlap(&mgr, b"a", b"b", &[]);
        check_overlap(&mgr, b"a", b"c", &[(b"b", b"f")]);
        check_overlap(&mgr, b"a", b"h", &[(b"b", b"f")]);
        check_overlap(&mgr, b"a", b"i", &[(b"b", b"f"), (b"h", b"n")]);
        check_overlap(&mgr, b"a", b"n", &[(b"b", b"f"), (b"h", b"n")]);
        check_overlap(&mgr, b"a", b"z", &[(b"b", b"f"), (b"h", b"n")]);

        // test overlapped ranges
        mgr.add_range(b"b".to_vec(), b"d".to_vec(), 10);
        check_overlap(&mgr, b"a", b"d", &[(b"b", b"f")]);

        mgr.add_range(b"bb".to_vec(), b"g".to_vec(), 10);
        check_overlap(&mgr, b"a", b"d", &[(b"b", b"g")]);
    }
}

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::slice::Iter;
use std::time::{Duration, SystemTime};

use kvproto::kvrpcpb::KeyRange;
use kvproto::metapb::Peer;
use kvproto::pdpb::QueryKind;

use rand::Rng;

use tikv_util::config::Tracker;
use tikv_util::{debug, info};

use crate::store::metrics::*;
use crate::store::worker::query_stats::{is_read_query, QueryStats};
use crate::store::worker::split_config::DEFAULT_SAMPLE_NUM;
use crate::store::worker::{FlowStatistics, SplitConfig, SplitConfigManager};

pub const TOP_N: usize = 10;
pub const MAX_RETRY_TIME: i32 = 1000;

pub struct SplitInfo {
    pub region_id: u64,
    pub split_key: Vec<u8>,
    pub peer: Peer,
}

pub struct Sample {
    pub key: Vec<u8>,
    pub left: i32,
    pub contained: i32,
    pub right: i32,
}

impl Sample {
    fn new(key: &[u8]) -> Sample {
        Sample {
            key: key.to_owned(),
            left: 0,
            contained: 0,
            right: 0,
        }
    }
}

// It will return prefix sum of iter. `read` is a function to be used to read data from iter.
fn prefix_sum<F, T>(iter: Iter<T>, read: F) -> Vec<usize>
where
    F: Fn(&T) -> usize,
{
    let mut pre_sum = vec![];
    let mut sum = 0;
    for item in iter {
        sum += read(&item);
        pre_sum.push(sum);
    }
    pre_sum
}

// It will return sample_num numbers by sample from lists.
// The list in the lists has the length of N1, N2, N3 ... Np ... NP in turn.
// Their prefix sum is pre_sum and we can get mut list from lists by get_mut.
// Take a random number d from [1, N]. If d < N1, select a data in the first list with an equal probability without replacement;
// If N1 <= d <(N1 + N2), then select a data in the second list with equal probability without replacement;
// and so on, repeat m times, and finally select sample_num pieces of data from lists.
fn sample<F, T>(
    sample_num: usize,
    pre_sum: &[usize],
    mut lists: Vec<T>,
    get_mut: F,
) -> Vec<KeyRange>
where
    F: Fn(&mut T) -> &mut Vec<KeyRange>,
{
    let mut rng = rand::thread_rng();
    let mut key_ranges = vec![];
    let high_bound = *pre_sum.last().unwrap();
    for _ in 0..MAX_RETRY_TIME {
        let d = rng.gen_range(0..high_bound + 1) as usize;
        let i = match pre_sum.binary_search(&d) {
            Ok(i) => i,
            Err(i) => i,
        };
        if i < lists.len() {
            let list = get_mut(&mut lists[i]);
            if !list.is_empty() {
                let j = rng.gen_range(0..list.len()) as usize;
                key_ranges.push(list.remove(j)); // Sampling without replacement
            }
        }
        if key_ranges.len() >= std::cmp::min(sample_num, high_bound) {
            break;
        }
    }
    key_ranges
}

// RegionInfo will maintain key_ranges with sample_num length by reservoir sampling.
// And it will save qps num and peer.
#[derive(Debug, Clone)]
pub struct RegionInfo {
    pub sample_num: usize,
    pub query_stats: QueryStats,
    pub peer: Peer,
    pub key_ranges: Vec<KeyRange>,
    pub flow: FlowStatistics,
}

impl RegionInfo {
    fn new(sample_num: usize) -> RegionInfo {
        RegionInfo {
            sample_num,
            query_stats: QueryStats::default(),
            key_ranges: Vec::with_capacity(sample_num),
            peer: Peer::default(),
            flow: FlowStatistics::default(),
        }
    }

    fn get_read_qps(&self) -> usize {
        self.query_stats.get_read_query_num() as usize
    }

    fn get_key_ranges_mut(&mut self) -> &mut Vec<KeyRange> {
        &mut self.key_ranges
    }

    fn add_key_ranges(&mut self, key_ranges: Vec<KeyRange>) {
        for key_range in key_ranges {
            if self.key_ranges.len() < self.sample_num || self.get_read_qps() == 0 {
                self.key_ranges.push(key_range);
            } else {
                let i = rand::thread_rng().gen_range(0..self.get_read_qps()) as usize;
                if i < self.sample_num {
                    self.key_ranges[i] = key_range;
                }
            }
        }
    }

    fn add_query_num(&mut self, kind: QueryKind, query_num: u64) {
        self.query_stats.add_query_num(kind, query_num);
    }

    fn update_peer(&mut self, peer: &Peer) {
        if self.peer != *peer {
            self.peer = peer.clone();
        }
    }
}

pub struct Recorder {
    pub detect_num: u64,
    pub peer: Peer,
    pub key_ranges: Vec<Vec<KeyRange>>,
    pub times: u64,
    pub create_time: SystemTime,
}

impl Recorder {
    fn new(detect_num: u64) -> Recorder {
        Recorder {
            detect_num,
            peer: Peer::default(),
            key_ranges: vec![],
            times: 0,
            create_time: SystemTime::now(),
        }
    }

    fn record(&mut self, key_ranges: Vec<KeyRange>) {
        self.times += 1;
        self.key_ranges.push(key_ranges);
    }

    fn update_peer(&mut self, peer: &Peer) {
        if self.peer != *peer {
            self.peer = peer.clone();
        }
    }

    fn is_ready(&self) -> bool {
        self.times >= self.detect_num
    }

    fn convert(&self, key_ranges: Vec<KeyRange>) -> Vec<Sample> {
        key_ranges
            .iter()
            .fold(HashSet::new(), |mut hash_set, key_range| {
                hash_set.insert(&key_range.start_key);
                hash_set.insert(&key_range.end_key);
                hash_set
            })
            .into_iter()
            .map(|key| Sample::new(key))
            .collect()
    }

    fn collect(&mut self, config: &SplitConfig) -> Vec<u8> {
        let pre_sum = prefix_sum(self.key_ranges.iter(), Vec::len);
        let sampled_key_ranges =
            sample(config.sample_num, &pre_sum, self.key_ranges.clone(), |x| x);
        let mut samples: Vec<Sample> = self.convert(sampled_key_ranges);
        for key_ranges in &self.key_ranges {
            for key_range in key_ranges {
                Recorder::sample(&mut samples, &key_range);
            }
        }
        Recorder::split_key(
            samples,
            config.split_balance_score,
            config.split_contained_score,
            config.sample_threshold as i32,
        )
    }

    fn sample(samples: &mut Vec<Sample>, key_range: &KeyRange) {
        for mut sample in samples.iter_mut() {
            let order_start = if key_range.start_key.is_empty() {
                Ordering::Greater
            } else {
                sample.key.cmp(&key_range.start_key)
            };

            let order_end = if key_range.end_key.is_empty() {
                Ordering::Less
            } else {
                sample.key.cmp(&key_range.end_key)
            };

            if order_start == Ordering::Greater && order_end == Ordering::Less {
                sample.contained += 1;
            } else if order_start != Ordering::Greater {
                sample.right += 1;
            } else {
                sample.left += 1;
            }
        }
    }

    fn split_key(
        samples: Vec<Sample>,
        split_balance_score: f64,
        split_contained_score: f64,
        sample_threshold: i32,
    ) -> Vec<u8> {
        let mut best_index: i32 = -1;
        let mut best_score = 2.0;
        for (index, sample) in samples.iter().enumerate() {
            if sample.key.is_empty() {
                continue;
            }
            let sampled = sample.contained + sample.left + sample.right;
            if (sample.left + sample.right) == 0 || sampled < sample_threshold {
                continue;
            }
            let diff = (sample.left - sample.right) as f64;
            let balance_score = diff.abs() / (sample.left + sample.right) as f64;
            if balance_score >= split_balance_score {
                continue;
            }
            let contained_score = sample.contained as f64 / sampled as f64;
            if contained_score >= split_contained_score {
                continue;
            }
            let final_score = balance_score + contained_score;
            if final_score < best_score {
                best_index = index as i32;
                best_score = final_score;
            }
        }
        if best_index >= 0 {
            return samples[best_index as usize].key.clone();
        }
        return vec![];
    }
}

#[derive(Clone, Debug)]
pub struct ReadStats {
    pub region_infos: HashMap<u64, RegionInfo>,
    pub sample_num: usize,
}

impl ReadStats {
    pub fn add_query_num(
        &mut self,
        region_id: u64,
        peer: &Peer,
        key_range: KeyRange,
        kind: QueryKind,
    ) {
        self.add_query_num_batch(region_id, peer, vec![key_range], kind);
    }

    pub fn add_query_num_batch(
        &mut self,
        region_id: u64,
        peer: &Peer,
        key_ranges: Vec<KeyRange>,
        kind: QueryKind,
    ) {
        let sample_num = self.sample_num;
        let query_num = key_ranges.len() as u64;
        let region_info = self
            .region_infos
            .entry(region_id)
            .or_insert_with(|| RegionInfo::new(sample_num));
        region_info.update_peer(peer);
        region_info.add_query_num(kind, query_num);
        if is_read_query(kind) {
            region_info.add_key_ranges(key_ranges);
        }
    }

    pub fn add_flow(&mut self, region_id: u64, write: &FlowStatistics, data: &FlowStatistics) {
        let num = self.sample_num;
        let region_info = self
            .region_infos
            .entry(region_id)
            .or_insert_with(|| RegionInfo::new(num));
        region_info.flow.add(write);
        region_info.flow.add(data);
    }

    pub fn is_empty(&self) -> bool {
        self.region_infos.is_empty()
    }
}

impl Default for ReadStats {
    fn default() -> ReadStats {
        ReadStats {
            sample_num: DEFAULT_SAMPLE_NUM,
            region_infos: HashMap::default(),
        }
    }
}

pub struct AutoSplitController {
    pub recorders: HashMap<u64, Recorder>,
    cfg: SplitConfig,
    cfg_tracker: Tracker<SplitConfig>,
}

impl AutoSplitController {
    pub fn new(config_manager: SplitConfigManager) -> AutoSplitController {
        AutoSplitController {
            recorders: HashMap::default(),
            cfg: config_manager.value().clone(),
            cfg_tracker: config_manager.0.clone().tracker("split_hub".to_owned()),
        }
    }

    pub fn default() -> AutoSplitController {
        AutoSplitController::new(SplitConfigManager::default())
    }

    fn collect_read_stats(&self, read_stats_vec: Vec<ReadStats>) -> HashMap<u64, Vec<RegionInfo>> {
        // collect from different thread
        let mut region_infos_map = HashMap::default(); // regionID-regionInfos
        let capacity = read_stats_vec.len();
        for read_stats in read_stats_vec {
            for (region_id, region_info) in read_stats.region_infos {
                let region_infos = region_infos_map
                    .entry(region_id)
                    .or_insert_with(|| Vec::with_capacity(capacity));
                region_infos.push(region_info);
            }
        }
        region_infos_map
    }

    pub fn flush(&mut self, read_stats_vec: Vec<ReadStats>) -> (Vec<usize>, Vec<SplitInfo>) {
        let mut split_infos = Vec::default();
        let mut top = BinaryHeap::with_capacity(TOP_N as usize);
        let region_infos_map = self.collect_read_stats(read_stats_vec);

        for (region_id, region_infos) in region_infos_map {
            let pre_sum = prefix_sum(region_infos.iter(), RegionInfo::get_read_qps);
            let qps = *pre_sum.last().unwrap(); // region_infos is not empty
            let byte = region_infos
                .iter()
                .fold(0, |flow, region_info| flow + region_info.flow.read_bytes);
            debug!("load base split params";"region_id"=>region_id,"qps"=>qps,"qps_threshold"=>self.cfg.qps_threshold,"byte"=>byte,"byte_threshold"=>self.cfg.byte_threshold);

            if qps < self.cfg.qps_threshold && byte < self.cfg.byte_threshold {
                self.recorders.remove_entry(&region_id);
                continue;
            }

            LOAD_BASE_SPLIT_EVENT.with_label_values(&["load_fit"]).inc();

            let num = self.cfg.detect_times;
            let recorder = self
                .recorders
                .entry(region_id)
                .or_insert_with(|| Recorder::new(num));
            recorder.update_peer(&region_infos[0].peer);

            let key_ranges = sample(
                self.cfg.sample_num,
                &pre_sum,
                region_infos,
                RegionInfo::get_key_ranges_mut,
            );

            recorder.record(key_ranges);
            if recorder.is_ready() {
                let key = recorder.collect(&self.cfg);
                if !key.is_empty() {
                    let split_info = SplitInfo {
                        region_id,
                        split_key: key,
                        peer: recorder.peer.clone(),
                    };
                    split_infos.push(split_info);
                    LOAD_BASE_SPLIT_EVENT
                        .with_label_values(&["prepare_to_split"])
                        .inc();
                    info!(
                        "load base split region";
                        "region_id"=>region_id,
                        "qps"=>qps,
                    );
                }
                self.recorders.remove(&region_id);
            } else {
                LOAD_BASE_SPLIT_EVENT
                    .with_label_values(&["no_fit_key"])
                    .inc();
            }

            top.push(qps);
        }

        (top.into_vec(), split_infos)
    }

    pub fn clear(&mut self) {
        let interval = Duration::from_secs(self.cfg.detect_times * 2);
        self.recorders
            .retain(|_, recorder| match recorder.create_time.elapsed() {
                Ok(life_time) => life_time < interval,
                Err(_) => true,
            });
    }

    pub fn refresh_cfg(&mut self) {
        if let Some(incoming) = self.cfg_tracker.any_new() {
            self.cfg = incoming.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::util::build_key_range;
    use txn_types::Key;

    enum Position {
        Left,
        Right,
        Contained,
    }

    impl Sample {
        fn num(&self, pos: Position) -> i32 {
            match pos {
                Position::Left => self.left,
                Position::Right => self.right,
                Position::Contained => self.contained,
            }
        }
    }

    struct SampleCase {
        key: Vec<u8>,
    }

    impl SampleCase {
        fn sample_key(&self, start_key: &[u8], end_key: &[u8], pos: Position) {
            let mut samples = vec![Sample::new(&self.key)];
            let key_range = build_key_range(start_key, end_key, false);
            Recorder::sample(&mut samples, &key_range);
            assert_eq!(
                samples[0].num(pos),
                1,
                "start_key is {:?}, end_key is {:?}",
                String::from_utf8(Vec::from(start_key)).unwrap(),
                String::from_utf8(Vec::from(end_key)).unwrap()
            );
        }
    }

    #[test]
    fn test_pre_sum() {
        let v = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let expect = vec![1, 3, 6, 10, 15, 21, 28, 36, 45];
        let pre = prefix_sum(v.iter(), |x| *x);
        for i in 0..v.len() {
            assert_eq!(expect[i], pre[i]);
        }
    }

    #[test]
    fn test_sample() {
        let sc = SampleCase { key: vec![b'c'] };

        // limit scan
        sc.sample_key(b"a", b"b", Position::Left);
        sc.sample_key(b"a", b"c", Position::Left);
        sc.sample_key(b"a", b"d", Position::Contained);
        sc.sample_key(b"c", b"d", Position::Right);
        sc.sample_key(b"d", b"e", Position::Right);

        // point get
        sc.sample_key(b"a", b"a", Position::Left);
        sc.sample_key(b"c", b"c", Position::Right); // when happened 100 times (a,a) and 100 times (c,c), we will split from c.
        sc.sample_key(b"d", b"d", Position::Right);

        // unlimited scan
        sc.sample_key(b"", b"", Position::Contained);
        sc.sample_key(b"a", b"", Position::Contained);
        sc.sample_key(b"c", b"", Position::Right);
        sc.sample_key(b"d", b"", Position::Right);
        sc.sample_key(b"", b"a", Position::Left);
        sc.sample_key(b"", b"c", Position::Left);
        sc.sample_key(b"", b"d", Position::Contained);
    }

    fn gen_read_stats(region_id: u64, key_ranges: Vec<KeyRange>) -> ReadStats {
        let mut qps_stats = ReadStats::default();
        for key_range in &key_ranges {
            qps_stats.add_query_num(
                region_id,
                &Peer::default(),
                key_range.clone(),
                QueryKind::Get,
            );
        }
        qps_stats
    }

    #[test]
    fn test_hub() {
        // raw key mode
        let raw_key_ranges = vec![
            build_key_range(b"a", b"b", false),
            build_key_range(b"b", b"c", false),
        ];
        check_split(
            b"raw key",
            vec![gen_read_stats(1, raw_key_ranges.clone())],
            vec![b"b"],
        );

        // encoded key mode
        let key_a = Key::from_raw(b"0080").append_ts(2.into());
        let key_b = Key::from_raw(b"0160").append_ts(2.into());
        let key_c = Key::from_raw(b"0240").append_ts(2.into());
        let encoded_key_ranges = vec![
            build_key_range(key_a.as_encoded(), key_b.as_encoded(), false),
            build_key_range(key_b.as_encoded(), key_c.as_encoded(), false),
        ];
        check_split(
            b"encoded key",
            vec![gen_read_stats(1, encoded_key_ranges.clone())],
            vec![key_b.as_encoded()],
        );

        // mix mode
        check_split(
            b"mix key",
            vec![
                gen_read_stats(1, raw_key_ranges),
                gen_read_stats(2, encoded_key_ranges),
            ],
            vec![b"b", key_b.as_encoded()],
        );

        // test distribution with contained key
        for _i in 0..100 {
            let key_ranges = vec![
                build_key_range(b"a", b"k", false),
                build_key_range(b"b", b"j", false),
                build_key_range(b"c", b"i", false),
                build_key_range(b"d", b"h", false),
                build_key_range(b"e", b"g", false),
                build_key_range(b"f", b"f", false),
            ];
            check_split(
                b"isosceles triangle",
                vec![gen_read_stats(1, key_ranges)],
                vec![],
            );

            let key_ranges = vec![
                build_key_range(b"a", b"f", false),
                build_key_range(b"b", b"g", false),
                build_key_range(b"c", b"h", false),
                build_key_range(b"d", b"i", false),
                build_key_range(b"e", b"j", false),
                build_key_range(b"f", b"k", false),
            ];
            check_split(
                b"parallelogram",
                vec![gen_read_stats(1, key_ranges)],
                vec![],
            );

            let key_ranges = vec![
                build_key_range(b"a", b"l", false),
                build_key_range(b"a", b"m", false),
            ];
            check_split(
                b"right-angle trapezoid",
                vec![gen_read_stats(1, key_ranges)],
                vec![],
            );

            let key_ranges = vec![
                build_key_range(b"a", b"l", false),
                build_key_range(b"b", b"l", false),
            ];
            check_split(
                b"right-angle trapezoid",
                vec![gen_read_stats(1, key_ranges)],
                vec![],
            );
        }
    }

    fn check_split(mode: &[u8], qps_stats: Vec<ReadStats>, split_keys: Vec<&[u8]>) {
        let mut hub = AutoSplitController::new(SplitConfigManager::default());
        hub.cfg.qps_threshold = 1;
        hub.cfg.sample_threshold = 0;

        for i in 0..10 {
            let (_, split_infos) = hub.flush(qps_stats.clone());
            if (i + 1) % hub.cfg.detect_times == 0 {
                assert_eq!(
                    split_infos.len(),
                    split_keys.len(),
                    "mode: {:?}",
                    String::from_utf8(Vec::from(mode)).unwrap()
                );
                for obtain in &split_infos {
                    let mut equal = false;
                    for expect in &split_keys {
                        if obtain.split_key.cmp(&expect.to_vec()) == Ordering::Equal {
                            equal = true;
                            break;
                        }
                    }
                    assert!(
                        equal,
                        "mode: {:?}",
                        String::from_utf8(Vec::from(mode)).unwrap()
                    );
                }
            }
        }
    }

    #[test]
    fn test_sample_key_num() {
        let mut hub = AutoSplitController::new(SplitConfigManager::default());
        hub.cfg.qps_threshold = 2000;
        hub.cfg.sample_num = 2000;
        hub.cfg.sample_threshold = 0;

        for _ in 0..100 {
            // qps_stats_vec contains 2000 qps and a readStats with a key range;
            let mut qps_stats_vec = vec![];

            let mut qps_stats = ReadStats::default();
            qps_stats.add_query_num(
                1,
                &Peer::default(),
                build_key_range(b"a", b"b", false),
                QueryKind::Get,
            );
            qps_stats_vec.push(qps_stats);

            let mut qps_stats = ReadStats::default();
            for _ in 0..2000 {
                qps_stats.add_query_num(
                    1,
                    &Peer::default(),
                    build_key_range(b"b", b"c", false),
                    QueryKind::Get,
                );
            }
            qps_stats_vec.push(qps_stats);
            hub.flush(qps_stats_vec);
        }
    }

    fn check_sample_length(sample_num: usize, key_ranges: Vec<Vec<KeyRange>>) {
        for _ in 0..100 {
            let pre_sum = prefix_sum(key_ranges.iter(), Vec::len);
            let sampled_key_ranges = sample(sample_num, &pre_sum, key_ranges.clone(), |x| x);
            assert_eq!(sampled_key_ranges.len(), sample_num);
        }
    }

    #[test]
    fn test_sample_length() {
        let sample_num = 20;
        let mut key_ranges = vec![];
        for _ in 0..sample_num {
            key_ranges.push(vec![build_key_range(b"a", b"b", false)]);
        }
        check_sample_length(sample_num, key_ranges);

        let mut key_ranges = vec![];
        let num = 100;
        for _ in 0..num {
            key_ranges.push(vec![build_key_range(b"a", b"b", false)]);
        }
        check_sample_length(sample_num, key_ranges);

        let mut key_ranges = vec![];
        for _ in 0..num {
            let mut ranges = vec![];
            for _ in 0..num {
                ranges.push(build_key_range(b"a", b"b", false));
            }
            key_ranges.push(ranges);
        }
        check_sample_length(sample_num, key_ranges);
    }

    #[test]
    fn test_recorder_convert() {
        let r = Recorder::new(1);
        let key_ranges = vec![];
        assert_eq!(r.convert(key_ranges).len(), 0);

        let key_ranges = vec![build_key_range(b"a", b"b", false)];
        assert_eq!(r.convert(key_ranges).len(), 2);

        let key_ranges = vec![
            build_key_range(b"a", b"a", false),
            build_key_range(b"b", b"c", false),
        ];
        assert_eq!(r.convert(key_ranges).len(), 3);
    }

    const REGION_NUM: u64 = 1000;
    const KEY_RANGE_NUM: u64 = 1000;

    fn default_qps_stats() -> ReadStats {
        let mut qps_stats = ReadStats::default();
        for i in 0..REGION_NUM {
            for _j in 0..KEY_RANGE_NUM {
                qps_stats.add_query_num(
                    i,
                    &Peer::default(),
                    build_key_range(b"a", b"b", false),
                    QueryKind::Get,
                )
            }
        }
        qps_stats
    }

    #[bench]
    fn recorder_sample(b: &mut test::Bencher) {
        let mut samples = vec![Sample::new(b"c")];
        let key_range = build_key_range(b"a", b"b", false);
        b.iter(|| {
            Recorder::sample(&mut samples, &key_range);
        });
    }

    #[bench]
    fn hub_flush(b: &mut test::Bencher) {
        let mut other_qps_stats = vec![];
        for _i in 0..10 {
            other_qps_stats.push(default_qps_stats());
        }
        b.iter(|| {
            let mut hub = AutoSplitController::new(SplitConfigManager::default());
            hub.flush(other_qps_stats.clone());
        });
    }

    #[bench]
    fn qps_scan(b: &mut test::Bencher) {
        let mut qps_stats = default_qps_stats();
        let start_key = Key::from_raw(b"a");
        let end_key = Some(Key::from_raw(b"b"));

        b.iter(|| {
            if let Ok(start_key) = start_key.to_owned().into_raw() {
                let mut key = vec![];
                if let Some(end_key) = &end_key {
                    if let Ok(end_key) = end_key.to_owned().into_raw() {
                        key = end_key;
                    }
                }
                qps_stats.add_query_num(
                    1,
                    &Peer::default(),
                    build_key_range(&start_key, &key, false),
                    QueryKind::Scan,
                );
            }
        });
    }

    #[bench]
    fn qps_add(b: &mut test::Bencher) {
        let mut qps_stats = default_qps_stats();
        b.iter(|| {
            qps_stats.add_query_num(
                1,
                &Peer::default(),
                build_key_range(b"a", b"b", false),
                QueryKind::Get,
            );
        });
    }
}

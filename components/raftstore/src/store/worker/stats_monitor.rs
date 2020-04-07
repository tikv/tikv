// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io;
use std::slice::Iter;
use std::sync::mpsc::{self, Sender};
use std::thread::{Builder, JoinHandle};
use std::time::{Duration, SystemTime};

use kvproto::kvrpcpb::KeyRange;
use kvproto::metapb::Peer;
use kvproto::pdpb::RecordPair;
use rand::Rng;

use tikv_util::collections::HashMap;
use tikv_util::config::Tracker;
use tikv_util::metrics::ThreadInfoStatistics;
use tikv_util::worker::FutureScheduler as Scheduler;
use txn_types::Key;

use crate::store::metrics::*;
use crate::store::worker::pd::RecordPairVec;
use crate::store::worker::pd::Task;
use crate::store::worker::split_config::DEFAULT_SAMPLE_NUM;
use crate::store::worker::{SplitHubConfig, SplitHubConfigManager};

const TOP_N: usize = 10;
const DEFAULT_QPS_INFO_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_COLLECT_INTERVAL: Duration = Duration::from_secs(1);

#[inline]
fn convert_record_pairs(m: HashMap<String, u64>) -> RecordPairVec {
    m.into_iter()
        .map(|(k, v)| {
            let mut pair = RecordPair::default();
            pair.set_key(k);
            pair.set_value(v);
            pair
        })
        .collect()
}

pub struct StatsMonitor {
    scheduler: Scheduler<Task>,
    handle: Option<JoinHandle<()>>,
    timer: Option<Sender<bool>>,
    sender: Option<Sender<QpsStats>>,
    thread_info_interval: Duration,
    qps_info_interval: Duration,
    collect_interval: Duration,
}

impl StatsMonitor {
    pub fn new(interval: Duration, scheduler: Scheduler<Task>) -> Self {
        StatsMonitor {
            scheduler,
            handle: None,
            timer: None,
            sender: None,
            thread_info_interval: interval,
            qps_info_interval: DEFAULT_QPS_INFO_INTERVAL,
            collect_interval: DEFAULT_COLLECT_INTERVAL,
        }
    }

    pub fn start(&mut self, config_manager: &SplitHubConfigManager) -> Result<(), io::Error> {
        let mut timer_cnt = 0;
        let collect_interval = self.collect_interval;
        let thread_info_interval = self
            .thread_info_interval
            .div_duration_f64(self.collect_interval) as i32;
        let qps_info_interval = self
            .qps_info_interval
            .div_duration_f64(self.collect_interval) as i32;

        let (tx, rx) = mpsc::channel();
        self.timer = Some(tx);
        let (sender, receiver) = mpsc::channel();
        self.sender = Some(sender);

        let scheduler = self.scheduler.clone();

        let mut unify_hub = SplitHub::new(config_manager);

        let h = Builder::new()
            .name(thd_name!("stats-monitor"))
            .spawn(move || {
                let mut thread_stats = ThreadInfoStatistics::new();
                while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(collect_interval) {
                    if timer_cnt % thread_info_interval == 0 {
                        thread_stats.record();
                        let cpu_usages = convert_record_pairs(thread_stats.get_cpu_usages());
                        let read_io_rates = convert_record_pairs(thread_stats.get_read_io_rates());
                        let write_io_rates =
                            convert_record_pairs(thread_stats.get_write_io_rates());

                        let task = Task::StoreInfos {
                            cpu_usages,
                            read_io_rates,
                            write_io_rates,
                        };
                        if let Err(e) = scheduler.schedule(task) {
                            error!(
                                "failed to send store infos to pd worker";
                                "err" => ?e,
                            );
                        }
                    }
                    if timer_cnt % qps_info_interval == 0 {
                        let mut others = vec![];
                        while let Ok(other) = receiver.try_recv() {
                            others.push(other);
                        }
                        let (top, split_infos) = unify_hub.flush(others);
                        unify_hub.clear();
                        let task = Task::AutoSplit { split_infos };
                        if let Err(e) = scheduler.schedule(task) {
                            error!(
                                "failed to send split infos to pd worker";
                                "err" => ?e,
                            );
                        }

                        for i in 0..TOP_N {
                            if i < top.len() {
                                READ_QPS_TOPN
                                    .with_label_values(&[&i.to_string()])
                                    .set(top[i] as f64);
                            } else {
                                READ_QPS_TOPN.with_label_values(&[&i.to_string()]).set(0.0);
                            }
                        }
                    }
                    timer_cnt = (timer_cnt + 1) % (qps_info_interval * thread_info_interval);
                    unify_hub.refresh_cfg();
                }
            })?;

        self.handle = Some(h);
        Ok(())
    }

    pub fn stop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }
        drop(self.timer.take().unwrap());
        drop(self.sender.take().unwrap());
        if let Err(e) = h.unwrap().join() {
            error!("join stats collector failed"; "err" => ?e);
            return;
        }
    }

    pub fn get_sender(&self) -> &Option<Sender<QpsStats>> {
        &self.sender
    }
}

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

fn prefix_sum<F, T>(iter: Iter<T>, read: F) -> Vec<usize>
where
    F: Fn(&T) -> usize,
{
    let mut pre_sum = vec![];
    for item in iter {
        let last = match pre_sum.last() {
            Some(last) => *last,
            None => 0,
        };
        pre_sum.push(read(&item) + last);
    }
    pre_sum
}

fn sample<F, T>(sample_num: usize, pre_sum: &[usize], mut mat: Vec<T>, read_mut: F) -> Vec<KeyRange>
where
    F: Fn(&mut T) -> &mut Vec<KeyRange>,
{
    let mut rng = rand::thread_rng();
    let mut key_ranges = vec![];
    let high_bound = pre_sum.last().unwrap();
    for _num in 0..sample_num {
        let d = rng.gen_range(0, *high_bound) as usize;
        let i = match pre_sum.binary_search(&d) {
            Ok(i) => i,
            Err(i) => i,
        };
        let row = read_mut(&mut mat[i]);
        let j = rng.gen_range(0, row.len()) as usize;
        key_ranges.push(row.remove(j)); // Sampling without replacement
    }
    key_ranges
}

#[derive(Debug, Clone)]
pub struct RegionInfo {
    pub sample_num: usize,
    pub qps: usize,
    pub peer: Peer,
    pub key_ranges: Vec<KeyRange>,
}

impl RegionInfo {
    fn new(sample_num: usize) -> RegionInfo {
        RegionInfo {
            sample_num,
            qps: 0,
            key_ranges: Vec::with_capacity(sample_num),
            peer: Peer::default(),
        }
    }

    fn get_qps(&self) -> usize {
        self.qps
    }

    fn get_key_ranges_mut(&mut self) -> &mut Vec<KeyRange> {
        &mut self.key_ranges
    }

    fn get_peer(&self) -> &Peer {
        &self.peer
    }

    fn add_key_ranges(&mut self, key_ranges: Vec<KeyRange>) {
        for key_range in key_ranges {
            if self.key_ranges.len() < self.sample_num {
                self.key_ranges.push(key_range);
            } else {
                let i = rand::thread_rng().gen_range(0, self.qps) as usize;
                if i < self.sample_num {
                    self.key_ranges[i] = key_range;
                }
            }
            self.qps += 1;
        }
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

    fn collect(&mut self, config: &SplitHubConfig) -> Vec<u8> {
        let pre_sum = prefix_sum(self.key_ranges.iter(), Vec::len);
        let key_ranges = self.key_ranges.clone();
        let mut samples = sample(config.sample_num, &pre_sum, key_ranges, |x| x)
            .iter()
            .map(|key_range| Sample::new(&key_range.start_key))
            .collect();
        for key_ranges in &self.key_ranges {
            for key_range in key_ranges {
                Recorder::sample(&mut samples, &key_range);
            }
        }
        Recorder::split_key(
            samples,
            config.split_balance_score,
            config.split_contained_score,
            config.sample_threshold,
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
            } else {
                if order_start != Ordering::Greater {
                    sample.right += 1;
                } else if order_start == Ordering::Greater {
                    sample.left += 1;
                }
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
        for index in 0..samples.len() {
            let sample = &samples[index];
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

#[derive(Clone)]
pub struct QpsStats {
    pub region_infos: HashMap<u64, RegionInfo>,
    pub sample_num: usize,
}

impl QpsStats {
    pub fn new() -> QpsStats {
        QpsStats {
            sample_num: DEFAULT_SAMPLE_NUM,
            region_infos: HashMap::default(),
        }
    }

    pub fn add(&mut self, region_id: u64, peer: &Peer, key_range: KeyRange) {
        self.batch_add(region_id, peer, vec![key_range]);
    }

    pub fn batch_add(&mut self, region_id: u64, peer: &Peer, key_ranges: Vec<KeyRange>) {
        let num = self.sample_num;
        let region_info = self
            .region_infos
            .entry(region_id)
            .or_insert_with(|| RegionInfo::new(num));
        region_info.update_peer(peer);
        region_info.add_key_ranges(key_ranges);
    }
}

pub struct SplitHub {
    pub recorders: HashMap<u64, Recorder>,
    cfg: SplitHubConfig,
    cfg_tracker: Tracker<SplitHubConfig>,
}

impl SplitHub {
    pub fn new(config_manager: &SplitHubConfigManager) -> SplitHub {
        SplitHub {
            recorders: HashMap::default(),
            cfg: config_manager.value().clone(),
            cfg_tracker: config_manager.0.clone().tracker("split_hub".to_owned()),
        }
    }

    pub fn flush(&mut self, others: Vec<QpsStats>) -> (Vec<usize>, Vec<SplitInfo>) {
        let mut split_infos = Vec::default();
        let mut top = BinaryHeap::with_capacity(TOP_N as usize);

        let mut region_info_mat = HashMap::default();
        let capacity = others.len();
        for other in others {
            for (region_id, region_info) in other.region_infos {
                if region_info.key_ranges.len() >= self.cfg.sample_num {
                    let region_infos = region_info_mat
                        .entry(region_id)
                        .or_insert_with(|| Vec::with_capacity(capacity));
                    region_infos.push(region_info);
                }
            }
        }

        for (region_id, region_infos) in region_info_mat {
            let pre_sum = prefix_sum(region_infos.iter(), RegionInfo::get_qps);

            let qps = *pre_sum.last().unwrap(); // region_infos is not empty
            let num = self.cfg.detect_times;
            if qps > self.cfg.qps_threshold {
                let recorder = self
                    .recorders
                    .entry(region_id)
                    .or_insert_with(|| Recorder::new(num));

                recorder.update_peer(region_infos[0].get_peer());

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
                            split_key: Key::from_raw(&key).into_encoded(),
                            peer: recorder.peer.clone(),
                        };
                        split_infos.push(split_info);
                        info!("load base split region";"region_id"=>region_id);
                    }
                    self.recorders.remove(&region_id);
                }
            } else {
                self.recorders.remove_entry(&region_id);
            }
            top.push(qps);
        }

        (top.into_vec(), split_infos)
    }

    pub fn clear(&mut self) {
        let interval = Duration::from_secs(self.cfg.detect_times * 2);
        self.recorders
            .retain(|_, recorder| recorder.create_time.elapsed().unwrap() < interval);
    }

    pub fn refresh_cfg(&mut self) {
        if let Some(incoming) = self.cfg_tracker.any_new() {
            self.cfg = incoming.clone();
        }
    }
}

pub fn build_key_range(start_key: &[u8], end_key: &[u8]) -> KeyRange {
    let mut range = KeyRange::default();
    range.set_start_key(start_key.to_vec());
    range.set_end_key(end_key.to_vec());
    range
}

#[cfg(not(target_os = "macos"))]
#[cfg(test)]
mod tests {
    use super::*;

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
            let key_range = build_key_range(start_key, end_key);
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
        sc.sample_key(b"c", b"c", Position::Right);
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

    #[test]
    fn test_hub() {
        let mut hub = SplitHub::new(&SplitHubConfigManager::default());
        hub.cfg.qps_threshold = 1;
        hub.cfg.sample_threshold = 0;

        for i in 0..100 {
            let mut qps_stats = QpsStats::new();
            for _ in 0..100 {
                qps_stats.add(1, &Peer::default(), build_key_range(b"a", b"b"));
                qps_stats.add(1, &Peer::default(), build_key_range(b"b", b"c"));
            }
            let (_, split_infos) = hub.flush(vec![qps_stats]);
            if (i + 1) % hub.cfg.detect_times == 0 {
                assert_eq!(split_infos.len(), 1);
                assert_eq!(
                    Key::from_encoded(split_infos[0].split_key.clone())
                        .into_raw()
                        .unwrap(),
                    b"b"
                );
            }
        }
    }

    const REGION_NUM: u64 = 1000;
    const KEY_RANGE_NUM: u64 = 1000;

    fn default_qps_stats() -> QpsStats {
        let mut qps_stats = QpsStats::new();
        for i in 0..REGION_NUM {
            for _j in 0..KEY_RANGE_NUM {
                qps_stats.add(i, &Peer::default(), build_key_range(b"a", b"b"))
            }
        }
        qps_stats
    }

    #[bench]
    fn recorder_sample(b: &mut test::Bencher) {
        let mut samples = vec![Sample::new(b"c")];
        let key_range = build_key_range(b"a", b"b");
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
            let mut hub = SplitHub::new(&SplitHubConfigManager::default());
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
                qps_stats.add(0, &Peer::default(), build_key_range(&start_key, &key));
            }
        });
    }

    #[bench]
    fn qps_add(b: &mut test::Bencher) {
        let mut qps_stats = default_qps_stats();
        b.iter(|| {
            qps_stats.add(0, &Peer::default(), build_key_range(b"a", b"b"));
        });
    }
}

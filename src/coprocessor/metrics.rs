// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::mem;

use crate::storage::{FlowStatistics, FlowStatsReporter, Statistics};
use tikv_util::collections::HashMap;

use prometheus::local::*;
use prometheus::*;

use kvproto::metapb;
use raftstore::store::SplitInfo;
use rand::Rng;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::{Duration, SystemTime};
use txn_types::Key;

lazy_static! {
    pub static ref COPR_REQ_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_request_duration_seconds",
        "Bucketed histogram of coprocessor request duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_REQ_HANDLE_TIME: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_request_handle_seconds",
        "Bucketed histogram of coprocessor handle request duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_REQ_WAIT_TIME: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_request_wait_seconds",
        "Bucketed histogram of coprocessor request wait duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_REQ_ERROR: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_request_error",
        "Total number of push down request error.",
        &["reason"]
    )
    .unwrap();
    pub static ref COPR_SCAN_KEYS: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_scan_keys",
        "Bucketed histogram of coprocessor per request scan keys",
        &["req"],
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_SCAN_DETAILS: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_scan_details",
        "Bucketed counter of coprocessor scan details for each CF",
        &["req", "cf", "tag"]
    )
    .unwrap();
    pub static ref COPR_QPS_TOPN: GaugeVec = register_gauge_vec!(
        "tikv_coprocessor_qps_topn",
        "tikv_coprocessor_qps_topn",
        &["order"]
    )
    .unwrap();
    pub static ref COPR_ROCKSDB_PERF_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_rocksdb_perf",
        "Total number of RocksDB internal operations from PerfContext",
        &["req", "metric"]
    )
    .unwrap();
    pub static ref COPR_DAG_REQ_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_dag_request_count",
        "Total number of DAG requests",
        &["vec_type"]
    )
    .unwrap();
    pub static ref COPR_RESP_SIZE: IntCounter = register_int_counter!(
        "tikv_coprocessor_response_bytes",
        "Total bytes of response body"
    )
    .unwrap();
}

pub struct CopLocalMetrics {
    pub local_copr_req_histogram_vec: LocalHistogramVec,
    pub local_copr_req_handle_time: LocalHistogramVec,
    pub local_copr_req_wait_time: LocalHistogramVec,
    pub local_copr_scan_keys: LocalHistogramVec,
    pub local_copr_rocksdb_perf_counter: LocalIntCounterVec,
    hub: Hub,
    local_scan_details: HashMap<&'static str, Statistics>,
    local_cop_flow_stats: HashMap<u64, FlowStatistics>,
}

thread_local! {
    pub static TLS_COP_METRICS: RefCell<CopLocalMetrics> = RefCell::new(
        CopLocalMetrics {
            local_copr_req_histogram_vec:
                COPR_REQ_HISTOGRAM_VEC.local(),
            local_copr_req_handle_time:
                COPR_REQ_HANDLE_TIME.local(),
            local_copr_req_wait_time:
                COPR_REQ_WAIT_TIME.local(),
            local_copr_scan_keys:
                COPR_SCAN_KEYS.local(),
            local_copr_rocksdb_perf_counter:
                COPR_ROCKSDB_PERF_COUNTER.local(),
            local_scan_details:
                HashMap::default(),
            local_cop_flow_stats:
                HashMap::default(),
            hub:
                build_hub(),
        }
    );
}

pub fn tls_flush<R: FlowStatsReporter>(reporter: &R) {
    TLS_COP_METRICS.with(|m| {
        // Flush Prometheus metrics
        let mut m = m.borrow_mut();
        m.local_copr_req_histogram_vec.flush();
        m.local_copr_req_handle_time.flush();
        m.local_copr_req_wait_time.flush();
        m.local_copr_scan_keys.flush();
        m.local_copr_rocksdb_perf_counter.flush();

        for (cmd, stat) in m.local_scan_details.drain() {
            for (cf, cf_details) in stat.details().iter() {
                for (tag, count) in cf_details.iter() {
                    COPR_SCAN_DETAILS
                        .with_label_values(&[cmd, *cf, *tag])
                        .inc_by(*count as i64);
                }
            }
        }
        {
            let (top, split_infos) = m.hub.flush();
            reporter.split(split_infos);
            for i in 0..10 {
                if i < top.len() {
                    COPR_QPS_TOPN
                        .with_label_values(&[&i.to_string()])
                        .set(top[i] as f64);
                } else {
                    COPR_QPS_TOPN.with_label_values(&[&i.to_string()]).set(0.0);
                }
            }
        }

        // Report PD metrics
        if m.local_cop_flow_stats.is_empty() {
            // Stats to report to PD is empty, ignore.
            return;
        }

        let mut read_stats = HashMap::default();
        mem::swap(&mut read_stats, &mut m.local_cop_flow_stats);

        reporter.report_read_stats(read_stats);
    });
}

pub fn tls_collect_scan_details(cmd: &'static str, stats: &Statistics) {
    TLS_COP_METRICS.with(|m| {
        m.borrow_mut()
            .local_scan_details
            .entry(cmd)
            .or_insert_with(Default::default)
            .add(stats);
    });
}

pub fn tls_collect_read_flow(region_id: u64, statistics: &Statistics) {
    TLS_COP_METRICS.with(|m| {
        let map = &mut m.borrow_mut().local_cop_flow_stats;
        let flow_stats = map
            .entry(region_id)
            .or_insert_with(crate::storage::FlowStatistics::default);
        flow_stats.add(&statistics.write.flow_stats);
        flow_stats.add(&statistics.data.flow_stats);
    });
}

pub fn tls_collect_qps(region_id: u64, peer: &metapb::Peer, start_key: &[u8], end_key: &[u8]) {
    TLS_COP_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.hub.add(region_id, peer, start_key, end_key);
    });
}

const QPS_THRESHOLD: u64 = 100;
const DETECT_TIMES: u32 = 10;
const TOP_N: u32 = 10;
const DETECT_INTERVAL: Duration = Duration::from_secs(1);
const MIN_SAMPLE_NUM: i32 = 100;
const DEFAULT_SPLIT_SCORE: f64 = 0.9;

pub struct Sample {
    pub key: Vec<u8>,
    pub left: i32,
    pub contained: i32,
    pub right: i32,
}

fn build_sample(key: &[u8]) -> Sample {
    Sample {
        key: key.to_owned(),
        left: 0,
        contained: 0,
        right: 0,
    }
}

pub struct KeyRange {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub qps: u64,
}

fn build_key_range(start_key: &[u8], end_key: &[u8]) -> KeyRange {
    KeyRange {
        start_key: start_key.to_owned(),
        end_key: end_key.to_owned(),
        qps: 0,
    }
}

pub struct Recorder {
    pub samples: Vec<Sample>,
    pub times: u32,
    pub count: u64,
    pub create_time: SystemTime,
}

fn build_recorder() -> Recorder {
    Recorder {
        samples: vec![],
        times: 0,
        count: 0,
        create_time: SystemTime::now(),
    }
}

impl Recorder {
    fn record(&mut self, key_ranges: &[KeyRange]) {
        self.times += 1;
        for key_range in key_ranges.iter() {
            self.count += 1;
            if self.samples.len() < 20 {
                self.samples.push(build_sample(&key_range.start_key));
            } else {
                let i = rand::thread_rng().gen_range(0, self.count) as usize;
                if i < 20 {
                    self.samples[i] = build_sample(&key_range.start_key);
                }
            }
            self.sample(key_range);
        }
    }

    fn sample(&mut self, key_range: &KeyRange) {
        for mut sample in self.samples.iter_mut() {
            if sample.key.cmp(&key_range.start_key) == Ordering::Less {
                sample.left += 1;
            } else if !key_range.end_key.is_empty()
                && sample.key.cmp(&key_range.end_key) == Ordering::Greater
            {
                sample.right += 1;
            } else {
                sample.contained += 1;
            }
        }
    }

    fn split_key(&self) -> Vec<u8> {
        if self.times < DETECT_TIMES {
            return vec![];
        }
        let mut best_index: i32 = -1;
        let mut best_score = DEFAULT_SPLIT_SCORE;
        for index in 0..self.samples.len() {
            let sample = &self.samples[index];
            if sample.contained + sample.left + sample.right < MIN_SAMPLE_NUM {
                continue;
            }
            let diff = (sample.left - sample.right) as f64;
            let balance_score = diff.abs() / (sample.left + sample.right) as f64;
            if balance_score < best_score {
                best_index = index as i32;
                best_score = balance_score;
            }
        }
        if best_index >= 0 {
            return self.samples[best_index as usize].key.clone();
        }
        return vec![];
    }
}

pub struct RegionInfo {
    pub peer: metapb::Peer,
    pub qps: u64,
}

fn build_region_info() -> RegionInfo {
    RegionInfo {
        qps: 0,
        peer: metapb::Peer::default(),
    }
}

impl RegionInfo {
    fn update(&mut self, peer: &metapb::Peer) {
        if self.peer != *peer {
            self.peer = peer.clone();
        }
        self.qps += 1
    }
}

pub struct Hub {
    pub region_qps: HashMap<u64, RegionInfo>,
    pub region_keys: HashMap<u64, Vec<KeyRange>>,
    pub region_recorder: HashMap<u64, Recorder>,
}

fn build_hub() -> Hub {
    Hub {
        region_qps: HashMap::default(),
        region_keys: HashMap::default(),
        region_recorder: HashMap::default(),
    }
}

impl Hub {
    fn add(&mut self, region_id: u64, peer: &metapb::Peer, start_key: &[u8], end_key: &[u8]) {
        let region_info = self
            .region_qps
            .entry(region_id)
            .or_insert_with(build_region_info);
        region_info.update(peer);
        let key_ranges = self.region_keys.entry(region_id).or_insert_with(|| vec![]);
        (*key_ranges).push(build_key_range(start_key, end_key));
    }

    fn clear(&mut self) {
        self.region_keys.clear();
        self.region_qps.clear();
        self.region_recorder.retain(|_, recorder| {
            recorder.create_time.elapsed().unwrap() < DETECT_INTERVAL * DETECT_TIMES * 10
        });
    }

    fn flush(&mut self) -> (Vec<u64>, Vec<SplitInfo>) {
        let mut split_infos = Vec::default();
        let mut top = BinaryHeap::with_capacity(TOP_N as usize);
        for (region_id, region_info) in self.region_qps.iter() {
            let qps = (*region_info).qps;
            if qps > QPS_THRESHOLD {
                let recorder = self
                    .region_recorder
                    .entry(*region_id)
                    .or_insert_with(build_recorder);
                recorder.record(self.region_keys.get(region_id).unwrap());
                let key = recorder.split_key();
                if !key.is_empty() {
                    let split_info = SplitInfo {
                        region_id: *region_id,
                        split_key: Key::from_raw(&key).into_encoded(),
                        peer: (*region_info).peer.clone(),
                    };
                    split_infos.push(split_info);
                    self.region_recorder.remove(region_id);
                    info!("reporter_key";"region_id"=>*region_id);
                }
            } else {
                self.region_recorder.remove_entry(region_id);
            }
            top.push(qps);
        }
        self.clear();
        (top.into_vec(), split_infos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recorder() {
        let mut recorder = build_recorder();

        let key_range = build_key_range(b"a", b"b");
        recorder.record(&[key_range]);
        assert_eq!(recorder.samples.len(), 1);
        assert_eq!(recorder.samples[0].contained, 1);

        let mut key_ranges: Vec<KeyRange> = Vec::new();

        key_ranges.push(build_key_range(b"a", b"b"));
        key_ranges.push(build_key_range(b"b", b"c"));
        key_ranges.push(build_key_range(b"c", b"d"));
        key_ranges.push(build_key_range(b"d", b""));

        for _ in 0..50 {
            recorder.record(key_ranges.as_slice());
        }

        assert_eq!(recorder.samples.len(), 20);
        assert_eq!(recorder.split_key(), b"c");
    }

    #[test]
    fn test_hub() {
        let mut hub = build_hub();

        for i in 0..100 {
            for _ in 0..100 {
                hub.add(1, &metapb::Peer::default(), b"a", b"b");
                hub.add(1, &metapb::Peer::default(), b"b", b"");
            }
            let (_, split_infos) = hub.flush();
            if (i + 1) % DETECT_TIMES == 0 {
                assert_eq!(split_infos.len(), 1);
            }
        }
    }
}

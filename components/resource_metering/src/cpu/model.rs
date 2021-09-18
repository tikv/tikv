// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::ResourceMeteringTag;
use collections::HashMap;
use std::cell::Cell;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Resource tag.
pub type Tag = Vec<u8>;

/// Timestamps in second.
pub type Timestamps = Vec<Timestamp>;

/// Timestamp in second.
pub type Timestamp = u64;

/// CPU times in millisecond.
pub type CpuTimes = Vec<CpuTime>;

/// CPU time in millisecond.
pub type CpuTime = u32;

/// Sum of CPU times in millisecond.
pub type CpuTimeSum = CpuTime;

/// Record value is layout as (<timestamp_secs>, <cpu_time_ms>, total_cpu_time_ms).
pub type RecordValue = (Timestamps, CpuTimes, CpuTimeSum);

/// Raw CPU statistics map.
///
/// This structure is used for initial aggregation in the [CpuRecorder] and also
/// used for reporting to [CpuReporter] through the [CpuCollector].
///
/// [CpuRecorder]: crate::cpu::CpuRecorder
/// [CpuReporter]: crate::cpu::CpuReporter
/// [CpuCollector]: crate::cpu::CpuCollector
#[derive(Debug)]
pub struct RawCpuRecords {
    pub begin_unix_time_secs: u64,
    pub duration: Duration,

    // tag -> ms
    pub records: HashMap<ResourceMeteringTag, u64>,
}

impl Default for RawCpuRecords {
    fn default() -> Self {
        let now_unix_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock may have gone backwards");
        Self {
            begin_unix_time_secs: now_unix_time.as_secs(),
            duration: Duration::default(),
            records: HashMap::default(),
        }
    }
}

/// CPU statistics map.
///
/// This structure is used for final aggregation in the [CpuReporter] and also
/// for uploading to the remote side through [Client].
///
/// [CpuReporter]: crate::cpu::CpuReporter
/// [Client]: crate::client::Client
#[derive(Default)]
pub struct CpuRecords {
    pub records: HashMap<Tag, RecordValue>,
    pub others: HashMap<Timestamp, CpuTime>,
}

impl CpuRecords {
    /// Aggregates [RawCpuRecords] into [CpuRecords].
    pub fn append(&mut self, raw_records: Arc<RawCpuRecords>) {
        // # Before
        //
        // ts: 1630464417
        // records: | tag | cpu time |
        //          | --- | -------- |
        //          | t1  |  500     |
        //          | t2  |  600     |
        //          | t3  |  200     |
        //          | t2  |  100     |

        // # After
        //
        // t1: | ts       | ... | 1630464417 |
        //     | cpu time | ... |    500     |
        //     | total    | $total + 500     |
        //
        // t2: | ts       | ... | 1630464417 |
        //     | cpu time | ... |    700     |
        //     | total    | $total + 700     |
        //
        // t3: | ts       | ... | 1630464417 |
        //     | cpu time | ... |    200     |
        //     | total    | $total + 200     |

        let ts = raw_records.begin_unix_time_secs;

        for (tag, ms) in &raw_records.records {
            let cpu_time = *ms as u32;
            let tag = &tag.infos.extra_attachment;
            if tag.is_empty() {
                continue;
            }

            let record_value = self.records.get_mut(tag);
            if record_value.is_none() {
                self.records
                    .insert(tag.clone(), (vec![ts], vec![cpu_time], cpu_time));
                continue;
            }

            let (ts_list, cpu_list, total_cpu) = record_value.unwrap();
            *total_cpu += cpu_time;
            if *ts_list.last().unwrap() == ts {
                *cpu_list.last_mut().unwrap() += cpu_time;
            } else {
                ts_list.push(ts);
                cpu_list.push(cpu_time);
            }
        }
    }

    /// Keep a maximum of `k` items and aggregate the others into `others`.
    pub fn keep_top_k(&mut self, k: usize) {
        // # Before
        //
        // K: 2
        //
        // t1: | ts       | 1630464416 | 1630464417 |
        //     | cpu time | 300        |    500     |
        //     | total    | 800                     |
        //
        // t2: | ts       | 1630464416 | 1630464417 |
        //     | cpu time | 200        |    700     |
        //     | total    | 900                     |
        //
        // t3: | ts       | 1630464416 | 1630464417 |
        //     | cpu time | 100        |    200     |
        //     | total    | 300                     |
        //
        // t4: | ts       | 1630464416 | 1630464417 |
        //     | cpu time | 500        |    200     |
        //     | total    | 700                     |

        // # After
        //
        // t1: | ts       | 1630464416 | 1630464417 |
        //     | cpu time | 300        |    500     |
        //     | total    | 800                     |
        //
        // t2: | ts       | 1630464416 | 1630464417 |
        //     | cpu time | 200        |    700     |
        //     | total    | 900                     |
        //
        // others: |  ts      | 1630464416 | 1630464417 |
        //         | cpu time | 600        | 400        |

        if self.records.len() <= k {
            return;
        }

        if k == 0 {
            self.records.clear();
            self.others.clear();
            return;
        }

        let mut buf = STATIC_BUF.with(|b| b.take());
        buf.clear();

        // Find kth top total cpu time
        for (_, _, total) in self.records.values() {
            buf.push(*total);
        }
        pdqselect::select_by(&mut buf, k, |a, b| b.cmp(a));
        let kth = buf[k];

        // Evict records with total cpu time less or equal than `kth`
        let others = &mut self.others;
        let evicted_records = self.records.drain_filter(|_, (_, _, total)| *total <= kth);

        // Record evicted cpu time into others
        for (_, (secs_list, cpu_time_list, _)) in evicted_records {
            for (ts, cpu_time) in secs_list.into_iter().zip(cpu_time_list.into_iter()) {
                *others.entry(ts).or_insert(0) += cpu_time;
            }
        }

        STATIC_BUF.with(move |b| b.set(buf));
    }

    /// Whether `CpuRecords` is empty.
    pub fn is_empty(&self) -> bool {
        if self.records.is_empty() {
            assert!(self.others.is_empty());
            true
        } else {
            false
        }
    }

    /// Clear all internal data.
    pub fn clear(&mut self) {
        self.records.clear();
        self.others.clear();
    }
}

thread_local! {
    static STATIC_BUF: Cell<Vec<u32>> = Cell::new(vec![]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TagInfos;

    #[test]
    fn test_cpu_records() {
        let tag1 = ResourceMeteringTag::from(Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            extra_attachment: b"a".to_vec(),
        }));
        let tag2 = ResourceMeteringTag::from(Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            extra_attachment: b"b".to_vec(),
        }));
        let tag3 = ResourceMeteringTag::from(Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            extra_attachment: b"c".to_vec(),
        }));
        let mut records = CpuRecords::default();
        let mut raw_map = HashMap::default();
        raw_map.insert(tag1, 111);
        raw_map.insert(tag2, 222);
        raw_map.insert(tag3, 333);
        let raw = RawCpuRecords {
            begin_unix_time_secs: 1,
            duration: Duration::from_secs(1),
            records: raw_map,
        };
        assert_eq!(records.records.len(), 0);
        records.append(Arc::new(raw));
        assert_eq!(records.records.len(), 3);
        records.keep_top_k(2);
        assert_eq!(records.records.len(), 2);
        assert_eq!(records.others.len(), 1);
    }
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::ResourceMeteringTag;

use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use collections::HashMap;

thread_local! {
    static STATIC_BUF: Cell<Vec<u32>> = Cell::new(vec![]);
}

/// Raw resource statistics record.
#[derive(Debug, Default)]
pub struct RawRecord {
    pub cpu_time: u32, // ms
}

impl RawRecord {
    pub fn merge(&mut self, other: &Self) {
        self.cpu_time += other.cpu_time;
    }
}

/// Raw resource statistics record list with time window.
///
/// This structure is used for initial aggregation in the [Recorder] and also
/// used for reporting to [Reporter] through the [Collector].
///
/// [Recorder]: crate::recorder::Recorder
/// [Reporter]: crate::reporter::Reporter
/// [Collector]: crate::collector::Collector
#[derive(Debug)]
pub struct RawRecords {
    pub begin_unix_time_secs: u64,
    pub duration: Duration,

    // tag -> record
    pub records: HashMap<ResourceMeteringTag, RawRecord>,
}

impl Default for RawRecords {
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

/// Resource statistics.
#[derive(Debug, Default)]
pub struct Record {
    pub timestamps: Vec<u64>,
    pub cpu_time_list: Vec<u32>,
    pub total_cpu_time: u32,
}

/// Resource statistics map.
///
/// This structure is used for final aggregation in the [Reporter] and also
/// for uploading to the remote side through [Client].
///
/// [Reporter]: crate::reporter::CpuReporter
/// [Client]: crate::client::Client
#[derive(Debug, Default)]
pub struct Records {
    pub records: HashMap<Vec<u8>, Record>,
    pub others: HashMap<u64, RawRecord>,
}

impl Records {
    /// Aggregates [RawCpuRecords] into [CpuRecords].
    pub fn append(&mut self, raw_records: Arc<RawRecords>) {
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
        for (tag, raw_record) in &raw_records.records {
            let tag = &tag.infos.extra_attachment;
            if tag.is_empty() {
                continue;
            }
            let record_value = self.records.get_mut(tag);
            if record_value.is_none() {
                self.records.insert(
                    tag.clone(),
                    Record {
                        timestamps: vec![ts],
                        cpu_time_list: vec![raw_record.cpu_time],
                        total_cpu_time: raw_record.cpu_time,
                    },
                );
                continue;
            }
            let record = record_value.unwrap();
            record.total_cpu_time += raw_record.cpu_time;
            if *record.timestamps.last().unwrap() == ts {
                *record.cpu_time_list.last_mut().unwrap() += raw_record.cpu_time;
            } else {
                record.timestamps.push(ts);
                record.cpu_time_list.push(raw_record.cpu_time);
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
        for record in self.records.values() {
            buf.push(record.total_cpu_time);
        }
        pdqselect::select_by(&mut buf, k, |a, b| b.cmp(a));
        let kth = buf[k];

        // Evict records with total cpu time less or equal than `kth`
        let others = &mut self.others;
        let evicted_records = self.records.drain_filter(|_, r| r.total_cpu_time <= kth);

        // Record evicted into others
        for (_, record) in evicted_records {
            for n in 0..record.timestamps.len() {
                others
                    .entry(record.timestamps[n])
                    .or_insert_with(RawRecord::default)
                    .merge(&RawRecord {
                        cpu_time: record.cpu_time_list[n],
                    });
            }
        }

        STATIC_BUF.with(move |b| b.set(buf));
    }

    /// Clear all internal data.
    pub fn clear(&mut self) {
        self.records.clear();
        self.others.clear();
    }

    /// Whether `Records` is empty.
    pub fn is_empty(&self) -> bool {
        if self.records.is_empty() {
            assert!(self.others.is_empty());
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TagInfos;

    #[test]
    fn test_records() {
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
        let mut records = Records::default();
        let mut raw_map = HashMap::default();
        raw_map.insert(tag1, RawRecord { cpu_time: 111 });
        raw_map.insert(tag2, RawRecord { cpu_time: 444 });
        raw_map.insert(tag3, RawRecord { cpu_time: 777 });
        let raw = RawRecords {
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

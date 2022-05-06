// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    sync::{
        atomic::{AtomicU32, Ordering::Relaxed},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use collections::HashMap;
use kvproto::resource_usage_agent::{GroupTagRecord, GroupTagRecordItem, ResourceUsageRecord};
use tikv_util::warn;

use crate::TagInfos;

thread_local! {
    static STATIC_BUF: Cell<Vec<u32>> = Cell::new(vec![]);
}

/// Raw resource statistics record.
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub struct RawRecord {
    pub cpu_time: u32, // ms
    pub read_keys: u32,
    pub write_keys: u32,
}

impl RawRecord {
    pub fn merge(&mut self, other: &Self) {
        self.cpu_time += other.cpu_time;
        self.read_keys += other.read_keys;
        self.write_keys += other.write_keys;
    }

    pub fn merge_summary(&mut self, r: &SummaryRecord) {
        self.read_keys += r.read_keys.load(Relaxed);
        self.write_keys += r.write_keys.load(Relaxed);
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
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct RawRecords {
    pub begin_unix_time_secs: u64,
    pub duration: Duration,

    // tag -> record
    pub records: HashMap<Arc<TagInfos>, RawRecord>,
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

impl RawRecords {
    /// Keep a maximum of `k` self.records and aggregate the others into returned [RawRecord].
    pub fn keep_top_k(&mut self, k: usize) -> RawRecord {
        let mut others = RawRecord::default();
        if self.records.len() <= k {
            return others;
        }
        let mut buf = STATIC_BUF.with(|b| b.take());
        buf.clear();
        // Find kth top cpu time.
        for record in self.records.values() {
            buf.push(record.cpu_time);
        }
        pdqselect::select_by(&mut buf, k, |a, b| b.cmp(a));
        let kth = buf[k];
        // Evict records with cpu time less or equal than `kth`
        let evicted_records = self.records.drain_filter(|_, r| r.cpu_time <= kth);
        // Record evicted into others
        for (_, record) in evicted_records {
            others.merge(&record);
        }
        STATIC_BUF.with(move |b| b.set(buf));
        others
    }

    /// Returns (TopK, Evicted).
    /// It is caller's responsibility to ensure that k < records.len().
    pub fn top_k(
        &self,
        k: usize,
    ) -> (
        impl Iterator<Item = (&Arc<TagInfos>, &RawRecord)>,
        impl Iterator<Item = (&Arc<TagInfos>, &RawRecord)>,
    ) {
        assert!(self.records.len() > k);
        let mut buf = STATIC_BUF.with(|b| b.take());
        buf.clear();
        for record in self.records.values() {
            buf.push(record.cpu_time);
        }
        pdqselect::select_by(&mut buf, k, |a, b| b.cmp(a));
        let kth = buf[k];
        STATIC_BUF.with(move |b| b.set(buf));
        (
            self.records.iter().filter(move |(_, v)| v.cpu_time > kth),
            self.records.iter().filter(move |(_, v)| v.cpu_time <= kth),
        )
    }
}

/// Resource statistics.
///
/// TODO(mornyx): Optimize to Vec<Item{timestamp,cpu_time,read_keys,write_keys}>
#[derive(Debug, Default, Clone)]
pub struct Record {
    pub timestamps: Vec<u64>,
    pub cpu_time_list: Vec<u32>,
    pub read_keys_list: Vec<u32>,
    pub write_keys_list: Vec<u32>,
    pub total_cpu_time: u32,
}

impl From<Record> for Vec<GroupTagRecordItem> {
    fn from(record: Record) -> Self {
        let mut items = Vec::with_capacity(record.timestamps.len());
        for n in 0..record.timestamps.len() {
            let mut item = GroupTagRecordItem::new();
            item.set_timestamp_sec(record.timestamps[n]);
            item.set_cpu_time_ms(record.cpu_time_list[n]);
            item.set_read_keys(record.read_keys_list[n]);
            item.set_write_keys(record.write_keys_list[n]);
            items.push(item);
        }
        items
    }
}

impl Record {
    pub fn valid(&self) -> bool {
        self.timestamps.len() == self.cpu_time_list.len()
            && self.timestamps.len() == self.read_keys_list.len()
            && self.timestamps.len() == self.write_keys_list.len()
    }
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

impl From<Records> for Vec<ResourceUsageRecord> {
    fn from(records: Records) -> Vec<ResourceUsageRecord> {
        let mut res = Vec::with_capacity(records.records.len() + 1);
        for (tag, record) in records.records {
            if !record.valid() {
                warn!("invalid record"); // should not happen
                continue;
            }
            let items: Vec<GroupTagRecordItem> = record.into();
            let mut tag_record = GroupTagRecord::new();
            tag_record.set_resource_group_tag(tag);
            tag_record.set_items(items.into());
            let mut r = ResourceUsageRecord::new();
            r.set_record(tag_record);
            res.push(r);
        }

        if !records.others.is_empty() {
            let mut items = Vec::with_capacity(records.others.len());
            for (
                ts,
                RawRecord {
                    cpu_time,
                    read_keys,
                    write_keys,
                },
            ) in records.others
            {
                let mut item = GroupTagRecordItem::new();
                item.set_timestamp_sec(ts);
                item.set_cpu_time_ms(cpu_time);
                item.set_read_keys(read_keys);
                item.set_write_keys(write_keys);
                items.push(item);
            }
            let mut tag_record = GroupTagRecord::new();
            tag_record.set_items(items.into());
            let mut r = ResourceUsageRecord::new();
            r.set_record(tag_record);
            res.push(r);
        }

        res
    }
}

impl Records {
    /// Aggregates [RawRecords] into [Records].
    pub fn append<'a>(
        &mut self,
        ts: u64,
        iter: impl Iterator<Item = (&'a Arc<TagInfos>, &'a RawRecord)>,
    ) {
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

        for (tag, raw_record) in iter {
            let tag = &tag.extra_attachment;
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
                        read_keys_list: vec![raw_record.read_keys],
                        write_keys_list: vec![raw_record.write_keys],
                        total_cpu_time: raw_record.cpu_time,
                    },
                );
                continue;
            }
            let record = record_value.unwrap();
            record.total_cpu_time += raw_record.cpu_time;
            if *record.timestamps.last().unwrap() == ts {
                *record.cpu_time_list.last_mut().unwrap() += raw_record.cpu_time;
                *record.read_keys_list.last_mut().unwrap() += raw_record.read_keys;
                *record.write_keys_list.last_mut().unwrap() += raw_record.write_keys;
            } else {
                record.timestamps.push(ts);
                record.cpu_time_list.push(raw_record.cpu_time);
                record.read_keys_list.push(raw_record.read_keys);
                record.write_keys_list.push(raw_record.write_keys);
            }
        }
    }

    /// Clear all internal data.
    pub fn clear(&mut self) {
        self.records.clear();
        self.others.clear();
    }

    /// Whether `Records` is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.others.is_empty()
    }
}

#[derive(Debug, Default)]
pub struct SummaryRecord {
    /// Number of keys that have been read.
    pub read_keys: AtomicU32,

    /// Number of keys that have been written.
    pub write_keys: AtomicU32,
}

impl Clone for SummaryRecord {
    fn clone(&self) -> Self {
        Self {
            read_keys: AtomicU32::new(self.read_keys.load(Relaxed)),
            write_keys: AtomicU32::new(self.write_keys.load(Relaxed)),
        }
    }
}

impl SummaryRecord {
    /// Reset all data to zero.
    pub fn reset(&self) {
        self.read_keys.store(0, Relaxed);
        self.write_keys.store(0, Relaxed);
    }

    /// Add two items.
    pub fn merge(&self, other: &Self) {
        self.read_keys
            .fetch_add(other.read_keys.load(Relaxed), Relaxed);
        self.write_keys
            .fetch_add(other.write_keys.load(Relaxed), Relaxed);
    }

    /// Gets the value and writes it to zero.
    #[must_use]
    pub fn take_and_reset(&self) -> Self {
        Self {
            read_keys: AtomicU32::new(self.read_keys.swap(0, Relaxed)),
            write_keys: AtomicU32::new(self.write_keys.swap(0, Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering::Relaxed;

    use super::*;
    use crate::TagInfos;

    #[test]
    fn test_summary_record() {
        let record = SummaryRecord {
            read_keys: AtomicU32::new(1),
            write_keys: AtomicU32::new(2),
        };
        assert_eq!(record.read_keys.load(Relaxed), 1);
        assert_eq!(record.write_keys.load(Relaxed), 2);
        let record2 = record.clone();
        assert_eq!(record2.read_keys.load(Relaxed), 1);
        assert_eq!(record2.write_keys.load(Relaxed), 2);
        record.merge(&SummaryRecord {
            read_keys: AtomicU32::new(3),
            write_keys: AtomicU32::new(4),
        });
        assert_eq!(record.read_keys.load(Relaxed), 4);
        assert_eq!(record.write_keys.load(Relaxed), 6);
        let record2 = record.take_and_reset();
        assert_eq!(record.read_keys.load(Relaxed), 0);
        assert_eq!(record.write_keys.load(Relaxed), 0);
        assert_eq!(record2.read_keys.load(Relaxed), 4);
        assert_eq!(record2.write_keys.load(Relaxed), 6);
        record2.reset();
        assert_eq!(record2.read_keys.load(Relaxed), 0);
        assert_eq!(record2.write_keys.load(Relaxed), 0);
    }

    #[test]
    fn test_records() {
        let tag1 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: b"a".to_vec(),
        });
        let tag2 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: b"b".to_vec(),
        });
        let tag3 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: b"c".to_vec(),
        });
        let mut records = Records::default();
        let mut raw_map = HashMap::default();
        raw_map.insert(
            tag1,
            RawRecord {
                cpu_time: 111,
                read_keys: 222,
                write_keys: 333,
            },
        );
        raw_map.insert(
            tag2,
            RawRecord {
                cpu_time: 444,
                read_keys: 555,
                write_keys: 666,
            },
        );
        raw_map.insert(
            tag3,
            RawRecord {
                cpu_time: 777,
                read_keys: 888,
                write_keys: 999,
            },
        );
        let raw = RawRecords {
            begin_unix_time_secs: 1,
            duration: Duration::from_secs(1),
            records: raw_map,
        };
        assert_eq!(records.records.len(), 0);
        records.append(raw.begin_unix_time_secs, raw.records.iter());
        assert_eq!(records.records.len(), 3);
    }

    #[test]
    fn test_raw_records_top_k() {
        let tag1 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: b"a".to_vec(),
        });
        let tag2 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: b"b".to_vec(),
        });
        let tag3 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: b"c".to_vec(),
        });
        let mut records = HashMap::default();
        records.insert(
            tag1,
            RawRecord {
                cpu_time: 111,
                read_keys: 222,
                write_keys: 333,
            },
        );
        records.insert(
            tag2,
            RawRecord {
                cpu_time: 444,
                read_keys: 555,
                write_keys: 666,
            },
        );
        records.insert(
            tag3,
            RawRecord {
                cpu_time: 777,
                read_keys: 888,
                write_keys: 999,
            },
        );
        let rs = RawRecords {
            begin_unix_time_secs: 1,
            duration: Duration::from_secs(1),
            records,
        };
        let (top, evicted) = rs.top_k(2);
        let others = evicted
            .map(|(_, v)| v)
            .fold(RawRecord::default(), |mut others, r| {
                others.merge(r);
                others
            });
        assert_eq!(top.count(), 2);
        assert_eq!(others.cpu_time, 111);
        assert_eq!(others.read_keys, 222);
        assert_eq!(others.write_keys, 333);
        let (top, evicted) = rs.top_k(0);
        // let top = top.collect::<Vec<(&Arc<TagInfos>, &RawRecord)>>();
        let others = evicted
            .map(|(_, v)| v)
            .fold(RawRecord::default(), |mut others, r| {
                others.merge(r);
                others
            });
        assert_eq!(top.count(), 0);
        assert_eq!(others.cpu_time, 111 + 444 + 777);
        assert_eq!(others.read_keys, 222 + 555 + 888);
        assert_eq!(others.write_keys, 333 + 666 + 999);
    }

    // Issue: https://github.com/tikv/tikv/issues/12234
    #[test]
    fn test_issue_12234() {
        let tag1 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: b"a".to_vec(),
        });
        let tag2 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: b"b".to_vec(),
        });
        let tag3 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: b"c".to_vec(),
        });

        // Keep cpu_time same for all tags.
        let mut raw_records = RawRecords {
            begin_unix_time_secs: 0,
            duration: Duration::new(0, 0),
            records: HashMap::default(),
        };
        raw_records.records.insert(
            tag1,
            RawRecord {
                cpu_time: 111,
                read_keys: 111,
                write_keys: 111,
            },
        );
        raw_records.records.insert(
            tag2,
            RawRecord {
                cpu_time: 111,
                read_keys: 111,
                write_keys: 111,
            },
        );
        raw_records.records.insert(
            tag3,
            RawRecord {
                cpu_time: 111,
                read_keys: 111,
                write_keys: 111,
            },
        );

        // top.len() == 0
        // evicted.len() == 3
        let (top, evicted) = raw_records.top_k(1);

        let mut records = Records::default();
        records.append(0, top);
        let others = records.others.entry(0).or_default();
        evicted.for_each(|(_, v)| {
            others.merge(v);
        });
        assert!(!records.is_empty());
    }
}

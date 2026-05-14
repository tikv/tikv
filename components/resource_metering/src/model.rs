// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering::Relaxed},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use collections::HashMap;
use kvproto::resource_usage_agent::{GroupTagRecord, GroupTagRecordItem, ResourceUsageRecord};
use tikv_util::warn;

use crate::TagInfos;

thread_local! {
    static STATIC_BUF: Cell<Vec<u32>> = const {Cell::new(vec![])};
}

/// Find the kth cpu time in the iterator.
pub fn find_kth_cpu_time<'a>(
    iter: impl Iterator<Item = (&'a Arc<Vec<u8>>, &'a RawRecord)>,
    k: usize,
) -> u32 {
    let mut buf = STATIC_BUF.with(|b| b.take());
    buf.clear();
    for (_, record) in iter {
        buf.push(record.cpu_time);
    }
    pdqselect::select_by(&mut buf, k, |a, b| b.cmp(a));
    let kth = buf[k];
    STATIC_BUF.with(move |b| b.set(buf));
    kth
}

/// Raw resource statistics record.
#[derive(Debug, Default, Copy, Clone, PartialEq)]
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
#[derive(Debug, PartialEq, Clone)]
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
    /// Returns RawRecord aggregated by extra tag.
    pub fn aggregate_by_extra_tag(&self) -> HashMap<Arc<Vec<u8>>, RawRecord> {
        let mut raw_map: HashMap<Arc<Vec<u8>>, RawRecord> = HashMap::default();
        for (tag_info, record) in self.records.iter() {
            let tag = &tag_info.extra_attachment;
            if tag.is_empty() {
                continue;
            }
            let value = raw_map.get_mut(tag);
            if value.is_none() {
                raw_map.insert(tag.clone(), *record);
                continue;
            }
            value.unwrap().merge(record);
        }
        raw_map
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
    pub records: HashMap<Arc<Vec<u8>>, Record>,
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
            tag_record.set_resource_group_tag(tag.to_vec());
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
    /// Append aggregated [RawRecords] into [Records].
    pub fn append<'a>(
        &mut self,
        ts: u64,
        iter: impl Iterator<Item = (&'a Arc<Vec<u8>>, &'a RawRecord)>,
    ) {
        // # Before
        //
        // ts: 1630464417
        // records: | tag | cpu time |
        //          | --- | -------- |
        //          | t1  |  500     |
        //          | t2  |  600     |
        //          | t3  |  200     |

        // # After
        //
        // t1: | ts       | ... | 1630464417 |
        //     | cpu time | ... |    500     |
        //     | total    | $total + 500     |
        //
        // t2: | ts       | ... | 1630464417 |
        //     | cpu time | ... |    600     |
        //     | total    | $total + 600     |
        //
        // t3: | ts       | ... | 1630464417 |
        //     | cpu time | ... |    200     |
        //     | total    | $total + 200     |

        for (tag, raw_record) in iter {
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
            extra_attachment: Arc::new(b"a".to_vec()),
        });
        let tag2 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"b".to_vec()),
        });
        let tag3 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"c".to_vec()),
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
        let agg_map = raw.aggregate_by_extra_tag();
        assert_eq!(records.records.len(), 0);
        records.append(raw.begin_unix_time_secs, agg_map.iter());
        assert_eq!(records.records.len(), 3);
    }

    #[test]
    fn test_raw_records_agg_and_top_k() {
        let tag1 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"a".to_vec()),
        });
        let tag2 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"b".to_vec()),
        });
        let tag3 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"c".to_vec()),
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

        let agg_map = rs.aggregate_by_extra_tag();
        let kth = find_kth_cpu_time(agg_map.iter(), 2);
        let (top, evicted) = (
            agg_map.iter().filter(move |(_, v)| v.cpu_time > kth),
            agg_map.iter().filter(move |(_, v)| v.cpu_time <= kth),
        );
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

        let kth = find_kth_cpu_time(agg_map.iter(), 0);
        let (top, evicted) = (
            agg_map.iter().filter(move |(_, v)| v.cpu_time > kth),
            agg_map.iter().filter(move |(_, v)| v.cpu_time <= kth),
        );
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
            extra_attachment: Arc::new(b"a".to_vec()),
        });
        let tag2 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"b".to_vec()),
        });
        let tag3 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"c".to_vec()),
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

        let agg_map = raw_records.aggregate_by_extra_tag();
        let kth = find_kth_cpu_time(agg_map.iter(), 1);
        // top.len() == 0
        // evicted.len() == 3
        let (top, evicted) = (
            agg_map.iter().filter(move |(_, v)| v.cpu_time > kth),
            agg_map.iter().filter(move |(_, v)| v.cpu_time <= kth),
        );

        let mut records = Records::default();
        records.append(0, top);
        let others = records.others.entry(0).or_default();
        evicted.for_each(|(_, v)| {
            others.merge(v);
        });
        assert!(!records.is_empty());
    }

    #[test]
    fn test_raw_records_agg() {
        let tag1 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"a".to_vec()),
        });
        let tag2 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"b".to_vec()),
        });
        let tag3 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"c".to_vec()),
        });
        // tag4's extra tag is equal to tag1's
        let tag4 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 2,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"a".to_vec()),
        });
        // tag5's extra tag is equal to tag1's
        let tag5 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 3,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"a".to_vec()),
        });
        // tag6's extra tag is equal to tag2's
        let tag6 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 5,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"b".to_vec()),
        });
        let mut records = HashMap::default();
        records.insert(
            tag1.clone(),
            RawRecord {
                cpu_time: 111,
                read_keys: 222,
                write_keys: 333,
            },
        );
        records.insert(
            tag2.clone(),
            RawRecord {
                cpu_time: 444,
                read_keys: 555,
                write_keys: 666,
            },
        );
        records.insert(
            tag3.clone(),
            RawRecord {
                cpu_time: 777,
                read_keys: 888,
                write_keys: 999,
            },
        );
        records.insert(
            tag4,
            RawRecord {
                cpu_time: 1110,
                read_keys: 2220,
                write_keys: 3330,
            },
        );
        records.insert(
            tag5,
            RawRecord {
                cpu_time: 4440,
                read_keys: 5550,
                write_keys: 6660,
            },
        );
        records.insert(
            tag6,
            RawRecord {
                cpu_time: 7770,
                read_keys: 8880,
                write_keys: 9990,
            },
        );
        let rs = RawRecords {
            begin_unix_time_secs: 1,
            duration: Duration::from_secs(1),
            records,
        };

        let agg_map = rs.aggregate_by_extra_tag();
        assert_eq!(agg_map.len(), 3);
        assert_eq!(
            agg_map.get(&tag1.extra_attachment).unwrap().cpu_time,
            111 + 1110 + 4440
        );
        assert_eq!(
            agg_map.get(&tag2.extra_attachment).unwrap().cpu_time,
            444 + 7770
        );
        assert_eq!(agg_map.get(&tag3.extra_attachment).unwrap().cpu_time, 777);
    }
}

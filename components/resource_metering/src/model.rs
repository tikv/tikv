// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use collections::HashMap;
use kvproto::resource_usage_agent::{
    GroupTagRecord, GroupTagRecordItem, RegionRecord, ResourceUsageRecord,
};
use tikv_util::warn;

use crate::TagInfos;

thread_local! {
    static STATIC_CPU_BUF: Cell<Vec<u32>> = const {Cell::new(vec![])};
    static STATIC_NETWORK_BUF: Cell<Vec<u64>> = const {Cell::new(vec![])};
    static STATIC_LOGICAL_IO_BUF: Cell<Vec<u64>> = const {Cell::new(vec![])};
}

/// Find the kth values in the iterator, returns (kth_cpu_time,
/// kth_network_traffic, kth_logical_io)
pub fn find_kth_values<'a, T: 'a>(
    iter: impl Iterator<Item = (&'a T, &'a RawRecord)>,
    k: usize,
) -> (u32, u64, u64) {
    let mut cpu_buf = STATIC_CPU_BUF.with(|b| b.take());
    let mut network_buf = STATIC_NETWORK_BUF.with(|b| b.take());
    let mut logical_io_buf = STATIC_LOGICAL_IO_BUF.with(|b| b.take());
    cpu_buf.clear();
    network_buf.clear();
    logical_io_buf.clear();
    for (_, record) in iter {
        cpu_buf.push(record.cpu_time);
        network_buf.push(record.network_in_bytes + record.network_out_bytes);
        logical_io_buf.push(record.logical_read_bytes + record.logical_write_bytes);
    }
    pdqselect::select_by(&mut cpu_buf, k, |a, b| b.cmp(a));
    let kth_cpu = cpu_buf[k];
    STATIC_CPU_BUF.with(move |b| b.set(cpu_buf));

    pdqselect::select_by(&mut network_buf, k, |a, b| b.cmp(a));
    let kth_network = network_buf[k];
    STATIC_NETWORK_BUF.with(move |b| b.set(network_buf));

    pdqselect::select_by(&mut logical_io_buf, k, |a, b| b.cmp(a));
    let kth_logical_io = logical_io_buf[k];
    STATIC_LOGICAL_IO_BUF.with(move |b| b.set(logical_io_buf));

    (kth_cpu, kth_network, kth_logical_io)
}

/// Find the kth cpu time in the iterator.
pub fn find_kth_cpu_time<'a, T: 'a>(
    iter: impl Iterator<Item = (&'a T, &'a RawRecord)>,
    k: usize,
) -> u32 {
    let mut buf = STATIC_CPU_BUF.with(|b| b.take());
    buf.clear();
    for (_, record) in iter {
        buf.push(record.cpu_time);
    }
    pdqselect::select_by(&mut buf, k, |a, b| b.cmp(a));
    let kth = buf[k];
    STATIC_CPU_BUF.with(move |b| b.set(buf));
    kth
}

/// Get two iterators: first is the one whose cpu_time > kth_cpu,
/// second is the one whose cpu_time <= kth_cpu.
pub fn get_iter_for_cpu_time<'a, T: 'a>(
    records: &'a HashMap<T, RawRecord>,
    kth_cpu: u32,
) -> (
    impl Iterator<Item = (&'a T, &'a RawRecord)>,
    impl Iterator<Item = (&'a T, &'a RawRecord)>,
) {
    (
        records.iter().filter(move |(_, v)| v.cpu_time > kth_cpu),
        records.iter().filter(move |(_, v)| v.cpu_time <= kth_cpu),
    )
}

/// Get two iterators, first is the one whose cpu_time > kth_cpu or network_io >
/// kth_network or logical_io > kth_logical_io, second is the one whose cpu_time
/// <= kth_cpu and network_io <= kth_network and logical_io <= kth_logical_io.
pub fn get_iter_for_cpu_network_io<'a, T: 'a>(
    records: &'a HashMap<T, RawRecord>,
    kth_cpu: u32,
    kth_network: u64,
    kth_logical_io: u64,
) -> (
    impl Iterator<Item = (&'a T, &'a RawRecord)>,
    impl Iterator<Item = (&'a T, &'a RawRecord)>,
) {
    (
        records.iter().filter(move |(_, v)| {
            v.cpu_time > kth_cpu
                || v.network_in_bytes + v.network_out_bytes > kth_network
                || v.logical_read_bytes + v.logical_write_bytes > kth_logical_io
        }),
        records.iter().filter(move |(_, v)| {
            v.cpu_time <= kth_cpu
                && v.network_in_bytes + v.network_out_bytes <= kth_network
                && v.logical_read_bytes + v.logical_write_bytes <= kth_logical_io
        }),
    )
}

/// Append raw_record to records[key] at timestamp ts.
fn append_impl<T>(records: &mut HashMap<T, Record>, ts: u64, key: &T, raw_record: &RawRecord)
where
    T: Clone + Eq + Hash,
{
    let record_value = records.get_mut(key);
    if record_value.is_none() {
        records.insert(
            key.clone(),
            Record {
                timestamps: vec![ts],
                cpu_time_list: vec![raw_record.cpu_time],
                read_keys_list: vec![raw_record.read_keys],
                write_keys_list: vec![raw_record.write_keys],
                total_cpu_time: raw_record.cpu_time,
                logical_read_bytes_list: vec![raw_record.logical_read_bytes],
                logical_write_bytes_list: vec![raw_record.logical_write_bytes],
                network_in_bytes_list: vec![raw_record.network_in_bytes],
                network_out_bytes_list: vec![raw_record.network_out_bytes],
            },
        );
        return;
    }
    let record = record_value.unwrap();
    record.total_cpu_time += raw_record.cpu_time;
    if *record.timestamps.last().unwrap() == ts {
        *record.cpu_time_list.last_mut().unwrap() += raw_record.cpu_time;
        *record.read_keys_list.last_mut().unwrap() += raw_record.read_keys;
        *record.write_keys_list.last_mut().unwrap() += raw_record.write_keys;
        *record.logical_read_bytes_list.last_mut().unwrap() += raw_record.logical_read_bytes;
        *record.logical_write_bytes_list.last_mut().unwrap() += raw_record.logical_write_bytes;
        *record.network_in_bytes_list.last_mut().unwrap() += raw_record.network_in_bytes;
        *record.network_out_bytes_list.last_mut().unwrap() += raw_record.network_out_bytes;
    } else {
        record.timestamps.push(ts);
        record.cpu_time_list.push(raw_record.cpu_time);
        record.read_keys_list.push(raw_record.read_keys);
        record.write_keys_list.push(raw_record.write_keys);
        record
            .logical_read_bytes_list
            .push(raw_record.logical_read_bytes);
        record
            .logical_write_bytes_list
            .push(raw_record.logical_write_bytes);
        record
            .network_in_bytes_list
            .push(raw_record.network_in_bytes);
        record
            .network_out_bytes_list
            .push(raw_record.network_out_bytes);
    }
}

/// Pick top n agged raw records, then append picked topN records and merge
/// unpicked ones to others. If enable_network_io_collection is true, pick top n
/// records by cpu_time, network_io and logical_io, otherwise, pick top n
/// records by cpu_time only.
pub fn handle_records_impl<'a, K, T>(
    records: &'a mut T,
    enable_network_io_collection: bool,
    agg_map: &'a HashMap<K, crate::RawRecord>,
    ts: u64,
    n: usize,
) where
    T: AppendableRecords<K>,
    K: Clone + Eq + Hash,
{
    if n >= agg_map.len() {
        records.append(ts, agg_map.iter());
        return;
    }
    if enable_network_io_collection {
        let (kth_cpu, kth_network, kth_logical_io) = find_kth_values(agg_map.iter(), n);
        let (picked_iter, unpicked_iter) =
            get_iter_for_cpu_network_io(agg_map, kth_cpu, kth_network, kth_logical_io);
        records.append(ts, picked_iter);
        records.merge_other(ts, unpicked_iter);
    } else {
        let kth_cpu = find_kth_cpu_time(agg_map.iter(), n);
        let (picked_iter, unpicked_iter) = get_iter_for_cpu_time(agg_map, kth_cpu);
        records.append(ts, picked_iter);
        records.merge_other(ts, unpicked_iter);
    }
}

/// Raw resource statistics record.
#[derive(Debug, Default, Copy, Clone, PartialEq)]
pub struct RawRecord {
    pub cpu_time: u32, // ms
    pub read_keys: u32,
    pub write_keys: u32,
    pub logical_read_bytes: u64,
    pub logical_write_bytes: u64,
    pub network_in_bytes: u64,
    pub network_out_bytes: u64,
}

impl RawRecord {
    pub fn merge(&mut self, other: &Self) {
        self.cpu_time += other.cpu_time;
        self.read_keys += other.read_keys;
        self.write_keys += other.write_keys;
        self.logical_read_bytes += other.logical_read_bytes;
        self.logical_write_bytes += other.logical_write_bytes;
        self.network_in_bytes += other.network_in_bytes;
        self.network_out_bytes += other.network_out_bytes;
    }

    pub fn merge_summary(&mut self, r: &SummaryRecord) {
        self.read_keys += r.read_keys.load(Relaxed);
        self.write_keys += r.write_keys.load(Relaxed);
        self.logical_read_bytes += r.logical_read_bytes.load(Relaxed);
        self.logical_write_bytes += r.logical_write_bytes.load(Relaxed);
        self.network_in_bytes += r.network_in_bytes.load(Relaxed);
        self.network_out_bytes += r.network_out_bytes.load(Relaxed);
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
    /// Returns (RawRecord aggregated by extra tag, RawRecord aggregated by
    /// region id). Merge these two aggregations together to save one
    /// iteration.
    pub fn aggregate_by_extra_tag_and_region(
        &self,
    ) -> (HashMap<Arc<Vec<u8>>, RawRecord>, HashMap<u64, RawRecord>) {
        let mut raw_map: HashMap<Arc<Vec<u8>>, RawRecord> = HashMap::default();
        let mut region_raw_map: HashMap<u64, RawRecord> = HashMap::default();
        for (tag_info, record) in self.records.iter() {
            let tag = &tag_info.extra_attachment;
            if !tag.is_empty() {
                let value = raw_map.get_mut(tag);
                if let Some(val) = value {
                    val.merge(record);
                } else {
                    raw_map.insert(tag.clone(), *record);
                }
            }

            let region_id = tag_info.region_id;
            if region_id != 0 {
                let value = region_raw_map.get_mut(&region_id);
                if let Some(val) = value {
                    val.merge(record);
                } else {
                    region_raw_map.insert(region_id, *record);
                }
            }
        }
        (raw_map, region_raw_map)
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
    pub logical_read_bytes_list: Vec<u64>,
    pub logical_write_bytes_list: Vec<u64>,
    pub network_in_bytes_list: Vec<u64>,
    pub network_out_bytes_list: Vec<u64>,
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
            item.set_logical_read_bytes(record.logical_read_bytes_list[n]);
            item.set_logical_write_bytes(record.logical_write_bytes_list[n]);
            item.set_network_in_bytes(record.network_in_bytes_list[n]);
            item.set_network_out_bytes(record.network_out_bytes_list[n]);
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
            && self.timestamps.len() == self.logical_read_bytes_list.len()
            && self.timestamps.len() == self.logical_write_bytes_list.len()
            && self.timestamps.len() == self.network_in_bytes_list.len()
            && self.timestamps.len() == self.network_out_bytes_list.len()
    }
}

pub trait AppendableRecords<K> {
    /// Append picked aggregated [RawRecords].
    fn append<'a>(&mut self, ts: u64, iter: impl Iterator<Item = (&'a K, &'a RawRecord)>)
    where
        K: 'a;
    /// Merge unpicked aggregated [RawRecords].
    fn merge_other<'a>(&mut self, ts: u64, iter: impl Iterator<Item = (&'a K, &'a RawRecord)>)
    where
        K: 'a;
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
                    logical_read_bytes,
                    logical_write_bytes,
                    network_in_bytes,
                    network_out_bytes,
                },
            ) in records.others
            {
                let mut item = GroupTagRecordItem::new();
                item.set_timestamp_sec(ts);
                item.set_cpu_time_ms(cpu_time);
                item.set_read_keys(read_keys);
                item.set_write_keys(write_keys);
                item.set_logical_read_bytes(logical_read_bytes);
                item.set_logical_write_bytes(logical_write_bytes);
                item.set_network_in_bytes(network_in_bytes);
                item.set_network_out_bytes(network_out_bytes);
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

impl AppendableRecords<Arc<Vec<u8>>> for Records {
    fn append<'a>(
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
            append_impl(&mut self.records, ts, tag, raw_record);
        }
    }

    fn merge_other<'a>(
        &mut self,
        ts: u64,
        iter: impl Iterator<Item = (&'a Arc<Vec<u8>>, &'a RawRecord)>,
    ) {
        let others = self.others.entry(ts).or_default();
        iter.for_each(|(_, v)| {
            others.merge(v);
        });
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
            append_impl(&mut self.records, ts, tag, raw_record);
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

/// Resource statistics map.
///
/// This structure is used for final aggregation in the [Reporter] and also
/// for uploading to the remote side through [Client].
///
/// [Reporter]: crate::reporter::CpuReporter
/// [Client]: crate::client::Client
#[derive(Debug, Default)]
pub struct RegionRecords {
    pub records: HashMap<u64, Record>,
    pub others: HashMap<u64, RawRecord>,
}

impl From<RegionRecords> for Vec<ResourceUsageRecord> {
    fn from(records: RegionRecords) -> Vec<ResourceUsageRecord> {
        let mut res = Vec::with_capacity(records.records.len() + 1);
        for (key, record) in records.records {
            if !record.valid() {
                warn!("invalid record"); // should not happen
                continue;
            }
            let items: Vec<GroupTagRecordItem> = record.into();
            let mut region_record = RegionRecord::new();
            region_record.set_region_id(key);
            region_record.set_items(items.into());
            let mut r = ResourceUsageRecord::new();
            r.set_region_record(region_record);
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
                    logical_read_bytes,
                    logical_write_bytes,
                    network_in_bytes,
                    network_out_bytes,
                },
            ) in records.others
            {
                let mut item = GroupTagRecordItem::new();
                item.set_timestamp_sec(ts);
                item.set_cpu_time_ms(cpu_time);
                item.set_read_keys(read_keys);
                item.set_write_keys(write_keys);
                item.set_logical_read_bytes(logical_read_bytes);
                item.set_logical_write_bytes(logical_write_bytes);
                item.set_network_in_bytes(network_in_bytes);
                item.set_network_out_bytes(network_out_bytes);
                items.push(item);
            }
            let mut region_record = RegionRecord::new();
            region_record.set_items(items.into());
            let mut r = ResourceUsageRecord::new();
            r.set_region_record(region_record);
            res.push(r);
        }

        res
    }
}

impl AppendableRecords<u64> for RegionRecords {
    fn append<'a>(&mut self, ts: u64, iter: impl Iterator<Item = (&'a u64, &'a RawRecord)>) {
        // # Before
        //
        // ts: 1630464417
        // records: | region_id | cpu time |
        //          | --------- | -------- |
        //          | 1         |  500     |
        //          | 2         |  600     |
        //          | 3         |  200     |

        // # After
        //
        // 1:  | ts       | ... | 1630464417 |
        //     | cpu time | ... |    500     |
        //     | total    | $total + 500     |
        //
        // 2:  | ts       | ... | 1630464417 |
        //     | cpu time | ... |    600     |
        //     | total    | $total + 600     |
        //
        // 3:  | ts       | ... | 1630464417 |
        //     | cpu time | ... |    200     |
        //     | total    | $total + 200     |
        for (region_id, raw_record) in iter {
            if *region_id == 0 {
                continue;
            }
            append_impl(&mut self.records, ts, region_id, raw_record);
        }
    }

    fn merge_other<'a>(&mut self, ts: u64, iter: impl Iterator<Item = (&'a u64, &'a RawRecord)>) {
        let others = self.others.entry(ts).or_default();
        iter.for_each(|(_, v)| {
            others.merge(v);
        });
    }
}

impl RegionRecords {
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

    /// Logical read bytes. TableScan executor's total read bytes recorded in
    /// execution summary.
    pub logical_read_bytes: AtomicU64,

    /// Logical write bytes.
    pub logical_write_bytes: AtomicU64,

    /// Network input bytes.
    pub network_in_bytes: AtomicU64,

    /// Network output bytes.
    pub network_out_bytes: AtomicU64,
}

impl Clone for SummaryRecord {
    fn clone(&self) -> Self {
        Self {
            read_keys: AtomicU32::new(self.read_keys.load(Relaxed)),
            write_keys: AtomicU32::new(self.write_keys.load(Relaxed)),
            logical_read_bytes: AtomicU64::new(self.logical_read_bytes.load(Relaxed)),
            logical_write_bytes: AtomicU64::new(self.logical_write_bytes.load(Relaxed)),
            network_in_bytes: AtomicU64::new(self.network_in_bytes.load(Relaxed)),
            network_out_bytes: AtomicU64::new(self.network_out_bytes.load(Relaxed)),
        }
    }
}

impl SummaryRecord {
    /// Reset all data to zero.
    pub fn reset(&self) {
        self.read_keys.store(0, Relaxed);
        self.write_keys.store(0, Relaxed);
        self.logical_read_bytes.store(0, Relaxed);
        self.logical_write_bytes.store(0, Relaxed);
        self.network_in_bytes.store(0, Relaxed);
        self.network_out_bytes.store(0, Relaxed);
    }

    /// Add two items.
    pub fn merge(&self, other: &Self) {
        self.read_keys
            .fetch_add(other.read_keys.load(Relaxed), Relaxed);
        self.write_keys
            .fetch_add(other.write_keys.load(Relaxed), Relaxed);
        self.logical_read_bytes
            .fetch_add(other.logical_read_bytes.load(Relaxed), Relaxed);
        self.logical_write_bytes
            .fetch_add(other.logical_write_bytes.load(Relaxed), Relaxed);
        self.network_in_bytes
            .fetch_add(other.network_in_bytes.load(Relaxed), Relaxed);
        self.network_out_bytes
            .fetch_add(other.network_out_bytes.load(Relaxed), Relaxed);
    }

    /// Gets the value and writes it to zero.
    #[must_use]
    pub fn take_and_reset(&self) -> Self {
        Self {
            read_keys: AtomicU32::new(self.read_keys.swap(0, Relaxed)),
            write_keys: AtomicU32::new(self.write_keys.swap(0, Relaxed)),
            logical_read_bytes: AtomicU64::new(self.logical_read_bytes.swap(0, Relaxed)),
            logical_write_bytes: AtomicU64::new(self.logical_write_bytes.swap(0, Relaxed)),
            network_in_bytes: AtomicU64::new(self.network_in_bytes.swap(0, Relaxed)),
            network_out_bytes: AtomicU64::new(self.network_out_bytes.swap(0, Relaxed)),
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
            network_in_bytes: AtomicU64::new(10),
            network_out_bytes: AtomicU64::new(20),
            logical_read_bytes: AtomicU64::new(100),
            logical_write_bytes: AtomicU64::new(200),
        };
        assert_eq!(record.read_keys.load(Relaxed), 1);
        assert_eq!(record.write_keys.load(Relaxed), 2);
        assert_eq!(record.network_in_bytes.load(Relaxed), 10);
        assert_eq!(record.network_out_bytes.load(Relaxed), 20);
        assert_eq!(record.logical_read_bytes.load(Relaxed), 100);
        assert_eq!(record.logical_write_bytes.load(Relaxed), 200);
        let record2 = record.clone();
        assert_eq!(record2.read_keys.load(Relaxed), 1);
        assert_eq!(record2.write_keys.load(Relaxed), 2);
        assert_eq!(record2.network_in_bytes.load(Relaxed), 10);
        assert_eq!(record2.network_out_bytes.load(Relaxed), 20);
        assert_eq!(record2.logical_read_bytes.load(Relaxed), 100);
        assert_eq!(record2.logical_write_bytes.load(Relaxed), 200);
        record.merge(&SummaryRecord {
            read_keys: AtomicU32::new(3),
            write_keys: AtomicU32::new(4),
            network_in_bytes: AtomicU64::new(30),
            network_out_bytes: AtomicU64::new(40),
            logical_read_bytes: AtomicU64::new(300),
            logical_write_bytes: AtomicU64::new(400),
        });
        assert_eq!(record.read_keys.load(Relaxed), 4);
        assert_eq!(record.write_keys.load(Relaxed), 6);
        assert_eq!(record.network_in_bytes.load(Relaxed), 40);
        assert_eq!(record.network_out_bytes.load(Relaxed), 60);
        assert_eq!(record.logical_read_bytes.load(Relaxed), 400);
        assert_eq!(record.logical_write_bytes.load(Relaxed), 600);
        let record2 = record.take_and_reset();
        assert_eq!(record.read_keys.load(Relaxed), 0);
        assert_eq!(record.write_keys.load(Relaxed), 0);
        assert_eq!(record.network_in_bytes.load(Relaxed), 0);
        assert_eq!(record.network_out_bytes.load(Relaxed), 0);
        assert_eq!(record.logical_read_bytes.load(Relaxed), 0);
        assert_eq!(record.logical_write_bytes.load(Relaxed), 0);
        assert_eq!(record2.read_keys.load(Relaxed), 4);
        assert_eq!(record2.write_keys.load(Relaxed), 6);
        assert_eq!(record2.network_in_bytes.load(Relaxed), 40);
        assert_eq!(record2.network_out_bytes.load(Relaxed), 60);
        assert_eq!(record2.logical_read_bytes.load(Relaxed), 400);
        assert_eq!(record2.logical_write_bytes.load(Relaxed), 600);
        record2.reset();
        assert_eq!(record2.read_keys.load(Relaxed), 0);
        assert_eq!(record2.write_keys.load(Relaxed), 0);
        assert_eq!(record2.network_in_bytes.load(Relaxed), 0);
        assert_eq!(record2.network_out_bytes.load(Relaxed), 0);
        assert_eq!(record2.logical_read_bytes.load(Relaxed), 0);
        assert_eq!(record2.logical_write_bytes.load(Relaxed), 0);
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
                network_in_bytes: 1111,
                network_out_bytes: 2222,
                logical_read_bytes: 3333,
                logical_write_bytes: 4444,
            },
        );
        raw_map.insert(
            tag2,
            RawRecord {
                cpu_time: 444,
                read_keys: 555,
                write_keys: 666,
                network_in_bytes: 4444,
                network_out_bytes: 5555,
                logical_read_bytes: 6666,
                logical_write_bytes: 7777,
            },
        );
        raw_map.insert(
            tag3,
            RawRecord {
                cpu_time: 777,
                read_keys: 888,
                write_keys: 999,
                network_in_bytes: 7777,
                network_out_bytes: 8888,
                logical_read_bytes: 9999,
                logical_write_bytes: 11110,
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
                network_in_bytes: 1111,
                network_out_bytes: 2222,
                logical_read_bytes: 3333,
                logical_write_bytes: 4444,
            },
        );
        records.insert(
            tag2,
            RawRecord {
                cpu_time: 444,
                read_keys: 555,
                write_keys: 666,
                network_in_bytes: 4444,
                network_out_bytes: 5555,
                logical_read_bytes: 6666,
                logical_write_bytes: 7777,
            },
        );
        records.insert(
            tag3,
            RawRecord {
                cpu_time: 777,
                read_keys: 888,
                write_keys: 999,
                network_in_bytes: 7777,
                network_out_bytes: 8888,
                logical_read_bytes: 9999,
                logical_write_bytes: 11110,
            },
        );
        let rs = RawRecords {
            begin_unix_time_secs: 1,
            duration: Duration::from_secs(1),
            records,
        };

        let agg_map = rs.aggregate_by_extra_tag();
        let kth = find_kth_cpu_time(agg_map.iter(), 2);
        let (top, evicted) = get_iter_for_cpu_time(&agg_map, kth);
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
        assert_eq!(others.network_in_bytes, 1111);
        assert_eq!(others.network_out_bytes, 2222);
        assert_eq!(others.logical_read_bytes, 3333);
        assert_eq!(others.logical_write_bytes, 4444);

        let kth = find_kth_cpu_time(agg_map.iter(), 0);
        let (top, evicted) = get_iter_for_cpu_time(&agg_map, kth);
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
        assert_eq!(others.network_in_bytes, 1111 + 4444 + 7777);
        assert_eq!(others.network_out_bytes, 2222 + 5555 + 8888);
        assert_eq!(others.logical_read_bytes, 3333 + 6666 + 9999);
        assert_eq!(others.logical_write_bytes, 4444 + 7777 + 11110);
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
                network_in_bytes: 111,
                network_out_bytes: 111,
                logical_read_bytes: 111,
                logical_write_bytes: 111,
            },
        );
        raw_records.records.insert(
            tag2,
            RawRecord {
                cpu_time: 111,
                read_keys: 111,
                write_keys: 111,
                network_in_bytes: 111,
                network_out_bytes: 111,
                logical_read_bytes: 111,
                logical_write_bytes: 111,
            },
        );
        raw_records.records.insert(
            tag3,
            RawRecord {
                cpu_time: 111,
                read_keys: 111,
                write_keys: 111,
                network_in_bytes: 111,
                network_out_bytes: 111,
                logical_read_bytes: 111,
                logical_write_bytes: 111,
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
                network_in_bytes: 1111,
                network_out_bytes: 2222,
                logical_read_bytes: 3333,
                logical_write_bytes: 4444,
            },
        );
        records.insert(
            tag2.clone(),
            RawRecord {
                cpu_time: 444,
                read_keys: 555,
                write_keys: 666,
                network_in_bytes: 4444,
                network_out_bytes: 5555,
                logical_read_bytes: 6666,
                logical_write_bytes: 7777,
            },
        );
        records.insert(
            tag3.clone(),
            RawRecord {
                cpu_time: 777,
                read_keys: 888,
                write_keys: 999,
                network_in_bytes: 7777,
                network_out_bytes: 8888,
                logical_read_bytes: 9999,
                logical_write_bytes: 11110,
            },
        );
        records.insert(
            tag4.clone(),
            RawRecord {
                cpu_time: 1110,
                read_keys: 2220,
                write_keys: 3330,
                network_in_bytes: 11110,
                network_out_bytes: 22220,
                logical_read_bytes: 33330,
                logical_write_bytes: 44440,
            },
        );
        records.insert(
            tag5.clone(),
            RawRecord {
                cpu_time: 4440,
                read_keys: 5550,
                write_keys: 6660,
                network_in_bytes: 44440,
                network_out_bytes: 55550,
                logical_read_bytes: 66660,
                logical_write_bytes: 77770,
            },
        );
        records.insert(
            tag6.clone(),
            RawRecord {
                cpu_time: 7770,
                read_keys: 8880,
                write_keys: 9990,
                network_in_bytes: 77770,
                network_out_bytes: 88880,
                logical_read_bytes: 99990,
                logical_write_bytes: 111110,
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
            agg_map.get(&tag1.extra_attachment).unwrap().read_keys,
            222 + 2220 + 5550
        );
        assert_eq!(
            agg_map.get(&tag1.extra_attachment).unwrap().write_keys,
            333 + 3330 + 6660
        );
        assert_eq!(
            agg_map
                .get(&tag1.extra_attachment)
                .unwrap()
                .network_in_bytes,
            1111 + 11110 + 44440
        );
        assert_eq!(
            agg_map
                .get(&tag1.extra_attachment)
                .unwrap()
                .network_out_bytes,
            2222 + 22220 + 55550
        );
        assert_eq!(
            agg_map
                .get(&tag1.extra_attachment)
                .unwrap()
                .logical_read_bytes,
            3333 + 33330 + 66660
        );
        assert_eq!(
            agg_map
                .get(&tag1.extra_attachment)
                .unwrap()
                .logical_write_bytes,
            4444 + 44440 + 77770
        );
        assert_eq!(
            agg_map.get(&tag2.extra_attachment).unwrap().cpu_time,
            444 + 7770
        );
        assert_eq!(agg_map.get(&tag3.extra_attachment).unwrap().cpu_time, 777);
    }

    #[test]
    fn test_raw_records_agg_by_region() {
        let tag1 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 1,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"a".to_vec()),
        });
        let tag2 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 2,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"b".to_vec()),
        });
        let tag3 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 3,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"c".to_vec()),
        });
        // tag4's region_id is equal to tag1's
        let tag4 = Arc::new(TagInfos {
            store_id: 1,
            region_id: 1,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"a".to_vec()),
        });
        // tag5's region_id is equal to tag1's
        let tag5 = Arc::new(TagInfos {
            store_id: 2,
            region_id: 1,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"a".to_vec()),
        });
        // tag6's region_id is equal to tag2's
        let tag6 = Arc::new(TagInfos {
            store_id: 3,
            region_id: 2,
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
                network_in_bytes: 1111,
                network_out_bytes: 2222,
                logical_read_bytes: 3333,
                logical_write_bytes: 4444,
            },
        );
        records.insert(
            tag2.clone(),
            RawRecord {
                cpu_time: 444,
                read_keys: 555,
                write_keys: 666,
                network_in_bytes: 4444,
                network_out_bytes: 5555,
                logical_read_bytes: 6666,
                logical_write_bytes: 7777,
            },
        );
        records.insert(
            tag3.clone(),
            RawRecord {
                cpu_time: 777,
                read_keys: 888,
                write_keys: 999,
                network_in_bytes: 7777,
                network_out_bytes: 8888,
                logical_read_bytes: 9999,
                logical_write_bytes: 11110,
            },
        );
        records.insert(
            tag4.clone(),
            RawRecord {
                cpu_time: 1110,
                read_keys: 2220,
                write_keys: 3330,
                network_in_bytes: 11110,
                network_out_bytes: 22220,
                logical_read_bytes: 33330,
                logical_write_bytes: 44440,
            },
        );
        records.insert(
            tag5.clone(),
            RawRecord {
                cpu_time: 4440,
                read_keys: 5550,
                write_keys: 6660,
                network_in_bytes: 44440,
                network_out_bytes: 55550,
                logical_read_bytes: 66660,
                logical_write_bytes: 77770,
            },
        );
        records.insert(
            tag6.clone(),
            RawRecord {
                cpu_time: 7770,
                read_keys: 8880,
                write_keys: 9990,
                network_in_bytes: 77770,
                network_out_bytes: 88880,
                logical_read_bytes: 99990,
                logical_write_bytes: 111110,
            },
        );
        let rs = RawRecords {
            begin_unix_time_secs: 1,
            duration: Duration::from_secs(1),
            records,
        };

        let (_, agg_map) = rs.aggregate_by_extra_tag_and_region();
        assert_eq!(agg_map.len(), 3);
        assert_eq!(
            agg_map.get(&tag1.region_id).unwrap().cpu_time,
            111 + 1110 + 4440
        );
        assert_eq!(
            agg_map.get(&tag1.region_id).unwrap().read_keys,
            222 + 2220 + 5550
        );
        assert_eq!(
            agg_map.get(&tag1.region_id).unwrap().write_keys,
            333 + 3330 + 6660
        );
        assert_eq!(
            agg_map.get(&tag1.region_id).unwrap().network_in_bytes,
            1111 + 11110 + 44440
        );
        assert_eq!(
            agg_map.get(&tag1.region_id).unwrap().network_out_bytes,
            2222 + 22220 + 55550
        );
        assert_eq!(
            agg_map.get(&tag1.region_id).unwrap().logical_read_bytes,
            3333 + 33330 + 66660
        );
        assert_eq!(
            agg_map.get(&tag1.region_id).unwrap().logical_write_bytes,
            4444 + 44440 + 77770
        );
        assert_eq!(agg_map.get(&tag2.region_id).unwrap().cpu_time, 444 + 7770);
        assert_eq!(agg_map.get(&tag3.region_id).unwrap().cpu_time, 777);
    }

    #[test]
    fn test_pick_top_k_cpu_network_io() {
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
        // tag1 largest network
        records.insert(
            tag1,
            RawRecord {
                cpu_time: 111,
                read_keys: 222,
                write_keys: 333,
                network_in_bytes: 9999,
                network_out_bytes: 8888,
                logical_read_bytes: 7777,
                logical_write_bytes: 6666,
            },
        );
        // tag2 largest logical io
        records.insert(
            tag2,
            RawRecord {
                cpu_time: 444,
                read_keys: 555,
                write_keys: 666,
                network_in_bytes: 7777,
                network_out_bytes: 6666,
                logical_read_bytes: 9999,
                logical_write_bytes: 9999,
            },
        );
        // tag3 largest cpu
        records.insert(
            tag3,
            RawRecord {
                cpu_time: 777,
                read_keys: 888,
                write_keys: 999,
                network_in_bytes: 1111,
                network_out_bytes: 2222,
                logical_read_bytes: 3333,
                logical_write_bytes: 4444,
            },
        );
        let rs = RawRecords {
            begin_unix_time_secs: 1,
            duration: Duration::from_secs(1),
            records,
        };

        let agg_map = rs.aggregate_by_extra_tag();
        let (kth_cpu, kth_network, kth_logical_io) = find_kth_values(agg_map.iter(), 2);
        let (top, evicted) =
            get_iter_for_cpu_network_io(&agg_map, kth_cpu, kth_network, kth_logical_io);
        let others = evicted
            .map(|(_, v)| v)
            .fold(RawRecord::default(), |mut others, r| {
                others.merge(r);
                others
            });
        assert_eq!(top.count(), 3);
        assert_eq!(others.cpu_time, 0);

        // With unpicked tags
        let tag4 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"d".to_vec()),
        });
        let tag5 = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"ad".to_vec()),
        });
        let mut records = rs.records.clone();
        // tag4 won't be picked
        records.insert(
            tag4,
            RawRecord {
                cpu_time: 77,
                read_keys: 88,
                write_keys: 99,
                network_in_bytes: 111,
                network_out_bytes: 222,
                logical_read_bytes: 333,
                logical_write_bytes: 444,
            },
        );
        // tag5 won't be picked
        records.insert(
            tag5,
            RawRecord {
                cpu_time: 66,
                read_keys: 55,
                write_keys: 44,
                network_in_bytes: 11,
                network_out_bytes: 22,
                logical_read_bytes: 33,
                logical_write_bytes: 44,
            },
        );
        let rs = RawRecords {
            begin_unix_time_secs: 1,
            duration: Duration::from_secs(1),
            records,
        };
        let agg_map = rs.aggregate_by_extra_tag();
        let (kth_cpu, kth_network, kth_logical_io) = find_kth_values(agg_map.iter(), 2);
        let (top, evicted) =
            get_iter_for_cpu_network_io(&agg_map, kth_cpu, kth_network, kth_logical_io);
        let others = evicted
            .map(|(_, v)| v)
            .fold(RawRecord::default(), |mut others, r| {
                others.merge(r);
                others
            });
        assert_eq!(top.count(), 3);
        assert_eq!(others.cpu_time, 77 + 66);
        assert_eq!(others.read_keys, 88 + 55);
        assert_eq!(others.write_keys, 99 + 44);
        assert_eq!(others.network_in_bytes, 111 + 11);
        assert_eq!(others.network_out_bytes, 222 + 22);
        assert_eq!(others.logical_read_bytes, 333 + 33);
        assert_eq!(others.logical_write_bytes, 444 + 44);
    }
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::recorder::CpuRecords;

use std::cell::Cell;
use std::sync::Arc;

use collections::HashMap;

// Resource tag
pub type Tag = Vec<u8>;

// Timestamps in second
pub type Timestamps = Vec<Timestamp>;

// Timestamp in second
pub type Timestamp = u64;

// CPU times in millisecond
pub type CpuTimes = Vec<CpuTime>;

// CPU time in millisecond
pub type CpuTime = u32;

// Sum of CPU times in millisecond
pub type CpuTimeSum = CpuTime;

// Record value is layout as ([timestamp_secs], [cpu_time_ms], total_cpu_time_ms)
pub type RecordValue = (Timestamps, CpuTimes, CpuTimeSum);

#[derive(Default)]
pub struct Records {
    pub records: HashMap<Tag, RecordValue>,
    pub others: HashMap<Timestamp, CpuTime>,
}

impl Records {
    pub fn append(&mut self, raw_records: Arc<CpuRecords>) {
        let timestamp_secs = raw_records.begin_unix_time_secs;

        for (tag, ms) in &raw_records.records {
            let ms = *ms as u32;
            let tag = &tag.infos.extra_attachment;
            if tag.is_empty() {
                continue;
            }

            let record_value = self.records.get_mut(tag);
            if record_value.is_none() {
                self.records
                    .insert(tag.clone(), (vec![timestamp_secs], vec![ms], ms));
                continue;
            }

            let (ts, cpu_time, total) = record_value.unwrap();
            *total += ms;
            if *ts.last().unwrap() == timestamp_secs {
                *cpu_time.last_mut().unwrap() += ms;
            } else {
                ts.push(timestamp_secs);
                cpu_time.push(ms);
            }
        }
    }

    pub fn keep_top_k(&mut self, k: usize) {
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

        for (_, _, total) in self.records.values() {
            buf.push(*total);
        }
        pdqselect::select_by(&mut buf, k, |a, b| b.cmp(a));
        let kth = buf[k];
        let others = &mut self.others;
        self.records
            .drain_filter(|_, (_, _, total)| *total <= kth)
            .for_each(|(_, (secs_list, cpu_time_list, _))| {
                secs_list
                    .into_iter()
                    .zip(cpu_time_list.into_iter())
                    .for_each(|(secs, cpu_time)| *others.entry(secs).or_insert(0) += cpu_time)
            });

        STATIC_BUF.with(move |b| b.set(buf));
    }

    pub fn is_empty(&self) -> bool {
        if self.records.is_empty() {
            assert!(self.others.is_empty());
            true
        } else {
            false
        }
    }

    pub fn clear(&mut self) {
        self.records.clear();
        self.others.clear();
    }
}

thread_local! {
    static STATIC_BUF: Cell<Vec<u32>> = Cell::new(vec![]);
}

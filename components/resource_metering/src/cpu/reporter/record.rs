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

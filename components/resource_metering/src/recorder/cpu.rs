// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::localstorage::LocalStorage;
use crate::recorder::SubRecorder;
use crate::utils;
use crate::utils::Stat;
use crate::{RawRecord, RawRecords, SharedTagPtr};

use collections::HashMap;
use fail::fail_point;
use lazy_static::lazy_static;

lazy_static! {
    static ref STAT_TASK_COUNT: prometheus::IntCounter = prometheus::register_int_counter!(
        "tikv_req_cpu_stat_task_count",
        "Counter of stat_task call"
    )
    .unwrap();
}

/// An implementation of [SubRecorder] for collecting cpu statistics.
///
/// `CpuRecorder` collects cpu usage at a fixed frequency.
///
/// See [SubRecorder] for more relevant designs.
///
/// [SubRecorder]: crate::recorder::SubRecorder
#[derive(Default)]
pub struct CpuRecorder {
    thread_stats: HashMap<usize, ThreadStat>,
}

impl SubRecorder for CpuRecorder {
    fn tick(&mut self, records: &mut RawRecords, _: &mut HashMap<usize, LocalStorage>) {
        let records = &mut records.records;
        self.thread_stats.iter_mut().for_each(|(tid, thread_stat)| {
            let cur_tag = thread_stat.shared_ptr.take_clone();
            fail_point!(
                "cpu-record-test-filter",
                cur_tag.as_ref().map_or(false, |t| !t
                    .infos
                    .extra_attachment
                    .starts_with(crate::TEST_TAG_PREFIX)),
                |_| {}
            );
            if let Some(cur_tag) = cur_tag {
                if let Ok(cur_stat) = utils::stat_task(utils::process_id(), *tid) {
                    STAT_TASK_COUNT.inc();
                    let last_stat = &thread_stat.stat;
                    let last_cpu_tick = last_stat.utime.wrapping_add(last_stat.stime);
                    let cur_cpu_tick = cur_stat.utime.wrapping_add(cur_stat.stime);
                    let delta_ticks = cur_cpu_tick.wrapping_sub(last_cpu_tick);
                    if delta_ticks > 0 {
                        let delta_ms = delta_ticks * 1_000 / utils::clock_tick();
                        let record = records.entry(cur_tag).or_insert_with(RawRecord::default);
                        record.cpu_time += delta_ms as u32;
                    }
                    thread_stat.stat = cur_stat;
                }
            }
        });
    }

    fn cleanup(&mut self) {
        const THREAD_STAT_LEN_THRESHOLD: usize = 500;

        if self.thread_stats.capacity() > THREAD_STAT_LEN_THRESHOLD
            && self.thread_stats.len() < THREAD_STAT_LEN_THRESHOLD / 2
        {
            self.thread_stats.shrink_to(THREAD_STAT_LEN_THRESHOLD);
        }
    }

    fn reset(&mut self) {
        for (thread_id, stat) in &mut self.thread_stats {
            stat.stat = utils::stat_task(utils::process_id(), *thread_id).unwrap_or_default();
        }
    }

    fn thread_created(&mut self, id: usize, shared_ptr: SharedTagPtr) {
        self.thread_stats.insert(
            id,
            ThreadStat {
                shared_ptr,
                stat: Stat::default(),
            },
        );
    }
}

struct ThreadStat {
    shared_ptr: SharedTagPtr,
    stat: Stat,
}

#[cfg(test)]
#[cfg(not(target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn test_record() {
        let mut recorder = CpuRecorder::default();
        let mut records = RawRecords::default();
        recorder.tick(&mut records, &mut HashMap::default());
        assert!(records.records.is_empty());
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use super::*;
    use crate::{utils, RawRecords, TagInfos};
    use std::sync::atomic::AtomicPtr;
    use std::sync::Arc;

    fn heavy_job() -> u64 {
        let m: u64 = rand::random();
        let n: u64 = rand::random();
        let m = m ^ n;
        let n = m.wrapping_mul(n);
        let m = m.wrapping_add(n);
        let n = m & n;
        let m = m | n;
        m.wrapping_sub(n)
    }

    #[test]
    fn test_record() {
        let info = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            extra_attachment: b"abc".to_vec(),
        });
        let shared_ptr = SharedTagPtr {
            ptr: Arc::new(AtomicPtr::new(Arc::into_raw(info) as _)),
        };
        let mut recorder = CpuRecorder::default();
        recorder.thread_created(utils::thread_id(), shared_ptr);
        let thread_id = utils::thread_id();
        let prev_stat = &recorder.thread_stats.get(&thread_id).unwrap().stat;
        let prev_cpu_ticks = prev_stat.utime.wrapping_add(prev_stat.stime);
        loop {
            let stat = utils::stat_task(utils::process_id(), thread_id).unwrap();
            let cpu_ticks = stat.utime.wrapping_add(stat.stime);
            let delta_ms = cpu_ticks.wrapping_sub(prev_cpu_ticks) * 1_000 / utils::clock_tick();
            if delta_ms != 0 {
                break;
            }
            heavy_job();
        }
        let mut records = RawRecords::default();
        recorder.tick(&mut records, &mut HashMap::default());
        assert!(!records.records.is_empty());
    }
}

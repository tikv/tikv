// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::localstorage::LocalStorage;
use crate::recorder::SubRecorder;
use crate::{RawRecord, RawRecords, SharedTagPtr};

use collections::HashMap;
use fail::fail_point;
use lazy_static::lazy_static;
use tikv_util::sys::thread::{self, Pid};

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
    thread_stats: HashMap<Pid, ThreadStat>,
}

impl SubRecorder for CpuRecorder {
    fn tick(&mut self, records: &mut RawRecords, _: &mut HashMap<Pid, LocalStorage>) {
        let records = &mut records.records;
        let pid = thread::process_id();
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
                if let Ok(cur_stat) = thread::thread_stat(pid, *tid) {
                    STAT_TASK_COUNT.inc();
                    let last_stat = &thread_stat.stat;
                    if *last_stat != cur_stat {
                        let delta_ms =
                            (cur_stat.total_cpu_time() - last_stat.total_cpu_time()) * 1_000.;
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
        let pid = thread::process_id();
        for (tid, stat) in &mut self.thread_stats {
            stat.stat = thread::thread_stat(pid, *tid).unwrap_or_default();
        }
    }

    fn thread_created(&mut self, id: Pid, shared_ptr: SharedTagPtr) {
        self.thread_stats.insert(
            id,
            ThreadStat {
                shared_ptr,
                stat: Default::default(),
            },
        );
    }
}

struct ThreadStat {
    shared_ptr: SharedTagPtr,
    stat: thread::ThreadStat,
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
    use crate::{RawRecords, TagInfos};
    use std::sync::atomic::AtomicPtr;
    use std::sync::Arc;
    use tikv_util::sys::thread;

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
        recorder.thread_created(thread::thread_id(), shared_ptr);
        let thread_id = thread::thread_id();
        let prev_stat = &recorder.thread_stats.get(&thread_id).unwrap().stat;
        let prev_cpu_ticks = prev_stat.u_time.wrapping_add(prev_stat.s_time);
        loop {
            let stat = thread::full_thread_stat(thread::process_id(), thread_id).unwrap();
            let cpu_ticks = stat.utime.wrapping_add(stat.stime);
            let delta_ms = cpu_ticks.wrapping_sub(prev_cpu_ticks) * 1_000 / thread::clock_tick();
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

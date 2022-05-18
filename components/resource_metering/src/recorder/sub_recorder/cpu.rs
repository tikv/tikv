// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use tikv_util::sys::thread::{self, Pid};

use crate::{
    metrics::STAT_TASK_COUNT,
    recorder::{
        localstorage::{LocalStorage, SharedTagInfos},
        SubRecorder,
    },
    RawRecord, RawRecords,
};

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
            let cur_tag = thread_stat.attached_tag.load_full();
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

    fn cleanup(
        &mut self,
        _records: &mut RawRecords,
        _thread_stores: &mut HashMap<Pid, LocalStorage>,
    ) {
        const THREAD_STAT_LEN_THRESHOLD: usize = 500;

        if self.thread_stats.capacity() > THREAD_STAT_LEN_THRESHOLD
            && self.thread_stats.len() < THREAD_STAT_LEN_THRESHOLD / 2
        {
            self.thread_stats.shrink_to(THREAD_STAT_LEN_THRESHOLD);
        }
    }

    fn resume(
        &mut self,
        _records: &mut RawRecords,
        _thread_stores: &mut HashMap<Pid, LocalStorage>,
    ) {
        let pid = thread::process_id();
        for (tid, stat) in &mut self.thread_stats {
            stat.stat = thread::thread_stat(pid, *tid).unwrap_or_default();
        }
    }

    fn thread_created(&mut self, id: Pid, store: &LocalStorage) {
        self.thread_stats.insert(
            id,
            ThreadStat {
                attached_tag: store.attached_tag.clone(),
                stat: Default::default(),
            },
        );
    }
}

struct ThreadStat {
    attached_tag: SharedTagInfos,
    stat: thread::ThreadStat,
}

#[cfg(test)]
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
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
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{RawRecords, TagInfos};

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
            key_ranges: vec![],
            extra_attachment: b"abc".to_vec(),
        });
        let store = LocalStorage {
            attached_tag: SharedTagInfos::new(info),
            ..Default::default()
        };
        let mut recorder = CpuRecorder::default();
        recorder.thread_created(thread::thread_id(), &store);
        let pid = thread::process_id();
        let tid = thread::thread_id();
        let prev_stat = &recorder.thread_stats.get(&tid).unwrap().stat;
        loop {
            let stat = thread::thread_stat(pid, tid).unwrap();
            let delta = stat.total_cpu_time() - prev_stat.total_cpu_time();
            if delta >= 0.001 {
                break;
            }
            heavy_job();
        }
        let mut records = RawRecords::default();
        recorder.tick(&mut records, &mut HashMap::default());
        assert!(!records.records.is_empty());
    }
}

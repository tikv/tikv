// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use tikv_util::{
    sys::thread::{self, Pid, THREAD_NAME_HASHMAP},
    thread_name_prefix::{
        SCHEDULE_WORKER_HIGH_PRI_THREAD, SCHEDULE_WORKER_POOL_THREAD,
        SCHEDULE_WORKER_PRIORITY_THREAD, UNIFIED_READ_POOL_THREAD, matches_thread_name_prefix,
    },
};

use crate::{
    RawRecords, ThreadPoolType,
    metrics::{
        SCHED_TAG_CPU_MILLIS_COUNTER, SCHED_TAG_SAMPLE_COUNTER, STAT_TASK_COUNT,
        UNIFIED_READ_TAG_CPU_MILLIS_COUNTER, UNIFIED_READ_TAG_SAMPLE_COUNTER,
    },
    recorder::{
        SubRecorder,
        localstorage::{LocalStorage, SharedTagInfos},
    },
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

/// Detect the thread pool type from the thread name stored in
/// [`THREAD_NAME_HASHMAP`].
fn detect_thread_pool_type(tid: Pid) -> ThreadPoolType {
    let map = THREAD_NAME_HASHMAP.lock().unwrap();
    match map.get(&tid) {
        Some(name) => {
            if matches_thread_name_prefix(name, UNIFIED_READ_POOL_THREAD) {
                ThreadPoolType::UnifiedRead
            } else if matches_thread_name_prefix(name, SCHEDULE_WORKER_POOL_THREAD)
                || matches_thread_name_prefix(name, SCHEDULE_WORKER_HIGH_PRI_THREAD)
                || matches_thread_name_prefix(name, SCHEDULE_WORKER_PRIORITY_THREAD)
            {
                ThreadPoolType::Scheduler
            } else {
                ThreadPoolType::Unknown
            }
        }
        None => ThreadPoolType::Unknown,
    }
}

impl SubRecorder for CpuRecorder {
    fn tick(&mut self, records: &mut RawRecords, _: &mut HashMap<Pid, LocalStorage>) {
        let pid = thread::process_id();
        self.thread_stats.iter_mut().for_each(|(tid, thread_stat)| {
            let cur_tag = thread_stat.attached_tag.load_full();
            let has_tag = cur_tag.is_some();
            let observe_scheduler = thread_stat.pool_type == ThreadPoolType::Scheduler;
            let observe_unified_read = matches!(
                thread_stat.pool_type,
                ThreadPoolType::UnifiedRead | ThreadPoolType::Coprocessor
            );
            let observe_pool = observe_scheduler || observe_unified_read;
            if !observe_pool && !has_tag {
                return;
            }

            if let Ok(cur_stat) = thread::thread_stat(pid, *tid) {
                STAT_TASK_COUNT.inc();

                if observe_pool {
                    let state = if cur_tag.is_some() {
                        "present"
                    } else {
                        "absent"
                    };
                    if observe_scheduler {
                        SCHED_TAG_SAMPLE_COUNTER.with_label_values(&[state]).inc();
                    } else {
                        UNIFIED_READ_TAG_SAMPLE_COUNTER
                            .with_label_values(&[state])
                            .inc();
                    }

                    let last_observe_stat = &thread_stat.observe_stat;
                    if *last_observe_stat != cur_stat {
                        let delta_ms = (cur_stat.total_cpu_time()
                            - last_observe_stat.total_cpu_time())
                            * 1_000.;
                        if delta_ms > 0.0 {
                            if observe_scheduler {
                                SCHED_TAG_CPU_MILLIS_COUNTER
                                    .with_label_values(&[state])
                                    .inc_by(delta_ms as u64);
                            } else {
                                UNIFIED_READ_TAG_CPU_MILLIS_COUNTER
                                    .with_label_values(&[state])
                                    .inc_by(delta_ms as u64);
                            }
                        }
                    }
                }

                let last_stat = &thread_stat.stat;
                if *last_stat != cur_stat {
                    let delta_ms =
                        (cur_stat.total_cpu_time() - last_stat.total_cpu_time()) * 1_000.;
                    if delta_ms > 0.0 {
                        if let Some(cur_tag) = cur_tag.as_ref() {
                            let record = records.records.entry(cur_tag.clone()).or_default();
                            record.add_cpu_time(delta_ms as u32, thread_stat.pool_type);
                        } else if observe_scheduler {
                            records
                                .scheduler_tag_absent_untracked
                                .add_cpu_time(delta_ms as u32, thread_stat.pool_type);
                        } else if observe_unified_read {
                            records
                                .unified_read_tag_absent_untracked
                                .add_cpu_time(delta_ms as u32, thread_stat.pool_type);
                        }
                    }
                }
                if observe_pool || has_tag {
                    thread_stat.stat = cur_stat;
                }
                thread_stat.observe_stat = cur_stat;
            }
        });
    }

    fn cleanup(
        &mut self,
        _records: &mut RawRecords,
        thread_stores: &mut HashMap<Pid, LocalStorage>,
    ) {
        // Remove thread stats that are no longer in thread_stores.
        self.thread_stats
            .retain(|tid, _| thread_stores.contains_key(tid));

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
            let cur_stat = thread::thread_stat(pid, *tid).unwrap_or_default();
            stat.stat = cur_stat;
            stat.observe_stat = cur_stat;
        }
    }

    fn thread_created(&mut self, id: Pid, store: &LocalStorage) {
        let pool_type = detect_thread_pool_type(id);
        let stat = thread::thread_stat(thread::process_id(), id).unwrap_or_default();
        self.thread_stats.insert(
            id,
            ThreadStat {
                attached_tag: store.attached_tag.clone(),
                stat,
                observe_stat: stat,
                pool_type,
            },
        );
    }
}

struct ThreadStat {
    attached_tag: SharedTagInfos,
    stat: thread::ThreadStat,
    observe_stat: thread::ThreadStat,
    pool_type: ThreadPoolType,
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
            extra_attachment: Arc::new(b"abc".to_vec()),
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

    #[test]
    fn test_scheduler_tick_advances_baseline_and_tracks_absent_cpu() {
        let tagged = Arc::new(TagInfos {
            store_id: 1,
            region_id: 2,
            peer_id: 3,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"abc".to_vec()),
        });
        let tid = thread::thread_id();
        let pid = thread::process_id();
        let stat = thread::thread_stat(pid, tid).unwrap_or_default();
        let attached_tag = SharedTagInfos::new(tagged.clone());
        let mut recorder = CpuRecorder::default();
        recorder.thread_stats.insert(
            tid,
            ThreadStat {
                attached_tag: attached_tag.clone(),
                stat,
                observe_stat: stat,
                pool_type: ThreadPoolType::Scheduler,
            },
        );

        let initial_stat = recorder.thread_stats.get(&tid).unwrap().stat;
        loop {
            let cur_stat = thread::thread_stat(pid, tid).unwrap();
            if cur_stat.total_cpu_time() - initial_stat.total_cpu_time() >= 0.001 {
                break;
            }
            heavy_job();
        }

        let mut records = RawRecords::default();
        recorder.tick(&mut records, &mut HashMap::default());
        let tagged_cpu_before_absent = records.records.get(&tagged).unwrap().scheduler_cpu_time;
        assert!(tagged_cpu_before_absent > 0);

        attached_tag.swap(None);
        let baseline_before_absent = recorder.thread_stats.get(&tid).unwrap().stat;
        loop {
            let cur_stat = thread::thread_stat(pid, tid).unwrap();
            if cur_stat.total_cpu_time() - baseline_before_absent.total_cpu_time() >= 0.001 {
                break;
            }
            heavy_job();
        }

        recorder.tick(&mut records, &mut HashMap::default());
        let baseline_after_absent = recorder.thread_stats.get(&tid).unwrap().stat;
        assert!(baseline_after_absent.total_cpu_time() > baseline_before_absent.total_cpu_time());
        assert!(records.scheduler_tag_absent_untracked.scheduler_cpu_time > 0);
        assert_eq!(
            records.records.get(&tagged).unwrap().scheduler_cpu_time,
            tagged_cpu_before_absent
        );
    }

    #[test]
    fn test_unified_read_tick_advances_baseline_and_tracks_absent_cpu() {
        let tagged = Arc::new(TagInfos {
            store_id: 1,
            region_id: 2,
            peer_id: 3,
            key_ranges: vec![],
            extra_attachment: Arc::new(b"abc".to_vec()),
        });
        let tid = thread::thread_id();
        let pid = thread::process_id();
        let stat = thread::thread_stat(pid, tid).unwrap_or_default();
        let attached_tag = SharedTagInfos::new(tagged.clone());
        let mut recorder = CpuRecorder::default();
        recorder.thread_stats.insert(
            tid,
            ThreadStat {
                attached_tag: attached_tag.clone(),
                stat,
                observe_stat: stat,
                pool_type: ThreadPoolType::UnifiedRead,
            },
        );

        let initial_stat = recorder.thread_stats.get(&tid).unwrap().stat;
        loop {
            let cur_stat = thread::thread_stat(pid, tid).unwrap();
            if cur_stat.total_cpu_time() - initial_stat.total_cpu_time() >= 0.001 {
                break;
            }
            heavy_job();
        }

        let mut records = RawRecords::default();
        recorder.tick(&mut records, &mut HashMap::default());
        let tagged_cpu_before_absent = records.records.get(&tagged).unwrap().unified_read_cpu_time;
        assert!(tagged_cpu_before_absent > 0);

        attached_tag.swap(None);
        let baseline_before_absent = recorder.thread_stats.get(&tid).unwrap().stat;
        loop {
            let cur_stat = thread::thread_stat(pid, tid).unwrap();
            if cur_stat.total_cpu_time() - baseline_before_absent.total_cpu_time() >= 0.001 {
                break;
            }
            heavy_job();
        }

        recorder.tick(&mut records, &mut HashMap::default());
        let baseline_after_absent = recorder.thread_stats.get(&tid).unwrap().stat;
        assert!(baseline_after_absent.total_cpu_time() > baseline_before_absent.total_cpu_time());
        assert!(
            records
                .unified_read_tag_absent_untracked
                .unified_read_cpu_time
                > 0
        );
        assert_eq!(
            records.records.get(&tagged).unwrap().unified_read_cpu_time,
            tagged_cpu_before_absent
        );
    }
}

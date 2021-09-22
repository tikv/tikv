// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collector::Collector;
use crate::cpu::collector::{
    DynCpuCollectorId, DynCpuCollectorReg, COLLECTOR_REGISTRATION_CHANNEL,
};
use crate::cpu::RawCpuRecords;
use crate::localstorage::LocalStorage;
use crate::recorder::SubRecorder;
use crate::utils;
use crate::{ResourceMeteringTag, SharedTagPtr};
use collections::HashMap;
use fail::fail_point;
use lazy_static::lazy_static;
use procinfo::pid::Stat;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use tikv_util::time::Instant;

const GC_INTERVAL_SECS: u64 = 15 * 60;
const THREAD_STAT_LEN_THRESHOLD: usize = 500;
const RECORD_LEN_THRESHOLD: usize = 20_000;
const TEST_TAG_PREFIX: &[u8] = b"__resource_metering::tests::";

lazy_static! {
    static ref PID: libc::pid_t = unsafe { libc::getpid() };
    static ref CLK_TCK: libc::c_long = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
}

lazy_static! {
    static ref STAT_TASK_COUNT: prometheus::IntCounter = prometheus::register_int_counter!(
        "tikv_req_cpu_stat_task_count",
        "Counter of stat_task call"
    )
    .unwrap();
}

/// An implementation of [SubRecorder] for collecting cpu statistics.
///
/// `CpuRecorder` collects cpu usage at a fixed frequency, and then send it to [Collector].
///
/// See [SubRecorder] for more relevant designs.
///
/// [SubRecorder]: crate::recorder::SubRecorder
/// [Collector]: crate::collector::Collector
pub struct CpuRecorder<C> {
    collector: C,
    dyn_collectors: HashMap<DynCpuCollectorId, Box<dyn Collector<Arc<RawCpuRecords>>>>,
    precision_ms: Arc<AtomicU64>,
    thread_stats: HashMap<usize, ThreadStat>,
    current_window_records: RawCpuRecords,
    last_collect_instant: Instant,
    last_gc_instant: Instant,
}

impl<C> SubRecorder for CpuRecorder<C>
where
    C: Collector<Arc<RawCpuRecords>>,
{
    fn tick(&mut self, _thread_stores: &mut HashMap<usize, LocalStorage>) {
        self.handle_dyn_collector_registration();
        self.record();
        self.may_gc();
        self.may_advance_window();
    }

    fn reset(&mut self) {
        let now = Instant::now();
        self.current_window_records = RawCpuRecords::default();
        for v in self.thread_stats.values_mut() {
            v.prev_tag = None;
        }
        self.last_collect_instant = now;
        self.last_gc_instant = now;
    }

    fn thread_created(&mut self, id: usize, shared_ptr: SharedTagPtr) {
        self.thread_stats.insert(
            id,
            ThreadStat {
                shared_ptr,
                prev_stat: Stat::default(),
                prev_tag: None,
            },
        );
    }
}

impl<C> CpuRecorder<C>
where
    C: Collector<Arc<RawCpuRecords>>,
{
    pub fn new(collector: C, precision_ms: Arc<AtomicU64>) -> Self {
        let now = Instant::now();
        Self {
            collector,
            precision_ms,
            dyn_collectors: HashMap::default(),
            thread_stats: HashMap::default(),
            current_window_records: RawCpuRecords::default(),
            last_collect_instant: now,
            last_gc_instant: now,
        }
    }

    fn record(&mut self) {
        let records = &mut self.current_window_records.records;
        self.thread_stats.iter_mut().for_each(|(tid, thread_stat)| {
            let cur_tag = thread_stat.shared_ptr.take_clone();
            let prev_tag = thread_stat.prev_tag.take();

            if cur_tag.is_some() || prev_tag.is_some() {
                STAT_TASK_COUNT.inc();

                // If existing current tag, need to store the beginning stat.
                // If existing previous tag, need to get the end stat to calculate delta.
                if let Ok(stat) = procinfo::pid::stat_task(*PID, *tid as _) {
                    // Accumulate the cpu time for the previous tag.
                    if let Some(prev_tag) = prev_tag {
                        let prev_cpu_ticks = (thread_stat.prev_stat.utime as u64)
                            .wrapping_add(thread_stat.prev_stat.stime as u64);
                        let current_cpu_ticks = (stat.utime as u64).wrapping_add(stat.stime as u64);
                        let delta_ms = current_cpu_ticks.wrapping_sub(prev_cpu_ticks) * 1_000
                            / (*CLK_TCK as u64);

                        if delta_ms != 0 {
                            *records.entry(prev_tag).or_insert(0) += delta_ms;
                        }
                    }

                    fail_point!(
                        "cpu-record-test-filter",
                        cur_tag.as_ref().map_or(false, |t| !t
                            .infos
                            .extra_attachment
                            .starts_with(TEST_TAG_PREFIX)),
                        |_| {}
                    );

                    // Store the beginning stat for the current tag.
                    if cur_tag.is_some() {
                        thread_stat.prev_tag = cur_tag;
                        thread_stat.prev_stat = stat;
                    }
                }
            }
        });
    }

    fn may_gc(&mut self) -> bool {
        let duration_secs = self.last_gc_instant.saturating_elapsed().as_secs();
        let need_gc = duration_secs >= GC_INTERVAL_SECS;
        if need_gc {
            if let Some(thread_ids) = utils::thread_ids() {
                self.thread_stats.retain(|k, v| {
                    let retain = thread_ids.contains(k);
                    assert!(retain || v.shared_ptr.take().is_none());
                    retain
                });
            }
            if self.thread_stats.capacity() > THREAD_STAT_LEN_THRESHOLD
                && self.thread_stats.len() < THREAD_STAT_LEN_THRESHOLD / 2
            {
                self.thread_stats.shrink_to(THREAD_STAT_LEN_THRESHOLD);
            }
            if self.current_window_records.records.capacity() > RECORD_LEN_THRESHOLD
                && self.current_window_records.records.len() < RECORD_LEN_THRESHOLD / 2
            {
                self.current_window_records
                    .records
                    .shrink_to(RECORD_LEN_THRESHOLD);
            }
        }
        need_gc
    }

    fn may_advance_window(&mut self) -> bool {
        let duration = self.last_collect_instant.saturating_elapsed();
        let need_advance = duration.as_millis() >= self.precision_ms.load(Relaxed) as _;
        if need_advance {
            let mut records = std::mem::take(&mut self.current_window_records);
            records.duration = duration;
            if !records.records.is_empty() {
                let r = Arc::new(records);
                self.collector.collect(r.clone());
                self.dyn_collectors
                    .values()
                    .for_each(|c| c.collect(r.clone()));
            }
            self.last_collect_instant = Instant::now();
        }
        need_advance
    }

    fn handle_dyn_collector_registration(&mut self) {
        while let Ok(msg) = COLLECTOR_REGISTRATION_CHANNEL.1.try_recv() {
            match msg {
                DynCpuCollectorReg::Register { id, collector } => {
                    self.dyn_collectors.insert(id, collector);
                }
                DynCpuCollectorReg::Unregister { id } => {
                    self.dyn_collectors.remove(&id);
                }
            }
        }
    }
}

struct ThreadStat {
    shared_ptr: SharedTagPtr,
    prev_tag: Option<ResourceMeteringTag>,
    prev_stat: Stat,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::Collector;
    use crate::cpu::recorder::CpuRecorder;
    use crate::cpu::RawCpuRecords;
    use crate::utils;
    use crate::TagInfos;
    use std::sync::atomic::{AtomicPtr, AtomicU64};
    use std::sync::Arc;

    struct MockCollector1;

    impl Collector<Arc<RawCpuRecords>> for MockCollector1 {
        fn collect(&self, _records: Arc<RawCpuRecords>) {}
    }

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
        let info = TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            extra_attachment: b"abc".to_vec(),
        };
        let shared_ptr = unsafe {
            SharedTagPtr {
                ptr: Arc::new(AtomicPtr::new(std::mem::transmute(&info))),
            }
        };
        let mut recorder = CpuRecorder::new(MockCollector1, Arc::new(AtomicU64::new(1000)));
        recorder.thread_stats.insert(
            utils::thread_id(),
            ThreadStat {
                shared_ptr,
                prev_stat: Stat::default(),
                prev_tag: None,
            },
        );
        assert_eq!(recorder.current_window_records.records.len(), 0);
        recorder.record();
        assert_eq!(recorder.current_window_records.records.len(), 0);
        let thread_id = utils::thread_id();
        let prev_stat = &recorder.thread_stats.get(&thread_id).unwrap().prev_stat;
        let prev_cpu_ticks = (prev_stat.utime as u64).wrapping_add(prev_stat.stime as u64);
        loop {
            let stat = procinfo::pid::stat_task(*PID, thread_id as _).unwrap();
            let cpu_ticks = (stat.utime as u64).wrapping_add(stat.stime as u64);
            let delta_ms = cpu_ticks.wrapping_sub(prev_cpu_ticks) * 1_000 / (*CLK_TCK as u64);
            if delta_ms != 0 {
                break;
            }
            heavy_job();
        }
        recorder.record();
        assert_eq!(recorder.current_window_records.records.len(), 1);
    }
}

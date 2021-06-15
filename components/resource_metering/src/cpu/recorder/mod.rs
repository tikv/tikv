// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod handle;
pub mod records;

pub const TEST_TAG_PREFIX: &[u8] = b"__resource_metering::tests::";

use crate::cpu::collector::{Collector, CollectorId};
use crate::cpu::collector::{CollectorRegistrationMsg, COLLECTOR_REGISTRATION_CHANNEL};
use crate::cpu::recorder::records::CpuRecords;
use crate::cpu::thread::{CpuTimeInstant, ThreadId, ThreadRegister};
use crate::tag::{ResourceMeteringTag, TagCell};

use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use collections::HashMap;
use fail::fail_point;

struct CpuRecorder {
    pause: Arc<AtomicBool>,
    precision_ms: Arc<AtomicU64>,

    thread_stats: HashMap<ThreadId, ThreadStat>,
    current_window_records: CpuRecords,

    last_collect_instant: Instant,
    last_shrink_instant: Instant,

    collectors: HashMap<CollectorId, Box<dyn Collector>>,
}

struct ThreadStat {
    tag_cell: TagCell,
    prev_tag: Option<ResourceMeteringTag>,
    prev_instant: CpuTimeInstant,
}

impl CpuRecorder {
    pub fn new(pause: Arc<AtomicBool>, precision_ms: Arc<AtomicU64>) -> Self {
        let now = Instant::now();

        Self {
            pause,
            precision_ms,

            last_collect_instant: now,
            last_shrink_instant: now,

            thread_stats: HashMap::default(),
            current_window_records: CpuRecords::default(),

            collectors: HashMap::default(),
        }
    }

    pub fn handle_pause(&mut self) {
        let mut should_reset = false;
        while self.pause.load(SeqCst) {
            thread::park();
            should_reset = true;
        }

        if should_reset {
            self.reset();
        }
    }

    pub fn handle_collector_registration(&mut self) {
        let mut should_reset = false;
        loop {
            while let Ok(msg) = COLLECTOR_REGISTRATION_CHANNEL.1.try_recv() {
                self.handle_collector_registration_msg(msg);
            }

            if self.collectors.is_empty() {
                // Block the record thread until a new collector coming.
                if let Ok(msg) = COLLECTOR_REGISTRATION_CHANNEL.1.recv() {
                    self.handle_collector_registration_msg(msg);
                }

                // May wait a long time. So drop out-dated state by resetting.
                should_reset = true;
            } else {
                break;
            }
        }

        if should_reset {
            self.reset();
        }
    }

    pub fn handle_thread_registration(&mut self) {
        ThreadRegister::consume_registrations(|thread_id, tag_cell| {
            self.thread_stats.insert(
                thread_id,
                ThreadStat {
                    tag_cell,
                    prev_instant: CpuTimeInstant::default(),
                    prev_tag: None,
                },
            );
        });
    }

    pub fn record(&mut self) {
        let current_window_records = &mut self.current_window_records.records;
        self.thread_stats.retain(|tid, thread_stat| {
            if !thread_stat.tag_cell.is_owner_alive() {
                return false;
            }

            let prev_tag = thread_stat.prev_tag.take();
            let cur_tag = thread_stat.tag_cell.take().map(|req_tag| {
                let tag = req_tag.clone();
                // Put it back as quickly as possible.
                assert!(thread_stat.tag_cell.swap(req_tag).is_none());
                tag
            });

            if cur_tag.is_some() || prev_tag.is_some() {
                // If existing current tag, need to store the beginning stat.
                // If existing previous tag, need to get the end stat to calculate delta.
                if let Some(instant) = CpuTimeInstant::now(tid) {
                    // Accumulate the cpu time for the previous tag.
                    if let Some(prev_tag) = prev_tag {
                        let delta_ms = instant.minus_ms(&thread_stat.prev_instant);
                        if delta_ms != 0 {
                            *current_window_records.entry(prev_tag).or_insert(0) += delta_ms;
                        }
                    }

                    fail_point!(
                        "cpu-record-test-filter",
                        cur_tag.as_ref().map_or(false, |t| !t
                            .infos
                            .extra_attachment
                            .starts_with(TEST_TAG_PREFIX)),
                        |_| true
                    );

                    // Store the beginning stat for the current tag.
                    if cur_tag.is_some() {
                        thread_stat.prev_tag = cur_tag;
                        thread_stat.prev_instant = instant;
                    }
                }
            }

            true
        });
    }

    pub fn may_shrink(&mut self) -> bool {
        const GC_INTERVAL_SECS: u64 = 15 * 60;
        const THREAD_STAT_LEN_THRESHOLD: usize = 500;
        const RECORD_LEN_THRESHOLD: usize = 20_000;

        let duration_secs = self.last_shrink_instant.elapsed().as_secs();
        let need_shrink = duration_secs >= GC_INTERVAL_SECS;

        if need_shrink {
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

        need_shrink
    }

    pub fn may_advance_window(&mut self) -> bool {
        let duration = self.last_collect_instant.elapsed();
        let need_advance = duration.as_millis() >= self.precision_ms.load(Relaxed) as _;

        if need_advance {
            let mut records = std::mem::take(&mut self.current_window_records);
            records.duration = duration;

            if !records.records.is_empty() {
                let records = Arc::new(records);
                for collector in self.collectors.values() {
                    collector.collect(records.clone());
                }
            }

            self.last_collect_instant = Instant::now();
        }

        need_advance
    }

    fn handle_collector_registration_msg(&mut self, msg: CollectorRegistrationMsg) {
        match msg {
            CollectorRegistrationMsg::Register { id, collector } => {
                self.collectors.insert(id, collector);
            }
            CollectorRegistrationMsg::Unregister { id } => {
                self.collectors.remove(&id);
            }
        }
    }

    fn reset(&mut self) {
        let now = Instant::now();
        self.current_window_records = CpuRecords::default();
        for v in self.thread_stats.values_mut() {
            v.prev_tag = None;
        }
        self.last_collect_instant = now;
        self.last_shrink_instant = now;
    }
}

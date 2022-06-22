// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::store::Config;

pub(crate) struct TickSchedule {
    run_at: u64,
    interval: u64,
}

impl TickSchedule {
    fn new(interval: u64) -> Self {
        Self {
            run_at: 0,
            interval,
        }
    }
}

pub(crate) struct Ticker {
    tick: u64,
    schedules: Vec<TickSchedule>,
}

impl Ticker {
    pub(crate) fn new(config: &Config) -> Self {
        let base_interval = config.raft_base_tick_interval.as_millis();
        let schedules = vec![
            TickSchedule::new(1),
            TickSchedule::new(config.split_region_check_tick_interval.as_millis() / base_interval),
            TickSchedule::new(config.pd_heartbeat_tick_interval.as_millis() / base_interval),
            TickSchedule::new(
                config.switch_mem_table_check_tick_interval.as_millis() / base_interval,
            ),
        ];
        Self { tick: 1, schedules }
    }

    pub(crate) fn new_store(config: &Config) -> Self {
        let base_interval = config.raft_base_tick_interval.as_millis();
        let schedules = vec![
            TickSchedule::new(config.pd_store_heartbeat_tick_interval.as_millis() / base_interval),
            TickSchedule::new(config.update_safe_ts_interval.as_millis() / base_interval),
            TickSchedule::new(config.local_file_gc_timeout.as_millis() / base_interval),
        ];
        Self { tick: 0, schedules }
    }

    pub(crate) fn tick_clock(&mut self) {
        self.tick += 1;
    }

    pub(crate) fn schedule(&mut self, tick: PeerTick) {
        let sched = &mut self.schedules[tick.idx];
        if sched.interval == 0 {
            sched.run_at = 0;
            return;
        }
        sched.run_at = self.tick + sched.interval;
    }

    pub(crate) fn is_on_tick(&self, tick: PeerTick) -> bool {
        let sched = &self.schedules[tick.idx];
        sched.run_at == self.tick
    }

    pub(crate) fn schedule_store(&mut self, tick: StoreTick) {
        let sched = &mut self.schedules[tick.idx];
        if sched.interval == 0 {
            sched.run_at = 0;
            return;
        }
        sched.run_at = self.tick + sched.interval;
    }

    pub(crate) fn is_on_store_tick(&self, tick: StoreTick) -> bool {
        let sched = &self.schedules[tick.idx];
        sched.run_at == self.tick
    }
}

#[derive(Copy, Clone)]
pub(crate) struct PeerTick {
    idx: usize,
}

pub(crate) const PEER_TICK_RAFT: PeerTick = PeerTick { idx: 0 };
pub(crate) const PEER_TICK_SPLIT_CHECK: PeerTick = PeerTick { idx: 1 };
pub(crate) const PEER_TICK_PD_HEARTBEAT: PeerTick = PeerTick { idx: 2 };
pub(crate) const PEER_TICK_SWITCH_MEM_TABLE_CHECK: PeerTick = PeerTick { idx: 3 };

#[derive(Eq, PartialEq)]
pub struct StoreTick {
    idx: usize,
}

pub(crate) const STORE_TICK_PD_HEARTBEAT: StoreTick = StoreTick { idx: 0 };
pub(crate) const STORE_TICK_UPDATE_SAFE_TS: StoreTick = StoreTick { idx: 1 };
pub(crate) const STORE_TICK_LOCAL_FILE_GC: StoreTick = StoreTick { idx: 2 };

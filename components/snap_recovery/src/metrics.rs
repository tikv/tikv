// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;
use prometheus_static_metric::*;

lazy_static! {
    pub static ref REGION_EVENT_COUNTER: RegionEvent = register_static_int_counter_vec!(
        RegionEvent,
        "tikv_snap_restore_region_event",
        "the total count of some events that each happened to one region. (But the counter counts all regions' events.)",
        &["event"]
    )
    .unwrap();

    // NOTE: should we handle the concurrent case by adding a tid parameter?
    pub static ref CURRENT_WAIT_APPLY_LEADER: IntGauge = register_int_gauge!(
        "tikv_current_waiting_leader_apply",
        "the current leader we are awaiting."
    ).unwrap();

    pub static ref CURRENT_WAIT_ELECTION_LEADER : IntGauge = register_int_gauge!(
        "tikv_current_waiting_leader_election",
        "the current leader we are awaiting."
    ).unwrap();

}

make_static_metric! {
    pub label_enum RegionEventType {
        collect_meta,
        promote_to_leader,
        keep_follower,
        start_wait_leader_apply,
        finish_wait_leader_apply,
    }

    pub struct RegionEvent : IntCounter {
        "event" => RegionEventType,
    }
}

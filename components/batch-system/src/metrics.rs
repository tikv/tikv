// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum FsmType {
        store,
        apply,
    }

    pub struct FsmRescheduleCounterVec: LocalIntCounter {
        "type" => FsmType,
    }

    pub struct FsmScheduleWaitDurationVec: LocalHistogram {
        "type" => FsmType,
    }

    pub struct FsmPollDurationVec: LocalHistogram {
        "type" => FsmType,
    }

    pub struct FsmPollRoundVec: LocalHistogram {
        "type" => FsmType,
    }

    pub struct FsmCountPerPollVec: LocalHistogram {
        "type" => FsmType,
    }

}

lazy_static! {
    pub static ref CHANNEL_FULL_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_channel_full_total",
        "Total number of channel full errors.",
        &["type"]
    )
    .unwrap();

    pub static ref FSM_RESCHEDULE_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_batch_system_fsm_reschedule_total",
        "Total number of fsm reschedule.",
        &["type"]
    )
    .unwrap();
    pub static ref FSM_RESCHEDULE_COUNTER: FsmRescheduleCounterVec =
        auto_flush_from!(FSM_RESCHEDULE_COUNTER_VEC, FsmRescheduleCounterVec);

    pub static ref FSM_SCHEDULE_WAIT_DURATION_VEC: HistogramVec =
    register_histogram_vec!(
        "tikv_batch_system_fsm_schedule_wait_seconds",
        "Duration of fsm waiting to be polled.",
        &["type"],
        exponential_buckets(0.001, 1.59, 20).unwrap(), // max 10s
    ).unwrap();
    pub static ref FSM_SCHEDULE_WAIT_DURATION: FsmScheduleWaitDurationVec =
    auto_flush_from!(FSM_SCHEDULE_WAIT_DURATION_VEC, FsmScheduleWaitDurationVec);

    pub static ref FSM_POLL_DURATION_VEC: HistogramVec =
    register_histogram_vec!(
        "tikv_batch_system_fsm_poll_seconds",
        "Total time for an FSM to finish processing all messages, potentially over multiple polling rounds.",
        &["type"],
        exponential_buckets(0.001, 1.59, 20).unwrap(), // max 10s
    ).unwrap();
    pub static ref FSM_POLL_DURATION: FsmPollDurationVec =
    auto_flush_from!(FSM_POLL_DURATION_VEC, FsmPollDurationVec);

    pub static ref FSM_POLL_ROUND_VEC: HistogramVec =
    register_histogram_vec!(
        "tikv_batch_system_fsm_poll_rounds",
        "Number of polling rounds for an FSM to finish processing all messages.",
        &["type"],
        exponential_buckets(1.0, 2.0, 20).unwrap(),
    ).unwrap();
    pub static ref FSM_POLL_ROUND: FsmPollRoundVec =
        auto_flush_from!(FSM_POLL_ROUND_VEC, FsmPollRoundVec);

    pub static ref FSM_COUNT_PER_POLL_VEC: HistogramVec =
    register_histogram_vec!(
        "tikv_batch_system_fsm_count_per_poll",
        "Number of fsm polled in one poll.",
        &["type"],
        exponential_buckets(1.0, 2.0, 20).unwrap(),
    ).unwrap();
    pub static ref FSM_COUNT_PER_POLL: FsmCountPerPollVec =
     auto_flush_from!(FSM_COUNT_PER_POLL_VEC, FsmCountPerPollVec);

    pub static ref BROADCAST_NORMAL_DURATION: Histogram =
    register_histogram!(
        "tikv_broadcast_normal_duration_seconds",
        "Duration of broadcasting normals.",
        exponential_buckets(0.001, 1.59, 20).unwrap() // max 10s
    ).unwrap();
}

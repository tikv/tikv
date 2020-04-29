use prometheus::IntCounterVec;

lazy_static! {
    pub static ref RESCHEDULE_FSM_COUNT: IntCounterVec = register_int_counter_vec!(
        "batch_system_reschedule_count",
        "The count of rescheduled fsms",
        &["tag"]
    )
    .unwrap();
}

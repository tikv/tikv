#[cfg(feature = "linearizability-track")]
pub use linearizability_track::*;

#[cfg(feature = "linearizability-track")]
mod linearizability_track {
    use tracker::{GLOBAL_TRACKERS, TrackerToken, get_tls_peer_state};
    use uuid::Uuid;

    pub fn log(token: TrackerToken) {
        GLOBAL_TRACKERS.with_tracker(token, |tracker| {
            if let Some(debug) = tracker.linearizability_track.as_ref() {
                match debug.scheduler_snapshot_states {
                    Some(ref states) => {
                        info!("Linearizability Track Write";
                            "apply_state" => ?debug.apply_state,
                            "ready_state" => ?debug.ready_state,
                            "propose_state" => ?debug.propose_state,
                            "scheduler_snapshot_seq_no" => ?debug.snapshot_seq_no,
                            "scheduler_snapshot_ready" => ?states.1,
                            "scheduler_snapshot_propose" => ?states.0,
                            "success" => debug.success,
                            "duration" => ?tracker.req_info.begin.elapsed(),
                            "req_type" => ?tracker.req_info.request_type,
                            "start_ts" => tracker.req_info.start_ts,
                        );
                    }
                    None => {
                        info!("Linearizability Track Read";
                            "snapshot_seq_no" => ?debug.snapshot_seq_no,
                            "ready_state" => ?debug.ready_state,
                            "propose_state" => ?debug.propose_state,
                            "success" => debug.success,
                            "duration" => ?tracker.req_info.begin.elapsed(),
                            "req_type" => ?tracker.req_info.request_type,
                            "start_ts" => tracker.req_info.start_ts,
                        );
                    }
                }
            }
        });
    }

    pub fn log_read_index(uuid: Uuid, start_ts: u64) {
        let peer_state = get_tls_peer_state();
        info!("Linearizability Track leader step read index";
            "peer_state" => ?peer_state,
            "uuid" => ?uuid,
            "start_ts" => start_ts,
        );
    }

    pub fn log_apply_entries(
        min_index: u64,
        max_index: u64,
        before_seq_no: u64,
        after_seq_no: u64,
    ) {
        let peer_state = get_tls_peer_state();
        info!("Linearizability Track apply entries";
            "after_seq_no" => after_seq_no,
            "before_seq_no" => before_seq_no,
            "to" => max_index,
            "from" => min_index,
            "peer_state" => ?peer_state,
        );
    }
}

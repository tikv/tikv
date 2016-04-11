use raftstore::store::{self, RaftStorage, SnapState};
use util::HandyRwLock;

use std::sync::Arc;
use std::fmt::{self, Formatter, Display};

use super::Runnable;

/// Snapshot generating task.
pub struct Task {
    storage: Arc<RaftStorage>,
}

impl Task {
    pub fn new(storage: Arc<RaftStorage>) -> Task {
        Task { storage: storage }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Snapshot Task for {}", self.storage.rl().get_region_id())
    }
}

pub struct Runner;

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        // do we need to check leader here?
        let db = task.storage.rl().get_engine();
        let snap;
        let region_id;
        let ranges;
        let applied_idx;
        let term;

        {
            let storage = task.storage.rl();
            snap = db.snapshot();
            region_id = storage.get_region_id();
            ranges = storage.region_key_ranges();
            match storage.load_applied_index(&snap) {
                Err(e) => {
                    error!("failed to load index for {}: {:?}", region_id, e);
                    return;
                }
                Ok(idx) => applied_idx = idx,
            }
            match storage.term(applied_idx) {
                Err(e) => {
                    error!("failed to get term for {}: {:}", region_id, e);
                    return;
                }
                Ok(t) => term = t,
            }
        }

        match store::do_snapshot(&snap, region_id, ranges, applied_idx, term) {
            Err(e) => error!("failed to generate snapshot for {}: {:?}", region_id, e),
            Ok(s) => task.storage.wl().snap_state = SnapState::Snap(s),
        }
    }
}

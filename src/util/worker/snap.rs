use raftstore::store::{self, RaftStorage, SnapState};
use util::HandyRwLock;

use std::sync::Arc;
use std::fmt::{self, Formatter, Display};
use std::error;

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

quick_error! {
    #[derive(Debug)]
    enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("snap failed {:?}", err)
        }
    }
}

pub struct Runner;

impl Runner {
    fn generate_snap(&self, task: &Task) -> Result<(), Error> {
        // do we need to check leader here?
        let db = task.storage.rl().get_engine();
        let raw_snap;
        let region_id;
        let ranges;
        let applied_idx;
        let term;

        {
            let storage = task.storage.rl();
            raw_snap = db.snapshot();
            region_id = storage.get_region_id();
            ranges = storage.region_key_ranges();
            applied_idx = box_try!(storage.load_applied_index(&raw_snap));
            term = box_try!(storage.term(applied_idx));
        }

        let snap = box_try!(store::do_snapshot(&raw_snap, region_id, ranges, applied_idx, term));
        task.storage.wl().snap_state = SnapState::Snap(snap);
        Ok(())
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        if let Err(e) = self.generate_snap(&task) {
            error!("failed to generate snap: {:?}!!!", e);
            task.storage.wl().snap_state = SnapState::Failed;
        }
    }
}

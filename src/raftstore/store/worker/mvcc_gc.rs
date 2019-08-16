use std::cmp::{max, min};
use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::mem;
use std::ops::Bound::{Excluded, Included};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::raftstore::store::keys::data_key;
use crate::storage::mvcc::{init_mvcc_gc_db, init_safe_point};
use engine::rocks::util::{compact_range, get_cf_handle};
use engine::rocks::DB;
use engine::CF_WRITE;
use tikv_util::time::{duration_to_ms, Instant};
use tikv_util::timer::Timer;
use tikv_util::worker::{Runnable, RunnableWithTimer};

#[derive(Clone, Debug)]
pub struct MvccGcTask {
    safe_point: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
}

impl Display for MvccGcTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MvccGc")
            .field("safe_point", &self.safe_point)
            .finish()
    }
}

pub struct MvccGcRunner {
    db: Arc<DB>,
    safe_point: Arc<AtomicU64>,
    pending_tasks: BTreeMap<Vec<u8>, (Vec<u8>, usize)>,
}

impl MvccGcRunner {
    pub fn new(db: Arc<DB>) -> MvccGcRunner {
        let safe_point = Arc::new(AtomicU64::new(0));
        init_safe_point(Arc::clone(&safe_point));
        MvccGcRunner {
            db,
            safe_point,
            pending_tasks: BTreeMap::new(),
        }
    }

    pub fn new_timer() -> Timer<()> {
        let mut timer = Timer::new(2);
        timer.add_task(COLLECT_TASK_INTERVAL, ());
        timer
    }

    fn compact(&self, start_key: &[u8], end_key: &[u8]) {
        init_mvcc_gc_db(Arc::clone(&self.db));
        let handle = get_cf_handle(&self.db, CF_WRITE).unwrap();
        compact_range(&self.db, handle, Some(start_key), Some(end_key), true, 1);
    }
}

impl Runnable<MvccGcTask> for MvccGcRunner {
    fn run(&mut self, task: MvccGcTask) {
        self.safe_point.store(task.safe_point, Ordering::Release);
        let mut start_key = data_key(&task.start_key);
        let mut end_key = data_key(&task.end_key);

        let mut removed: Vec<Vec<u8>> = self
            .pending_tasks
            .range::<Vec<_>, _>((Included(&start_key), Included(&end_key)))
            .map(|(k, _)| k.to_owned())
            .collect();
        if let Some((sk, (ek, _))) = self
            .pending_tasks
            .range::<Vec<_>, _>((Excluded(&vec![]), Included(&start_key)))
            .next_back()
        {
            if ek >= &start_key {
                removed.push(sk.to_owned());
            }
        }

        let mut merged_count = 0;
        for sk in removed {
            let (ek, mc) = self.pending_tasks.remove(&sk).unwrap();
            start_key = min(sk, start_key);
            end_key = max(ek, end_key);
            merged_count = max(merged_count, mc);
        }

        if merged_count + 1 >= CONTIGUOUS_REGION_GC_THRESHOLD {
            let start_time = Instant::now();
            self.compact(&start_key, &end_key);
            let elapsed = duration_to_ms(start_time.elapsed());
            info!(
                "gc worker handles {} regions in {} ms",
                merged_count + 1,
                elapsed
            );
        } else {
            self.pending_tasks
                .insert(start_key, (end_key, merged_count + 1));
        }
    }
}

impl RunnableWithTimer<MvccGcTask, ()> for MvccGcRunner {
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        let start_time = Instant::now();
        let mut merged_count = 0;
        let pending_tasks = mem::replace(&mut self.pending_tasks, Default::default());
        for (start_key, (end_key, count)) in pending_tasks {
            merged_count += count;
            self.compact(&start_key, &end_key);
        }
        let elapsed = duration_to_ms(start_time.elapsed());
        info!(
            "gc worker handles {} regions in {} ms",
            merged_count, elapsed
        );
    }
}

const COLLECT_TASK_INTERVAL: Duration = Duration::from_secs(120);
const CONTIGUOUS_REGION_GC_THRESHOLD: usize = 10;

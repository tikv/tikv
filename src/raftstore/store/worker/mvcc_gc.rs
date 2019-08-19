use std::cmp::{max, min};
use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::mem;
use std::ops::Bound::Included;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::raftstore::store::keys::{data_end_key, data_key};
use crate::storage::mvcc::{init_metrics, init_mvcc_gc_db, init_safe_point, GcMetrics};
use engine::rocks::util::{compact_range, get_cf_handle};
use engine::rocks::DB;
use engine::CF_WRITE;
use tikv_util::time::{duration_to_ms, Instant};
use tikv_util::timer::Timer;
use tikv_util::worker::{Runnable, RunnableWithTimer};

#[derive(Clone, Debug)]
pub struct MvccGcTask {
    pub safe_point: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
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
    tasks: BTreeMap<Vec<u8>, (Vec<u8>, usize)>,
}

impl MvccGcRunner {
    pub fn new(db: Arc<DB>) -> MvccGcRunner {
        let safe_point = Arc::new(AtomicU64::new(0));
        init_safe_point(Arc::clone(&safe_point));
        MvccGcRunner {
            db,
            safe_point,
            tasks: BTreeMap::new(),
        }
    }

    pub fn new_timer() -> Timer<()> {
        let mut timer = Timer::new(2);
        timer.add_task(COLLECT_TASK_INTERVAL, ());
        timer
    }

    fn compact(&self, start_key: &[u8], end_key: &[u8]) {
        let handle = get_cf_handle(&self.db, CF_WRITE).unwrap();
        compact_range(&self.db, handle, Some(start_key), Some(end_key), false, 2);
    }
}

impl Runnable<MvccGcTask> for MvccGcRunner {
    fn run(&mut self, task: MvccGcTask) {
        self.safe_point.store(task.safe_point, Ordering::Release);
        if let Some((sk, ek, mc)) = insert_range(&mut self.tasks, &task.start_key, &task.end_key) {
            init_mvcc_gc_db(Arc::clone(&self.db));
            let metrics = Arc::new(GcMetrics::default());
            init_metrics(Arc::clone(&metrics));

            let start_time = Instant::now();
            self.compact(&sk, &ek);
            let elapsed = duration_to_ms(start_time.elapsed());
            info!(
                "gc worker handles {} regions in {} ms, delete {} writes, {} defaults",
                mc,
                elapsed,
                metrics.write_versions.load(Ordering::Relaxed),
                metrics.default_versions.load(Ordering::Relaxed),
            );
        }
    }
}

impl RunnableWithTimer<MvccGcTask, ()> for MvccGcRunner {
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        init_mvcc_gc_db(Arc::clone(&self.db));
        let metrics = Arc::new(GcMetrics::default());
        init_metrics(Arc::clone(&metrics));

        let start_time = Instant::now();
        let mut merged_count = 0;
        let tasks = mem::replace(&mut self.tasks, Default::default());
        for (start_key, (end_key, count)) in tasks {
            merged_count += count;
            self.compact(&start_key, &end_key);
        }
        let elapsed = duration_to_ms(start_time.elapsed());
        info!(
            "on timeout, gc worker handles {} regions in {} ms, delete {} writes, {} defaults",
            merged_count,
            elapsed,
            metrics.write_versions.load(Ordering::Relaxed),
            metrics.default_versions.load(Ordering::Relaxed),
        );
        timer.add_task(COLLECT_TASK_INTERVAL, ());
    }
}

const COLLECT_TASK_INTERVAL: Duration = Duration::from_secs(60);
const CONTIGUOUS_REGION_GC_THRESHOLD: usize = 16;

fn insert_range(
    map: &mut BTreeMap<Vec<u8>, (Vec<u8>, usize)>,
    start_key: &[u8],
    end_key: &[u8],
) -> Option<(Vec<u8>, Vec<u8>, usize)> {
    let mut start_key = data_key(start_key);
    let mut end_key = data_end_key(end_key);

    let mut removed: Vec<Vec<u8>> = map
        .range::<Vec<_>, _>((Included(&start_key), Included(&end_key)))
        .map(|(k, _)| k.to_owned())
        .collect();

    if let Some((sk, (ek, _))) = map
        .range::<Vec<_>, _>((Included(&vec![]), Included(&start_key)))
        .next_back()
    {
        if ek >= &start_key {
            removed.push(sk.to_owned());
        }
    }

    let mut merged_count = 1;
    for sk in removed {
        if let Some((ek, mc)) = map.remove(&sk) {
            start_key = min(sk, start_key);
            end_key = max(ek, end_key);
            merged_count += mc;
        }
    }
    if merged_count > CONTIGUOUS_REGION_GC_THRESHOLD {
        return Some((start_key.clone(), end_key.clone(), merged_count));
    }
    map.insert(start_key, (end_key, merged_count));
    None
}

#[test]
fn test_insert_range() {
    let mut tasks = BTreeMap::default();
    insert_range(&mut tasks, b"", b"001");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks.get(b"z".as_ref()).unwrap().1, 1);

    insert_range(&mut tasks, b"002", b"003");
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks.get(b"z002".as_ref()).unwrap().1, 1);

    insert_range(&mut tasks, b"001", b"002");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks.get(b"z".as_ref()).unwrap().1, 3);
    assert_eq!(tasks.get(b"z".as_ref()).unwrap().0, b"z003".as_ref());

    insert_range(&mut tasks, b"002", b"004");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks.get(b"z".as_ref()).unwrap().1, 4);
    assert_eq!(tasks.get(b"z".as_ref()).unwrap().0, b"z004".as_ref());

    tasks.clear();
    insert_range(&mut tasks, b"008", b"010");
    insert_range(&mut tasks, b"007", b"009");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks.get(b"z007".as_ref()).unwrap().1, 2);
    assert_eq!(tasks.get(b"z007".as_ref()).unwrap().0, b"z010".as_ref());

    let max_key = data_end_key(&[]);

    insert_range(&mut tasks, b"008", b"");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks.get(b"z007".as_ref()).unwrap().1, 3);
    assert_eq!(tasks.get(b"z007".as_ref()).unwrap().0, max_key);

    insert_range(&mut tasks, b"", b"");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks.get(b"z".as_ref()).unwrap().1, 4);
    assert_eq!(tasks.get(b"z".as_ref()).unwrap().0, max_key);
}

use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display};
use std::ops::Bound::Included;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::raftstore::coprocessor::properties::MvccProperties;
use crate::raftstore::store::keys::{data_end_key, data_key};
use crate::storage::mvcc::{init_mvcc_gc_db, init_safe_point};
use engine::rocks::util::get_cf_handle;
use engine::rocks::{CompactOptions, DBBottommostLevelCompaction, DB};
use engine::util::get_range_properties_cf;
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
    running_tasks: HashMap<Vec<u8>, Vec<u8>>,
    sender: SyncSender<(Vec<u8>, Vec<u8>)>,
    receiver: Receiver<(Vec<u8>, Vec<u8>)>,
}

impl MvccGcRunner {
    pub fn new(db: Arc<DB>) -> MvccGcRunner {
        init_mvcc_gc_db(Arc::clone(&db));
        let safe_point = Arc::new(AtomicU64::new(0));
        init_safe_point(Arc::clone(&safe_point));

        let (tx, rx) = mpsc::sync_channel(CONCURRENCY);
        MvccGcRunner {
            db,
            safe_point,
            tasks: BTreeMap::new(),
            running_tasks: HashMap::new(),
            sender: tx,
            receiver: rx,
        }
    }

    pub fn new_timer() -> Timer<()> {
        let mut timer = Timer::new(2);
        timer.add_task(COLLECT_TASK_INTERVAL, ());
        timer
    }
}

impl Runnable<MvccGcTask> for MvccGcRunner {
    fn run(&mut self, task: MvccGcTask) {
        self.safe_point.store(task.safe_point, Ordering::Release);
        for (sk, ek) in &self.running_tasks {
            let dsk = data_key(&task.start_key);
            let dek = data_end_key(&task.end_key);
            if sk < &dek && &dsk < ek {
                // overlap with a range in compaction.
                info!("gc task overlap, skip");
                return;
            }
        }

        if let Some((sk, ek, mc)) = insert_range(&mut self.tasks, &task.start_key, &task.end_key) {
            if self.running_tasks.len() >= CONCURRENCY {
                let (recv_sk, _) = self.receiver.recv().unwrap();
                self.running_tasks.remove(&recv_sk).unwrap();
            }

            // Check properties.
            let collection = get_range_properties_cf(&self.db, CF_WRITE, &sk, &ek).unwrap();
            if collection.is_empty() {
                info!("empty properties for the given range, skip");
                return;
            }
            let mut props = MvccProperties::new();
            for (_, v) in &*collection {
                let mvcc = MvccProperties::decode(v.user_collected_properties()).unwrap();
                props.add(&mvcc);
            }
            if props.min_ts > task.safe_point {
                info!(
                    "min ts: {}, safe_point: {}, skip",
                    props.min_ts, task.safe_point
                );
                return;
            }
            if props.num_versions as f64 <= props.num_rows as f64 * 1.1
                && props.num_versions as f64 <= props.num_puts as f64 * 1.1
            {
                info!(
                    "num_versions: {}, num_rows: {}, num_puts: {}, skip",
                    props.num_versions, props.num_rows, props.num_puts,
                );
                return;
            }

            self.running_tasks.insert(sk.clone(), ek.clone());
            let sender = self.sender.clone();
            let db = Arc::clone(&self.db);

            thread::spawn(move || {
                let start_time = Instant::now();
                let handle = get_cf_handle(&db, CF_WRITE).unwrap();
                let mut compact_opts = CompactOptions::new();
                compact_opts.set_exclusive_manual_compaction(false);
                compact_opts.set_max_subcompactions(1);
                compact_opts.set_change_level(true);
                compact_opts.set_target_level(6);
                // Skip memtable and level 0.
                compact_opts.set_first_level(1);
                // Compaction from L5 to L6 has already cleaned all delete flags.
                compact_opts.set_bottommost_level_compaction(DBBottommostLevelCompaction::Skip);
                db.compact_range_cf_opt(handle, &compact_opts, Some(&sk), Some(&ek));

                let elapsed = duration_to_ms(start_time.elapsed());
                info!("gc worker handles {} regions in {} ms", mc, elapsed,);
                sender.send((sk, ek)).unwrap();
            });
            // Avoid 2 compaction filter uses 1 metrics.
            thread::sleep(Duration::from_secs(1));
        }
    }
}

impl RunnableWithTimer<MvccGcTask, ()> for MvccGcRunner {
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        let mut total_pending = 0;
        for (_, count) in self.tasks.values() {
            total_pending += count;
        }
        info!("{} pending GC tasks in mvcc_gc worker", total_pending);
        timer.add_task(COLLECT_TASK_INTERVAL, ());
    }
}

const COLLECT_TASK_INTERVAL: Duration = Duration::from_secs(60);
const CONTIGUOUS_REGION_GC_THRESHOLD: usize = 16;
const CONCURRENCY: usize = 1;

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

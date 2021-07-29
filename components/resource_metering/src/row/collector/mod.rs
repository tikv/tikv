use crate::row::recorder::RowRecords;
use collections::HashMap;
use lazy_static::lazy_static;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};

pub trait RowCollector: Send {
    fn collect_row(&self, records: Arc<RowRecords>);
}

#[derive(Copy, Clone, Default, Debug, Eq, PartialEq, Hash)]
pub struct CollectorId(pub(crate) u64);

lazy_static! {
    pub static ref GLOBAL_COLLOCTERS: Mutex<HashMap<CollectorId, Box<dyn RowCollector>>> =
        Mutex::new(HashMap::default());
}

pub fn register_row_collector(collector: Box<dyn RowCollector>) -> RowCollectorHandle {
    static NEXT_COLLECTOR_ID: AtomicU64 = AtomicU64::new(1);
    let id = CollectorId(NEXT_COLLECTOR_ID.fetch_add(1, Relaxed));
    let mut guard = GLOBAL_COLLOCTERS.lock().unwrap();
    guard.insert(id, collector);
    RowCollectorHandle { id }
}

pub struct RowCollectorHandle {
    id: CollectorId,
}
impl Drop for RowCollectorHandle {
    fn drop(&mut self) {
        let mut guard = GLOBAL_COLLOCTERS.lock().unwrap();
        guard.remove(&self.id);
    }
}

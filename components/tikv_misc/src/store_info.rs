use engine::DB;
use std::sync::Arc;

pub struct StoreInfo {
    pub engine: Arc<DB>,
    pub capacity: u64,
}


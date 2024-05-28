use std::sync::Arc;

use external_storage::{ExternalStorage, WalkBlobStorage};
use txn_types::TimeStamp;

trait CompactStorage: WalkBlobStorage + ExternalStorage {}

impl<T: WalkBlobStorage + ExternalStorage> CompactStorage for T {}

struct MetaStorage {
    files: Vec<MetaFile>,
    from_ts: TimeStamp,
    to_ts: TimeStamp,
}

struct MetaFile {
    log_files: Vec<LogFile>,
}

struct LogFile {
    name: Arc<str>,
    offset: u64,
    length: u64,
}

impl MetaStorage {
    async fn load_from(s: &dyn CompactStorage) {}
}

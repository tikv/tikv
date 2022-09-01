// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{atomic::Ordering, Arc, Mutex};

use parking_lot_core::SpinWait;
use rocksdb::{EventListener, FlushJobInfo, MemTableInfo};
use tikv_util::{
    debug,
    sequence_number::{Notifier, SYNCED_MAX_SEQUENCE_NUMBER, VERSION_COUNTER_ALLOCATOR},
};

use crate::RocksEngine;

#[derive(Clone)]
pub struct FlushListener {
    notifier: Arc<Mutex<Box<dyn Notifier>>>,
    engine: Arc<Mutex<Option<RocksEngine>>>,
}

impl FlushListener {
    pub fn new<N: Notifier + 'static>(notifier: N) -> Self {
        FlushListener {
            notifier: Arc::new(Mutex::new(Box::new(notifier))),
            engine: Arc::default(),
        }
    }

    pub fn set_engine(&self, engine: RocksEngine) {
        let mut e = self.engine.lock().unwrap();
        *e = Some(engine);
    }

    pub fn update_notifier(&self, notifier: impl Notifier + 'static) {
        let mut n = self.notifier.lock().unwrap();
        *n = Box::new(notifier);
    }
}

impl EventListener for FlushListener {
    fn on_flush_begin(&self, info: &FlushJobInfo) {
        let flush_seqno = info.largest_seqno();
        let mut spin_wait = SpinWait::new();
        loop {
            let max_seqno = SYNCED_MAX_SEQUENCE_NUMBER.load(Ordering::SeqCst);
            if max_seqno >= flush_seqno {
                break;
            }
            spin_wait.spin_no_yield();
        }
        debug!("flush begin"; "seqno" => flush_seqno);
    }

    fn on_flush_completed(&self, info: &FlushJobInfo) {
        let largest_seqno = info.largest_seqno();
        let cf = info.cf_name();
        // notify raftlog GC worker to GC relations and raft logs
        let notifier = self.notifier.lock().unwrap();
        notifier.notify_memtable_flushed(cf, largest_seqno);
        debug!("flush completed"; "seqno" => largest_seqno);
    }

    fn on_memtable_sealed(&self, info: &MemTableInfo) {
        let version = VERSION_COUNTER_ALLOCATOR.fetch_add(1, Ordering::SeqCst);
        debug!(
            "memtable sealed";
            "cf" => info.cf_name(),
            "first_seqno" => info.first_seqno(),
            "earliest_seqno" => info.earliest_seqno(),
            "version" => version+1
        );
        let seqno = {
            let engine = self.engine.lock().unwrap();
            engine
                .as_ref()
                .unwrap()
                .as_inner()
                .get_latest_sequence_number()
        };
        let notifier = self.notifier.lock().unwrap();
        notifier.notify_memtable_sealed(seqno);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use engine_traits::{MiscExt, Mutable, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT};
    use rocksdb::{ColumnFamilyOptions, DBOptions as RawDBOptions};
    use tikv_util::sequence_number::{Notifier, SYNCED_MAX_SEQUENCE_NUMBER};

    use crate::{util::new_engine_opt, FlushListener, RocksCfOptions, RocksDbOptions};

    #[derive(Clone)]
    struct TestNotifier;

    impl Notifier for TestNotifier {
        fn notify_memtable_sealed(&self, _seqno: u64) {}
        fn notify_memtable_flushed(&self, _cf: &str, _seqno: u64) {}
    }

    #[test]
    fn test_flush_listener() {
        let dir = tempfile::Builder::new()
            .prefix("tikv-engine-tests")
            .tempdir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        let mut db_opts = RawDBOptions::new();
        let listener = FlushListener::new(TestNotifier);
        db_opts.add_event_listener(listener.clone());
        let cf_opts = ColumnFamilyOptions::new();
        let engine = new_engine_opt(
            path,
            RocksDbOptions::from_raw(db_opts),
            vec![(CF_DEFAULT, RocksCfOptions::from_raw(cf_opts))],
        )
        .unwrap();
        listener.set_engine(engine.clone());
        let mut batch = engine.write_batch();
        batch.put_cf(CF_DEFAULT, b"k", b"v").unwrap();
        let mut write_opts = WriteOptions::new();
        write_opts.set_disable_wal(true);
        batch.write_opt(&write_opts).unwrap();
        batch.write_opt(&write_opts).unwrap();
        let seq = batch.write_opt(&write_opts).unwrap();
        SYNCED_MAX_SEQUENCE_NUMBER.store(seq, Ordering::SeqCst);
        engine.flush_cfs(true).unwrap();
        let seq = batch.write_opt(&write_opts).unwrap();
        SYNCED_MAX_SEQUENCE_NUMBER.store(seq, Ordering::SeqCst);
        engine.flush_cfs(true).unwrap();
    }
}

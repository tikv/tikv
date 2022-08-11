// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    iter::FromIterator,
    lazy::SyncOnceCell,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use collections::HashMap;
use parking_lot_core::SpinWait;
use rocksdb::{EventListener, FlushJobInfo, MemTableInfo};
use tikv_util::{
    debug,
    sequence_number::{Notifier, SYNCED_MAX_SEQUENCE_NUMBER, VERSION_COUNTER_ALLOCATOR},
};

use crate::RocksEngine;

#[derive(Clone)]
pub struct FlushListener {
    notifier: Arc<Mutex<dyn Notifier>>,
    engine: Arc<Mutex<Option<RocksEngine>>>,
    flushed_seqnos: Arc<SyncOnceCell<HashMap<String, AtomicU64>>>,
}

impl FlushListener {
    pub fn new<N: Notifier + 'static>(notifier: N) -> Self {
        FlushListener {
            notifier: Arc::new(Mutex::new(notifier)),
            engine: Arc::default(),
            flushed_seqnos: Arc::new(SyncOnceCell::new()),
        }
    }

    pub fn set_engine(&self, engine: RocksEngine) {
        let db = engine.as_inner();
        let cf_names = db.cf_names();
        self.flushed_seqnos
            .set(HashMap::from_iter(
                cf_names
                    .into_iter()
                    .map(|name| (name.to_string(), AtomicU64::new(0))),
            ))
            .unwrap();
        let mut e = self.engine.lock().unwrap();
        *e = Some(engine);
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
        let flushed_seqnos = self.flushed_seqnos.get().unwrap();
        let cf_flushed_seqno = flushed_seqnos.get(cf).unwrap();
        let mut current = cf_flushed_seqno.load(Ordering::SeqCst);
        while current < largest_seqno {
            if let Err(cur) = cf_flushed_seqno.compare_exchange_weak(
                current,
                largest_seqno,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                current = cur;
            }
        }
        let min_flushed_seqno = flushed_seqnos
            .iter()
            .map(|(_, seqno)| seqno.load(Ordering::SeqCst))
            .min()
            .unwrap();
        if min_flushed_seqno == largest_seqno {
            // notify raftlog GC worker to GC relations and raft logs
            let notifier = self.notifier.lock().unwrap();
            notifier.notify_memtable_flushed(largest_seqno);
        }
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

    use engine_traits::{
        ColumnFamilyOptions, MiscExt, Mutable, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT,
    };
    use rocksdb::DBOptions as RawDBOptions;
    use tikv_util::sequence_number::{Notifier, SYNCED_MAX_SEQUENCE_NUMBER};

    use crate::{
        util::{new_engine_opt, RocksCFOptions},
        FlushListener, RocksColumnFamilyOptions, RocksDBOptions,
    };

    #[derive(Clone)]
    struct TestNotifier;

    impl Notifier for TestNotifier {
        fn notify_memtable_sealed(&self, _seqno: u64) {}
        fn notify_memtable_flushed(&self, _seqno: u64) {}
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
        let cf_opts = RocksColumnFamilyOptions::new();
        let engine = new_engine_opt(
            path,
            RocksDBOptions::from_raw(db_opts),
            vec![RocksCFOptions::new(CF_DEFAULT, cf_opts)],
        )
        .unwrap();
        listener.set_engine(engine.clone());
        let flushed_seqnos = listener.flushed_seqnos.get().unwrap();
        let mut batch = engine.write_batch();
        batch.put_cf(CF_DEFAULT, b"k", b"v").unwrap();
        let mut write_opts = WriteOptions::new();
        write_opts.set_disable_wal(true);
        batch.write_opt(&write_opts).unwrap();
        batch.write_opt(&write_opts).unwrap();
        let seq = batch.write_opt(&write_opts).unwrap();
        SYNCED_MAX_SEQUENCE_NUMBER.store(seq, Ordering::SeqCst);
        engine.flush(true).unwrap();
        let flushed_max_seqno = flushed_seqnos.get(CF_DEFAULT).unwrap();
        assert_eq!(flushed_max_seqno.load(Ordering::SeqCst), 3);
        let seq = batch.write_opt(&write_opts).unwrap();
        SYNCED_MAX_SEQUENCE_NUMBER.store(seq, Ordering::SeqCst);
        engine.flush(true).unwrap();
        assert_eq!(flushed_max_seqno.load(Ordering::SeqCst), 4);
    }
}

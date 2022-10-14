// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use engine_traits::util::{MemtableEventNotifier, SequenceNumberProgress};
use rocksdb::{EventListener, FlushJobInfo, MemTableInfo};
use tikv_util::debug;

use crate::RocksEngine;

#[derive(Clone)]
pub struct FlushListener {
    notifier: Arc<Mutex<Box<dyn MemtableEventNotifier>>>,
    engine: Arc<Mutex<Option<RocksEngine>>>,
    seqno_progress: Arc<SequenceNumberProgress>,
}

impl FlushListener {
    pub fn new<N: MemtableEventNotifier + 'static>(
        notifier: N,
        seqno_progress: Arc<SequenceNumberProgress>,
    ) -> Self {
        FlushListener {
            notifier: Arc::new(Mutex::new(Box::new(notifier))),
            engine: Arc::default(),
            seqno_progress,
        }
    }

    pub fn set_engine(&self, engine: RocksEngine) {
        let mut e = self.engine.lock().unwrap();
        *e = Some(engine);
    }

    pub fn update_notifier(&self, notifier: impl MemtableEventNotifier + 'static) {
        let mut n = self.notifier.lock().unwrap();
        *n = Box::new(notifier);
    }

    pub fn notify_flush_cfs(&self, seqno: u64) {
        let n = self.notifier.lock().unwrap();
        n.notify_flush_cfs(seqno);
    }

    pub fn get_seqno_progress(&self) -> Arc<SequenceNumberProgress> {
        self.seqno_progress.clone()
    }
}

impl EventListener for FlushListener {
    fn on_flush_completed(&self, info: &FlushJobInfo) {
        let largest_seqno = info.largest_seqno();
        let cf = info.cf_name();
        // notify workers to GC relations and raft logs
        let notifier = self.notifier.lock().unwrap();
        notifier.notify_memtable_flushed(cf, largest_seqno);
        debug!("flush completed"; "seqno" => largest_seqno);
    }

    fn on_memtable_sealed(&self, info: &MemTableInfo) {
        let version = self.seqno_progress.fetch_add_memtable_version();
        debug!(
            "memtable sealed";
            "cf" => info.cf_name(),
            "first_seqno" => info.first_seqno(),
            "earliest_seqno" => info.earliest_seqno(),
            "version" => version,
        );
        let notifier = self.notifier.lock().unwrap();
        notifier.notify_memtable_sealed(info.largest_seqno());
    }
}

impl Debug for FlushListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlushListener").finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            mpsc::{self, SyncSender},
            Arc,
        },
        time::Duration,
    };

    use engine_traits::{
        util::{MemtableEventNotifier, SequenceNumberProgress},
        MiscExt, Mutable, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT,
    };
    use rocksdb::{ColumnFamilyOptions, DBOptions as RawDBOptions};

    use crate::{util::new_engine_opt, FlushListener, RocksCfOptions, RocksDbOptions};

    #[derive(Debug, PartialEq)]
    enum FlushEvent {
        MemtableSealed(u64),
        FlushCompleted(String, u64),
    }

    #[derive(Clone)]
    struct TestNotifier(SyncSender<FlushEvent>);

    impl MemtableEventNotifier for TestNotifier {
        fn notify_memtable_sealed(&self, seqno: u64) {
            let _ = self.0.send(FlushEvent::MemtableSealed(seqno));
        }
        fn notify_memtable_flushed(&self, cf: &str, seqno: u64) {
            let _ = self
                .0
                .send(FlushEvent::FlushCompleted(cf.to_string(), seqno));
        }
        fn notify_flush_cfs(&self, _seqno: u64) {}
    }

    #[test]
    fn test_flush_listener() {
        let dir = tempfile::Builder::new()
            .prefix("tikv-engine-tests")
            .tempdir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        let mut db_opts = RawDBOptions::new();
        let seqno_progress = Arc::new(SequenceNumberProgress::default());
        let (tx, rx) = mpsc::sync_channel(2);
        let listener = FlushListener::new(TestNotifier(tx), seqno_progress);
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
        let seqno = batch.write_opt(&write_opts).unwrap();
        engine.flush_cfs(true).unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)),
            Ok(FlushEvent::MemtableSealed(seqno))
        );
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)),
            Ok(FlushEvent::FlushCompleted(CF_DEFAULT.to_string(), seqno))
        );
    }
}

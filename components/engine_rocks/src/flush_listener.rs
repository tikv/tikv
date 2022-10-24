// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use engine_traits::util::MemtableEventNotifier;
use rocksdb::{EventListener, FlushJobInfo};
use tikv_util::debug;

#[derive(Clone)]
pub struct FlushListener {
    notifier: Arc<Mutex<Box<dyn MemtableEventNotifier>>>,
}

impl FlushListener {
    pub fn new<N: MemtableEventNotifier + 'static>(notifier: N) -> Self {
        FlushListener {
            notifier: Arc::new(Mutex::new(Box::new(notifier))),
        }
    }

    pub fn update_notifier(&self, notifier: impl MemtableEventNotifier + 'static) {
        let mut n = self.notifier.lock().unwrap();
        *n = Box::new(notifier);
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
}

impl Debug for FlushListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlushListener").finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc::{self, SyncSender},
        time::Duration,
    };

    use engine_traits::{
        util::MemtableEventNotifier, MiscExt, Mutable, WriteBatch, WriteBatchExt, WriteOptions,
        CF_DEFAULT,
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
        fn notify_memtable_flushed(&self, cf: &str, seqno: u64) {
            let _ = self
                .0
                .send(FlushEvent::FlushCompleted(cf.to_string(), seqno));
        }
    }

    #[test]
    fn test_flush_listener() {
        let dir = tempfile::Builder::new()
            .prefix("tikv-engine-tests")
            .tempdir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        let mut db_opts = RawDBOptions::new();
        let (tx, rx) = mpsc::sync_channel(2);
        let listener = FlushListener::new(TestNotifier(tx));
        db_opts.add_event_listener(listener);
        let cf_opts = ColumnFamilyOptions::new();
        let engine = new_engine_opt(
            path,
            RocksDbOptions::from_raw(db_opts),
            vec![(CF_DEFAULT, RocksCfOptions::from_raw(cf_opts))],
        )
        .unwrap();
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

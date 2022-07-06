// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::Ordering;

use parking_lot_core::SpinWait;
use rocksdb::{EventListener, FlushJobInfo, MemTableInfo};
use tikv_util::{
    debug,
    sequence_number::{
        FLUSHED_MAX_SEQUENCE_NUMBERS, MEMTABLE_SEALED_COUNTER_ALLOCATOR, SYNCED_MAX_SEQUENCE_NUMBER,
    },
};

#[derive(Clone, Default)]
pub struct FlushListener;

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
        let flush_seqno = info.largest_seqno();
        let cf = info.cf_name();
        let flushed_max_seqno = FLUSHED_MAX_SEQUENCE_NUMBERS.get(cf).unwrap();
        // notify store to GC relations and raft logs
        let mut current = flushed_max_seqno.load(Ordering::SeqCst);
        while current < flush_seqno {
            if let Err(cur) = flushed_max_seqno.compare_exchange_weak(
                current,
                flush_seqno,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                current = cur;
            }
        }
        debug!("flush completed"; "seqno" => flush_seqno);
    }

    fn on_memtable_sealed(&self, info: &MemTableInfo) {
        let version = MEMTABLE_SEALED_COUNTER_ALLOCATOR.fetch_add(1, Ordering::SeqCst);
        debug!(
            "memtable sealed";
            "cf" => info.cf_name(),
            "first_seqno" => info.first_seqno(),
            "earliest_seqno" => info.earliest_seqno(),
            "version" => version+1
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use engine_traits::{
        ColumnFamilyOptions, MiscExt, Mutable, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT,
    };
    use rocksdb::DBOptions as RawDBOptions;
    use tikv_util::sequence_number::{FLUSHED_MAX_SEQUENCE_NUMBERS, SYNCED_MAX_SEQUENCE_NUMBER};

    use crate::{
        util::{new_engine_opt, RocksCFOptions},
        FlushListener, RocksColumnFamilyOptions, RocksDBOptions,
    };

    #[test]
    fn test_flush_listener() {
        let dir = tempfile::Builder::new()
            .prefix("tikv-engine-tests")
            .tempdir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        let mut db_opts = RawDBOptions::new();
        db_opts.add_event_listener(FlushListener::default());
        let cf_opts = RocksColumnFamilyOptions::new();
        let engine = new_engine_opt(
            path,
            RocksDBOptions::from_raw(db_opts),
            vec![RocksCFOptions::new(CF_DEFAULT, cf_opts)],
        )
        .unwrap();
        let mut batch = engine.write_batch();
        batch.put_cf(CF_DEFAULT, b"k", b"v").unwrap();
        let mut write_opts = WriteOptions::new();
        write_opts.set_disable_wal(true);
        batch.write_opt(&write_opts).unwrap();
        batch.write_opt(&write_opts).unwrap();
        let seq = batch.write_opt(&write_opts).unwrap();
        SYNCED_MAX_SEQUENCE_NUMBER.store(seq, Ordering::SeqCst);
        engine.flush(true).unwrap();
        let flushed_max_seqno = FLUSHED_MAX_SEQUENCE_NUMBERS.get(CF_DEFAULT).unwrap();
        assert_eq!(flushed_max_seqno.load(Ordering::SeqCst), 3);
        let seq = batch.write_opt(&write_opts).unwrap();
        SYNCED_MAX_SEQUENCE_NUMBER.store(seq, Ordering::SeqCst);
        engine.flush(true).unwrap();
        assert_eq!(flushed_max_seqno.load(Ordering::SeqCst), 4);
    }
}

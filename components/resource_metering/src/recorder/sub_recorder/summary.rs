// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::Ordering::{Relaxed, SeqCst};

use collections::HashMap;
use tikv_util::sys::thread::Pid;

use crate::{
    ENABLE_NETWORK_IO_COLLECTION, RawRecords,
    recorder::{
        SubRecorder,
        localstorage::{LocalStorage, STORAGE},
    },
};

/// Records how many keys have been read in the current context.
pub fn record_read_keys(count: u32) {
    STORAGE.with(|s| {
        s.borrow()
            .summary_cur_record
            .read_keys
            .fetch_add(count, Relaxed);
    })
}

/// Records how many keys have been written in the current context.
pub fn record_write_keys(count: u32) {
    STORAGE.with(|s| {
        s.borrow()
            .summary_cur_record
            .write_keys
            .fetch_add(count, Relaxed);
    })
}

/// Records how many bytes have been received in the current context.
pub fn record_network_in_bytes(bytes: u64) {
    if !ENABLE_NETWORK_IO_COLLECTION.load(Relaxed) {
        return;
    }
    STORAGE.with(|s| {
        s.borrow()
            .summary_cur_record
            .network_in_bytes
            .fetch_add(bytes, Relaxed);
    })
}

/// Records how many bytes have been sent in the current context.
pub fn record_network_out_bytes(bytes: u64) {
    if !ENABLE_NETWORK_IO_COLLECTION.load(Relaxed) {
        return;
    }
    STORAGE.with(|s| {
        s.borrow()
            .summary_cur_record
            .network_out_bytes
            .fetch_add(bytes, Relaxed);
    })
}

/// Records how many bytes have been read in the current context.
pub fn record_logical_read_bytes(bytes: u64) {
    if !ENABLE_NETWORK_IO_COLLECTION.load(Relaxed) {
        return;
    }
    STORAGE.with(|s| {
        s.borrow()
            .summary_cur_record
            .logical_read_bytes
            .fetch_add(bytes, Relaxed);
    })
}

/// Records how many bytes have been written in the current context.
pub fn record_logical_write_bytes(bytes: u64) {
    if !ENABLE_NETWORK_IO_COLLECTION.load(Relaxed) {
        return;
    }
    STORAGE.with(|s| {
        s.borrow()
            .summary_cur_record
            .logical_write_bytes
            .fetch_add(bytes, Relaxed);
    })
}

/// An implementation of [SubRecorder] for collecting summary data.
///
/// `SummaryRecorder` uses some special methods
/// ([record_read_keys]/[record_write_keys]) to collect external statistical
/// information.
///
/// See [SubRecorder] for more relevant designs.
///
/// [SubRecorder]: crate::recorder::SubRecorder
#[derive(Default)]
pub struct SummaryRecorder {
    enabled: bool,
}

impl SubRecorder for SummaryRecorder {
    fn collect(
        &mut self,
        records: &mut RawRecords,
        thread_stores: &mut HashMap<Pid, LocalStorage>,
    ) {
        thread_stores.iter_mut().for_each(|(_, ls)| {
            let summary = { std::mem::take(&mut *ls.summary_records.lock().unwrap()) };
            for (k, v) in summary {
                records.records.entry(k).or_default().merge_summary(&v);
            }
            // The request currently being polled has not yet been merged into the hashmap,
            // so it needs to be processed separately. (For example, a slow request that is
            // blocking needs to reflect in real time how many keys have been read
            // currently)
            if let Some(t) = ls.attached_tag.load_full() {
                if t.extra_attachment.is_empty() {
                    return;
                }
                let s = ls.summary_cur_record.take_and_reset();
                records.records.entry(t).or_default().merge_summary(&s);
            }
            // Update latest switch.
            ls.summary_enable.store(self.enabled, SeqCst);
        });
    }

    fn pause(&mut self, _records: &mut RawRecords, thread_stores: &mut HashMap<Pid, LocalStorage>) {
        thread_stores.iter().for_each(|(_, ls)| {
            ls.summary_enable.store(false, SeqCst);
        });
        self.enabled = false;
    }

    fn resume(
        &mut self,
        _records: &mut RawRecords,
        thread_stores: &mut HashMap<Pid, LocalStorage>,
    ) {
        thread_stores.iter().for_each(|(_, ls)| {
            ls.summary_enable.store(true, SeqCst);
        });
        self.enabled = true;
    }

    fn thread_created(&mut self, _id: Pid, store: &LocalStorage) {
        store.summary_enable.store(self.enabled, SeqCst);
    }
}

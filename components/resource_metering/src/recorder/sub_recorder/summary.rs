// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::recorder::localstorage::{LocalStorage, STORAGE};
use crate::recorder::SubRecorder;
use crate::{RawRecords, SummaryRecord, TagInfos};

use std::sync::atomic::Ordering::{Relaxed, SeqCst};

use collections::HashMap;

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

/// An implementation of [SubRecorder] for collecting summary data.
///
/// `SummaryRecorder` uses some special methods ([record_read_keys]/[record_write_keys])
/// to collect external statistical information.
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
        thread_stores: &mut HashMap<usize, LocalStorage>,
    ) {
        thread_stores.iter_mut().for_each(|(_, ls)| {
            let summary = { std::mem::take(&mut *ls.summary_records.lock().unwrap()) };
            for (k, v) in summary {
                merge_summary(records, &k, &v);
            }
            // The request currently being polled has not yet been merged into the hashmap,
            // so it needs to be processed separately. (For example, a slow request that is
            // blocking needs to reflect in real time how many keys have been read currently)
            if let Some(t) = ls.attached_tag.load_full() {
                if t.extra_attachment.is_empty() {
                    return;
                }
                let s = ls.summary_cur_record.take_and_reset();
                merge_summary(records, &t, &s);
            }
            // Update latest switch.
            ls.summary_enable.store(self.enabled, SeqCst);
        });
    }

    fn pause(
        &mut self,
        _records: &mut RawRecords,
        thread_stores: &mut HashMap<usize, LocalStorage>,
    ) {
        thread_stores.iter().for_each(|(_, ls)| {
            ls.summary_enable.store(false, SeqCst);
        });
        self.enabled = false;
    }

    fn resume(
        &mut self,
        _records: &mut RawRecords,
        thread_stores: &mut HashMap<usize, LocalStorage>,
    ) {
        thread_stores.iter().for_each(|(_, ls)| {
            ls.summary_enable.store(true, SeqCst);
        });
        self.enabled = true;
    }

    fn thread_created(&mut self, _id: usize, store: &LocalStorage) {
        store.summary_enable.store(self.enabled, SeqCst);
    }
}

fn merge_summary(records: &mut RawRecords, tag: &TagInfos, summary: &SummaryRecord) {
    if let Some(r) = records.records.get_mut(tag) {
        r.merge_summary(summary);
    } else {
        // We will merge any TagInfo which cannot be found in `records.records`
        // into `records.others` after CpuRecorder executes `keep_top_k()`. So we
        // depend on SummaryRecorder to execute after CpuRecorder.
        records.others.merge_summary(summary);
    }
}

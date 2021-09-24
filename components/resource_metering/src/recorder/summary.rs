// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::localstorage::{LocalStorage, STORAGE};
use crate::recorder::SubRecorder;
use crate::{RawRecord, RawRecords};
use collections::HashMap;
use std::sync::atomic::Ordering::Relaxed;

/// Records how many keys have been read in the current context.
pub fn record_read_keys(count: u32) {
    STORAGE.with(|s| {
        s.summary_cur_record.r_count.fetch_add(count, Relaxed);
    })
}

/// Records how many keys have been written in the current context.
pub fn record_write_keys(count: u32) {
    STORAGE.with(|s| {
        s.summary_cur_record.w_count.fetch_add(count, Relaxed);
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
pub struct SummaryRecorder;

impl SubRecorder for SummaryRecorder {
    fn collect(
        &mut self,
        records: &mut RawRecords,
        thread_stores: &mut HashMap<usize, LocalStorage>,
    ) {
        thread_stores.iter_mut().for_each(|(_, s)| {
            {
                let mut summary = s.summary_records.lock().unwrap();
                for (k, v) in summary.drain() {
                    records
                        .records
                        .entry(k)
                        .or_insert_with(RawRecord::default)
                        .merge_summary(&v);
                }
                // unlock records here.
            }
            // The request currently being polled has not yet been merged into the hashmap,
            // so it needs to be processed separately. (For example, a slow request that is
            // blocking needs to reflect in real time how many keys have been read currently)
            if let Some(tag) = s.shared_ptr.take_clone() {
                if !tag.infos.extra_attachment.is_empty() {
                    records
                        .records
                        .entry(tag)
                        .or_insert_with(RawRecord::default)
                        .merge_summary(&s.summary_cur_record.take_and_reset())
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::localstorage::register_storage_chan_tx;
    use crate::GLOBAL_ENABLE;
    use crate::{ResourceMeteringTag, TagInfos};
    use crossbeam::channel::unbounded;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use tikv_util::defer;

    #[test]
    fn test_collect() {
        GLOBAL_ENABLE.store(true, SeqCst);
        defer! {{
            GLOBAL_ENABLE.store(false, SeqCst);
        }};
        let (tx, rx) = unbounded();
        register_storage_chan_tx(tx);

        std::thread::spawn(|| {
            let tag = ResourceMeteringTag {
                infos: Arc::new(TagInfos {
                    store_id: 0,
                    region_id: 0,
                    peer_id: 0,
                    extra_attachment: b"abc".to_vec(),
                }),
            };
            {
                let _guard = tag.attach();
                record_read_keys(1);
                record_write_keys(2);
                STORAGE.with(|s| {
                    assert_eq!(s.summary_cur_record.r_count.load(Relaxed), 1);
                    assert_eq!(s.summary_cur_record.w_count.load(Relaxed), 2);
                    assert_eq!(s.summary_records.lock().unwrap().len(), 0);
                });
                // summary_cur_record here will be merged into the summary_records.
            }
            STORAGE.with(|s| {
                assert_eq!(s.summary_records.lock().unwrap().len(), 1);
            });
            let _guard = tag.attach();
            record_read_keys(3);
            record_write_keys(4);
            STORAGE.with(|s| {
                assert_eq!(s.summary_cur_record.r_count.load(Relaxed), 3);
                assert_eq!(s.summary_cur_record.w_count.load(Relaxed), 4);
                assert_eq!(s.summary_records.lock().unwrap().len(), 1);
            });

            std::thread::spawn(move || {
                let tag = ResourceMeteringTag {
                    infos: Arc::new(TagInfos {
                        store_id: 0,
                        region_id: 0,
                        peer_id: 0,
                        extra_attachment: b"def".to_vec(),
                    }),
                };
                let _guard = tag.attach();
                record_read_keys(5);
                record_write_keys(6);
                STORAGE.with(|s| {
                    assert_eq!(s.summary_cur_record.r_count.load(Relaxed), 5);
                    assert_eq!(s.summary_cur_record.w_count.load(Relaxed), 6);
                    assert_eq!(s.summary_records.lock().unwrap().len(), 0);
                });
            })
            .join()
            .unwrap();
        })
        .join()
        .unwrap();

        let mut records = RawRecords::default();
        let mut thread_stores = HashMap::default();
        while let Ok(lsr) = rx.try_recv() {
            thread_stores.insert(lsr.id, lsr.storage.clone());
        }
        let mut recorder = SummaryRecorder::default();
        recorder.collect(&mut records, &mut thread_stores);
        assert!(!records.records.is_empty());
    }
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collector::Collector;
use crate::localstorage::{LocalStorage, STORAGE};
use crate::recorder::SubRecorder;
use crate::summary::SummaryRecord;
use collections::HashMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use tikv_util::time::Instant;

const COLLECT_INTERVAL_SECS: u64 = 5;

/// An implementation of [SubRecorder] for collecting summary data.
///
/// `SummaryRecorder` uses some special methods to collect external statistical
/// information, and then send it to [Collector].
///
/// See [SubRecorder] for more relevant designs.
///
/// [SubRecorder]: crate::recorder::SubRecorder
/// [Collector]: crate::collector::Collector
pub struct SummaryRecorder<C> {
    collector: C,
    last_collect: Instant,
}

impl<C> SubRecorder for SummaryRecorder<C>
where
    C: Collector<Arc<HashMap<Vec<u8>, SummaryRecord>>>,
{
    fn tick(&mut self, thread_stores: &mut HashMap<usize, LocalStorage>) {
        if self.last_collect.saturating_elapsed().as_secs() >= COLLECT_INTERVAL_SECS {
            self.collect(thread_stores);
            self.last_collect = Instant::now();
        }
    }

    fn reset(&mut self) {
        self.last_collect = Instant::now();
    }
}

impl<C> SummaryRecorder<C>
where
    C: Collector<Arc<HashMap<Vec<u8>, SummaryRecord>>>,
{
    pub fn new(collector: C) -> Self {
        Self {
            collector,
            last_collect: Instant::now(),
        }
    }

    fn collect(&mut self, thread_stores: &mut HashMap<usize, LocalStorage>) {
        let mut total = HashMap::default();
        thread_stores.iter_mut().for_each(|(_, s)| {
            {
                let mut records = s.summary_records.lock().unwrap();
                for (k, v) in records.drain() {
                    total
                        .entry(k)
                        .or_insert_with(SummaryRecord::default)
                        .merge(&v);
                }
                // unlock records here.
            }
            // The request currently being polled has not yet been merged into the hashmap,
            // so it needs to be processed separately. (For example, a slow request that is
            // blocking needs to reflect in real time how many keys have been read currently)
            if let Some(tag) = s.shared_ptr.take_clone() {
                total
                    .entry(tag.infos.extra_attachment.clone())
                    .or_insert_with(SummaryRecord::default)
                    .merge(&s.summary_cur_record.take_and_reset())
            }
        });
        self.collector.collect(Arc::new(total));
    }
}

/// Records how many keys have been read in the current context.
pub fn record_read_keys(count: u64) {
    STORAGE.with(|s| {
        s.summary_cur_record.r_count.fetch_add(count, Relaxed);
    })
}

/// Records how many keys have been written in the current context.
pub fn record_write_keys(count: u64) {
    STORAGE.with(|s| {
        s.summary_cur_record.w_count.fetch_add(count, Relaxed);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::Collector;
    use crate::localstorage::STORAGE_CHAN;
    use crate::{ResourceMeteringTag, TagInfos};
    use std::sync::mpsc::channel;

    struct MockCollector;

    impl Collector<Arc<HashMap<Vec<u8>, SummaryRecord>>> for MockCollector {
        fn collect(&self, v: Arc<HashMap<Vec<u8>, SummaryRecord>>) {
            assert_eq!(v.len(), 2);
            assert_eq!(v.get("abc".as_bytes()).unwrap().r_count.load(Relaxed), 4);
            assert_eq!(v.get("abc".as_bytes()).unwrap().w_count.load(Relaxed), 6);
            assert_eq!(v.get("def".as_bytes()).unwrap().r_count.load(Relaxed), 5);
            assert_eq!(v.get("def".as_bytes()).unwrap().w_count.load(Relaxed), 6);
        }
    }

    #[test]
    fn test_collect() {
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

        // Start another thread for testing.
        let (done_send, done_recv) = channel();
        let handle = std::thread::spawn(move || {
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
            done_send.send(()).unwrap();
            std::thread::park(); // Waiting for test.
        });

        // Prepare thread stores.
        done_recv.recv().unwrap();
        let mut thread_stores = HashMap::default();
        while let Ok(lsr) = STORAGE_CHAN.1.try_recv() {
            thread_stores.insert(lsr.id, lsr.storage.clone());
        }

        // Do it.
        let mut recorder = SummaryRecorder::new(MockCollector);
        recorder.collect(&mut thread_stores);

        handle.thread().unpark();
        handle.join().unwrap();
    }
}

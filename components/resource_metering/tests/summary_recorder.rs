// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use resource_metering::{
    Collector, RecorderBuilder, ResourceMeteringTag, SummaryRecord, SummaryRecorder, TagInfos,
    GLOBAL_ENABLE,
};
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tikv_util::defer;
use Operation::*;

enum Operation {
    SetContext(&'static str),
    ResetContext,
    RecordReadKeys(u32),
    RecordWriteKeys(u32),
}

struct Operations {
    ops: Vec<Operation>,
    current_ctx: Option<&'static str>,
    records: HashMap<Vec<u8>, SummaryRecord>,
}

impl Operations {
    fn begin() -> Self {
        Self {
            ops: Vec::default(),
            current_ctx: None,
            records: HashMap::default(),
        }
    }

    fn then(mut self, op: Operation) -> Self {
        match op {
            SetContext(tag) => {
                assert!(self.current_ctx.is_none(), "cannot set nested contexts");
                self.current_ctx = Some(tag);
                self.ops.push(op);
                self
            }
            ResetContext => {
                assert!(self.current_ctx.is_some(), "context is not set");
                self.current_ctx = None;
                self.ops.push(op);
                self
            }
            RecordReadKeys(count) => {
                if let Some(tag) = self.current_ctx {
                    self.records
                        .entry(tag.as_bytes().to_vec())
                        .or_insert_with(SummaryRecord::default)
                        .r_count
                        .fetch_add(count, Relaxed);
                }
                self.ops.push(op);
                self
            }
            RecordWriteKeys(count) => {
                if let Some(tag) = self.current_ctx {
                    self.records
                        .entry(tag.as_bytes().to_vec())
                        .or_insert_with(SummaryRecord::default)
                        .w_count
                        .fetch_add(count, Relaxed);
                }
                self.ops.push(op);
                self
            }
        }
    }

    fn spawn(self) -> (JoinHandle<()>, HashMap<Vec<u8>, SummaryRecord>) {
        assert!(
            self.current_ctx.is_none(),
            "should keep context clean finally"
        );

        let Operations { ops, records, .. } = self;

        let handle = std::thread::spawn(|| {
            let mut guard = None;

            for op in ops {
                match op {
                    SetContext(tag) => {
                        let tag = ResourceMeteringTag::from(Arc::new(TagInfos {
                            store_id: 0,
                            region_id: 0,
                            peer_id: 0,
                            extra_attachment: tag.as_bytes().to_vec(),
                        }));
                        guard = Some(tag.attach());
                    }
                    ResetContext => {
                        guard.take();
                    }
                    RecordReadKeys(count) => {
                        resource_metering::record_read_keys(count);
                    }
                    RecordWriteKeys(count) => {
                        resource_metering::record_write_keys(count);
                    }
                }
            }
        });

        (handle, records)
    }
}

#[derive(Default, Clone)]
struct MockCollector {
    records: Arc<Mutex<HashMap<Vec<u8>, SummaryRecord>>>,
}

impl Collector<Arc<HashMap<Vec<u8>, SummaryRecord>>> for MockCollector {
    fn collect(&self, records: Arc<HashMap<Vec<u8>, SummaryRecord>>) {
        if let Ok(mut r) = self.records.lock() {
            for (tag, record) in records.iter() {
                r.entry(tag.clone())
                    .or_insert_with(SummaryRecord::default)
                    .merge(record);
            }
        }
    }
}

impl MockCollector {
    fn check(&self, mut expected: HashMap<Vec<u8>, SummaryRecord>) {
        // Wait a collect interval to avoid losing records.
        //
        // See resource_metering::summary::recorder::COLLECT_INTERVAL_SECS
        std::thread::sleep(Duration::from_millis(1500));

        let mut records = self.records.lock().unwrap();
        for k in expected.keys() {
            records
                .entry(k.clone())
                .or_insert_with(SummaryRecord::default);
        }
        for k in records.keys() {
            expected
                .entry(k.clone())
                .or_insert_with(SummaryRecord::default);
        }
        for (k, expected_value) in expected {
            let value = records.get(&k).unwrap();
            if value.r_count.load(Relaxed) != expected_value.r_count.load(Relaxed) {
                panic!(
                    "tag {} read count expected {:?} but got {:?}",
                    String::from_utf8_lossy(&k),
                    expected_value,
                    value
                );
            }
            if value.w_count.load(Relaxed) != expected_value.w_count.load(Relaxed) {
                panic!(
                    "tag {} write count expected {:?} but got {:?}",
                    String::from_utf8_lossy(&k),
                    expected_value,
                    value
                );
            }
        }
    }
}

#[test]
fn test_summary_recorder() {
    // Turn on the switch explicitly.
    GLOBAL_ENABLE.store(true, SeqCst);

    let collector = MockCollector::default();
    let records = collector.records.clone();
    let handle = RecorderBuilder::default()
        .add_sub_recorder(Box::new(SummaryRecorder::new(collector.clone())))
        .spawn()
        .unwrap();
    handle.resume();
    defer! {{
        handle.pause();
    }};

    {
        let (handle, expected) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(RecordReadKeys(101))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();
        collector.check(expected);
        records.lock().unwrap().clear();
    }

    {
        let (handle, expected) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(RecordReadKeys(101))
            .then(RecordWriteKeys(102))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();
        collector.check(expected);
        records.lock().unwrap().clear();
    }

    {
        let (handle, expected) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(RecordReadKeys(101))
            .then(RecordWriteKeys(102))
            .then(ResetContext)
            .then(SetContext("ctx-1"))
            .then(RecordReadKeys(103))
            .then(RecordWriteKeys(104))
            .then(ResetContext)
            .then(SetContext("ctx-2"))
            .then(RecordReadKeys(105))
            .then(RecordWriteKeys(106))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();
        collector.check(expected);
        records.lock().unwrap().clear();
    }

    // Execute `record_xxx` out of context.
    {
        let (handle, expected) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(ResetContext)
            .then(RecordReadKeys(101))
            .then(RecordWriteKeys(102))
            .then(SetContext("ctx-1"))
            .then(RecordReadKeys(103))
            .then(RecordWriteKeys(104))
            .then(ResetContext)
            .then(SetContext("ctx-2"))
            .then(RecordReadKeys(105))
            .then(RecordWriteKeys(106))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();
        collector.check(expected);
        records.lock().unwrap().clear();
    }

    {
        // Turn off the switch explicitly.
        GLOBAL_ENABLE.store(false, SeqCst);
        let (handle, _) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(RecordReadKeys(101))
            .then(RecordWriteKeys(102))
            .then(ResetContext)
            .then(SetContext("ctx-1"))
            .then(RecordReadKeys(103))
            .then(RecordWriteKeys(104))
            .then(ResetContext)
            .then(SetContext("ctx-2"))
            .then(RecordReadKeys(105))
            .then(RecordWriteKeys(106))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();
        // No matter how many times we execute `record_xxx`, the result should be empty.
        collector.check(HashMap::default());
        records.lock().unwrap().clear();
    }
}

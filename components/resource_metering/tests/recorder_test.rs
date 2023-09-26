// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        thread::JoinHandle,
        time::Duration,
    };

    use collections::HashMap;
    use resource_metering::{init_recorder, Collector, RawRecord, RawRecords, ResourceTagFactory};
    use tikv_util::sys::thread;
    use Operation::*;

    enum Operation {
        SetContext(&'static str),
        ResetContext,
        CpuHeavy(u32),
        Sleep(u64),
    }

    struct Operations {
        ops: Vec<Operation>,
        current_ctx: Option<&'static str>,
        records: HashMap<Vec<u8>, RawRecord>,
        resource_tag_factory: ResourceTagFactory,
    }

    impl Operations {
        fn begin(resource_tag_factory: ResourceTagFactory) -> Self {
            Self {
                ops: Vec::default(),
                current_ctx: None,
                records: HashMap::default(),
                resource_tag_factory,
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
                CpuHeavy(ms) => {
                    if let Some(tag) = self.current_ctx {
                        self.records
                            .entry(tag.as_bytes().to_vec())
                            .or_insert_with(RawRecord::default)
                            .cpu_time += ms;
                    }
                    self.ops.push(op);
                    self
                }
                Sleep(_) => {
                    self.ops.push(op);
                    self
                }
            }
        }

        fn spawn(self) -> (JoinHandle<()>, HashMap<Vec<u8>, RawRecord>) {
            assert!(
                self.current_ctx.is_none(),
                "should keep context clean finally"
            );

            let Operations {
                ops,
                records,
                resource_tag_factory,
                ..
            } = self;

            let handle = std::thread::spawn(move || {
                let mut guard = None;

                for op in ops {
                    match op {
                        SetContext(tag) => {
                            let mut ctx = kvproto::kvrpcpb::Context::default();
                            ctx.mut_resource_group_tag()
                                .extend_from_slice(tag.as_bytes());
                            let tag = resource_tag_factory.new_tag(&ctx);
                            guard = Some(tag.attach());
                        }
                        ResetContext => {
                            guard.take();
                        }
                        CpuHeavy(ms) => {
                            let begin_stat = thread::current_thread_stat().unwrap();
                            loop {
                                Self::heavy_job();
                                let later_stat = thread::current_thread_stat().unwrap();
                                let delta =
                                    later_stat.total_cpu_time() - begin_stat.total_cpu_time();
                                if delta >= ms as f64 * 0.001 {
                                    break;
                                }
                            }
                        }
                        Sleep(ms) => {
                            std::thread::sleep(Duration::from_millis(ms));
                        }
                    }
                }
            });

            (handle, records)
        }

        fn heavy_job() -> u64 {
            let m: u64 = rand::random();
            let n: u64 = rand::random();
            let m = m ^ n;
            let n = m.wrapping_mul(n);
            let m = m.wrapping_add(n);
            let n = m & n;
            let m = m | n;
            m.wrapping_sub(n)
        }
    }

    #[derive(Default, Clone)]
    struct DummyCollector {
        records: Arc<Mutex<HashMap<Vec<u8>, RawRecord>>>,
    }

    impl Collector for DummyCollector {
        fn collect(&self, records: Arc<RawRecords>) {
            if let Ok(mut r) = self.records.lock() {
                for (tag, record) in records.records.iter() {
                    r.entry(tag.extra_attachment.to_vec())
                        .or_insert_with(RawRecord::default)
                        .merge(record);
                }
            }
        }
    }

    impl DummyCollector {
        fn check(&self, mut expected: HashMap<Vec<u8>, RawRecord>) {
            const MAX_DRIFT: u32 = 200;

            // Wait a collect interval to avoid losing records.
            std::thread::sleep(Duration::from_millis(1200));

            let mut records = self.records.lock().unwrap();
            for k in expected.keys() {
                records.entry(k.clone()).or_insert_with(RawRecord::default);
            }
            for k in records.keys() {
                expected.entry(k.clone()).or_insert_with(RawRecord::default);
            }
            for (k, expected_value) in expected {
                let value = records.get(&k).unwrap();
                let l = value.cpu_time.saturating_sub(MAX_DRIFT);
                let r = value.cpu_time.saturating_add(MAX_DRIFT);
                if !(l <= expected_value.cpu_time && expected_value.cpu_time <= r) {
                    panic!(
                        "tag {} cpu time expected {} but got {}",
                        String::from_utf8_lossy(&k),
                        expected_value.cpu_time,
                        value.cpu_time
                    );
                }
            }
        }
    }

    #[test]
    fn test_cpu_recorder_heavy_single_thread() {
        let (_, collector_reg_handle, resource_tag_factory, worker) = init_recorder(1000);

        let collector = DummyCollector::default();
        let _handle = collector_reg_handle.register(Box::new(collector.clone()), false);

        let (handle, expected) = Operations::begin(resource_tag_factory)
            .then(SetContext("ctx-0"))
            .then(CpuHeavy(2000))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();

        collector.check(expected);

        worker.stop_worker();
    }

    #[test]
    fn test_cpu_recorder_sleep_single_thread() {
        let (_, collector_reg_handle, resource_tag_factory, worker) = init_recorder(1000);

        let collector = DummyCollector::default();
        let _handle = collector_reg_handle.register(Box::new(collector.clone()), false);

        let (handle, expected) = Operations::begin(resource_tag_factory)
            .then(SetContext("ctx-0"))
            .then(Sleep(2000))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();

        collector.check(expected);

        worker.stop_worker();
    }

    #[test]
    fn test_cpu_recorder_hybrid_single_thread() {
        let (_, collector_reg_handle, resource_tag_factory, worker) = init_recorder(1000);

        // Hybrid workload with 1 thread
        let collector = DummyCollector::default();
        let _handle = collector_reg_handle.register(Box::new(collector.clone()), false);

        let (handle, expected) = Operations::begin(resource_tag_factory)
            .then(SetContext("ctx-0"))
            .then(CpuHeavy(600))
            .then(Sleep(400))
            .then(ResetContext)
            .then(SetContext("ctx-1"))
            .then(CpuHeavy(500))
            .then(Sleep(500))
            .then(ResetContext)
            .then(SetContext("ctx-2"))
            .then(Sleep(600))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();

        collector.check(expected);

        worker.stop_worker();
    }

    #[test]
    fn test_cpu_recorder_heavy_multiple_threads() {
        let (_, collector_reg_handle, resource_tag_factory, worker) = init_recorder(1000);

        // Heavy CPU with 3 threads
        let collector = DummyCollector::default();
        let _handle = collector_reg_handle.register(Box::new(collector.clone()), false);

        let (handle0, expected0) = Operations::begin(resource_tag_factory.clone())
            .then(SetContext("ctx-0"))
            .then(CpuHeavy(1500))
            .then(ResetContext)
            .spawn();
        let (handle1, expected1) = Operations::begin(resource_tag_factory.clone())
            .then(SetContext("ctx-1"))
            .then(CpuHeavy(1500))
            .then(ResetContext)
            .spawn();
        let (handle2, expected2) = Operations::begin(resource_tag_factory)
            .then(SetContext("ctx-2"))
            .then(CpuHeavy(1500))
            .then(ResetContext)
            .spawn();
        handle0.join().unwrap();
        handle1.join().unwrap();
        handle2.join().unwrap();

        collector.check(merge(vec![expected0, expected1, expected2]));

        worker.stop_worker();
    }

    #[test]
    fn test_cpu_recorder_hybrid_multiple_threads() {
        let (_, collector_reg_handle, resource_tag_factory, worker) = init_recorder(1000);

        // Hybrid workload with 3 threads
        let collector = DummyCollector::default();
        let _handle = collector_reg_handle.register(Box::new(collector.clone()), false);

        let (handle0, expected0) = Operations::begin(resource_tag_factory.clone())
            .then(SetContext("ctx-0"))
            .then(CpuHeavy(200))
            .then(Sleep(300))
            .then(ResetContext)
            .then(SetContext("ctx-1"))
            .then(Sleep(200))
            .then(CpuHeavy(600))
            .then(ResetContext)
            .then(CpuHeavy(500))
            .spawn();
        let (handle1, expected1) = Operations::begin(resource_tag_factory.clone())
            .then(SetContext("ctx-1"))
            .then(CpuHeavy(500))
            .then(ResetContext)
            .then(SetContext("ctx-2"))
            .then(Sleep(400))
            .then(ResetContext)
            .then(Sleep(300))
            .spawn();
        let (handle2, expected2) = Operations::begin(resource_tag_factory)
            .then(SetContext("ctx-2"))
            .then(CpuHeavy(800))
            .then(ResetContext)
            .then(SetContext("ctx-1"))
            .then(Sleep(200))
            .then(ResetContext)
            .then(CpuHeavy(200))
            .spawn();
        handle0.join().unwrap();
        handle1.join().unwrap();
        handle2.join().unwrap();

        collector.check(merge(vec![expected0, expected1, expected2]));

        worker.stop_worker();
    }

    fn merge(
        maps: impl IntoIterator<Item = HashMap<Vec<u8>, RawRecord>>,
    ) -> HashMap<Vec<u8>, RawRecord> {
        let mut map = HashMap::default();
        for m in maps {
            for (k, v) in m {
                map.entry(k).or_insert_with(RawRecord::default).merge(&v);
            }
        }
        map
    }
}

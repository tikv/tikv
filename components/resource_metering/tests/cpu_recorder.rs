// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use lazy_static::lazy_static;
    use resource_metering::cpu::collector::{register_collector, Collector};
    use resource_metering::cpu::recorder::{init_recorder, CpuRecords, TEST_TAG_PREFIX};
    use resource_metering::{ResourceMeteringTag, TagInfos};
    use tikv_util::defer;

    enum Operation {
        SetContext(&'static str),
        ResetContext,
        CpuHeavy(u64),
        Sleep(u64),
    }

    use Operation::*;

    struct Operations {
        ops: Vec<Operation>,
        current_ctx: Option<&'static str>,
        cpu_time: HashMap<String, u64>,
    }

    impl Operations {
        fn begin() -> Self {
            Self {
                ops: Vec::default(),
                current_ctx: None,
                cpu_time: HashMap::default(),
            }
        }

        fn then(mut self, op: Operation) -> Self {
            match op {
                Operation::SetContext(tag) => {
                    assert!(self.current_ctx.is_none(), "cannot set nested contexts");
                    self.current_ctx = Some(tag);
                    self.ops.push(op);
                    self
                }
                Operation::ResetContext => {
                    assert!(self.current_ctx.is_some(), "context is not set");
                    self.ops.push(op);
                    self.current_ctx = None;
                    self
                }
                Operation::CpuHeavy(ms) => {
                    if let Some(tag) = self.current_ctx {
                        *self.cpu_time.entry(tag.to_string()).or_insert(0) += ms;
                    }
                    self.ops.push(op);
                    self
                }
                Operation::Sleep(_) => {
                    self.ops.push(op);
                    self
                }
            }
        }

        fn spawn(self) -> (JoinHandle<()>, HashMap<String, u64>) {
            assert!(
                self.current_ctx.is_none(),
                "should keep context clean finally"
            );

            let Operations { ops, cpu_time, .. } = self;

            let handle = std::thread::spawn(|| {
                let mut guard = None;
                let thread_id = unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t };

                for op in ops {
                    match op {
                        Operation::SetContext(tag) => {
                            let tag = ResourceMeteringTag::from(Arc::new(TagInfos {
                                store_id: 0,
                                region_id: 0,
                                peer_id: 0,
                                extra_attachment: {
                                    let mut t = Vec::from(TEST_TAG_PREFIX);
                                    t.extend_from_slice(tag.as_bytes());
                                    t
                                },
                            }));

                            guard = Some(tag.attach());
                        }
                        Operation::ResetContext => {
                            guard.take();
                        }
                        Operation::CpuHeavy(ms) => {
                            let begin_stat = procinfo::pid::stat_task(*PID, thread_id).unwrap();
                            let begin_ticks =
                                (begin_stat.utime as u64).wrapping_add(begin_stat.stime as u64);

                            loop {
                                Self::heavy_job();
                                let later_stat = procinfo::pid::stat_task(*PID, thread_id).unwrap();

                                let later_ticks =
                                    (later_stat.utime as u64).wrapping_add(later_stat.stime as u64);
                                let delta_ms = later_ticks.wrapping_sub(begin_ticks) * 1_000
                                    / (*CLK_TCK as u64);

                                if delta_ms >= ms {
                                    break;
                                }
                            }
                        }
                        Operation::Sleep(ms) => {
                            std::thread::sleep(Duration::from_millis(ms));
                        }
                    }
                }
            });

            (handle, cpu_time)
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
        records: Arc<Mutex<HashMap<String, u64>>>,
    }

    impl Collector for DummyCollector {
        fn collect(&self, records: Arc<CpuRecords>) {
            if let Ok(mut r) = self.records.lock() {
                for (tag, ms) in &records.records {
                    let (_, t) = tag.infos.extra_attachment.split_at(TEST_TAG_PREFIX.len());
                    let str = String::from_utf8_lossy(t).into_owned();
                    *r.entry(str).or_insert(0) += *ms;
                }
            }
        }
    }

    lazy_static! {
        static ref PID: libc::pid_t = unsafe { libc::getpid() };
        static ref CLK_TCK: libc::c_long = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    }

    #[test]
    fn test_cpu_record() {
        let handle = init_recorder();
        handle.resume();
        fail::cfg("cpu-record-test-filter", "return").unwrap();

        defer! {{
            fail::remove("cpu-record-test-filter");
            handle.pause();
        }};

        // Heavy CPU only with 1 thread
        {
            let collector = DummyCollector::default();
            let _handle = register_collector(Box::new(collector.clone()));

            let (handle, expected) = Operations::begin()
                .then(SetContext("ctx-0"))
                .then(CpuHeavy(2000))
                .then(ResetContext)
                .spawn();
            handle.join().unwrap();

            collector.check(expected);
        }

        // Sleep only with 1 thread
        {
            let collector = DummyCollector::default();
            let _handle = register_collector(Box::new(collector.clone()));

            let (handle, expected) = Operations::begin()
                .then(SetContext("ctx-0"))
                .then(Sleep(2000))
                .then(ResetContext)
                .spawn();
            handle.join().unwrap();

            collector.check(expected);
        }

        // Hybrid workload with 1 thread
        {
            let collector = DummyCollector::default();
            let _handle = register_collector(Box::new(collector.clone()));

            let (handle, expected) = Operations::begin()
                .then(SetContext("ctx-0"))
                .then(CpuHeavy(600))
                .then(Sleep(400))
                .then(ResetContext)
                .then(SetContext("ctx-1"))
                .then(CpuHeavy(500))
                .then(Sleep(500))
                .then(ResetContext)
                .then(CpuHeavy(400))
                .then(SetContext("ctx-2"))
                .then(Sleep(600))
                .then(ResetContext)
                .spawn();
            handle.join().unwrap();

            collector.check(expected);
        }

        // Heavy CPU with 3 threads
        {
            let collector = DummyCollector::default();
            let _handle = register_collector(Box::new(collector.clone()));

            let (handle0, expected0) = Operations::begin()
                .then(SetContext("ctx-0"))
                .then(CpuHeavy(1500))
                .then(ResetContext)
                .spawn();
            let (handle1, expected1) = Operations::begin()
                .then(SetContext("ctx-1"))
                .then(CpuHeavy(1500))
                .then(ResetContext)
                .spawn();
            let (handle2, expected2) = Operations::begin()
                .then(SetContext("ctx-2"))
                .then(CpuHeavy(1500))
                .then(ResetContext)
                .spawn();
            handle0.join().unwrap();
            handle1.join().unwrap();
            handle2.join().unwrap();

            collector.check(merge(vec![expected0, expected1, expected2]));
        }

        // Hybrid workload with 3 threads
        {
            let collector = DummyCollector::default();
            let _handle = register_collector(Box::new(collector.clone()));

            let (handle0, expected0) = Operations::begin()
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
            let (handle1, expected1) = Operations::begin()
                .then(SetContext("ctx-1"))
                .then(CpuHeavy(500))
                .then(ResetContext)
                .then(CpuHeavy(200))
                .then(SetContext("ctx-2"))
                .then(Sleep(400))
                .then(ResetContext)
                .then(Sleep(300))
                .spawn();
            let (handle2, expected2) = Operations::begin()
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
        }
    }

    impl DummyCollector {
        fn check(&self, mut expected: HashMap<String, u64>) {
            // Wait a collect interval to avoid losing records.
            std::thread::sleep(Duration::from_millis(1200));

            const MAX_DRIFT: u64 = 50;
            let mut res = self.records.lock().unwrap();

            for k in expected.keys() {
                res.entry(k.clone()).or_insert(0);
            }
            for k in res.keys() {
                expected.entry(k.clone()).or_insert(0);
            }

            for (k, expected_value) in expected {
                let value = res.get(&k).unwrap();
                let l = value.saturating_sub(MAX_DRIFT);
                let r = value.saturating_add(MAX_DRIFT);
                if !(l <= expected_value && expected_value <= r) {
                    panic!(
                        "tag {} cpu time expected {} but got {}",
                        k, expected_value, value
                    );
                }
            }
        }
    }

    fn merge(maps: impl IntoIterator<Item = HashMap<String, u64>>) -> HashMap<String, u64> {
        let mut map = HashMap::default();
        for m in maps {
            for (k, v) in m {
                *map.entry(k).or_insert(0) += v;
            }
        }
        map
    }
}

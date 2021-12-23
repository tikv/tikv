// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use self::collector_reg::CollectorReg;
use self::sub_recorder::SubRecorder;
use crate::collector::Collector;
use crate::{utils, RawRecords, ResourceTagFactory};

use std::fmt::{self, Display, Formatter};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;
use std::time::Duration;

use collections::HashMap;
use tikv_util::time::Instant;
use tikv_util::worker::{Builder as WorkerBuilder, LazyWorker, Runnable, RunnableWithTimer};

mod collector_reg;
mod localstorage;
mod sub_recorder;

pub use self::collector_reg::{CollectorHandle, CollectorId, CollectorRegHandle};
pub use self::localstorage::{LocalStorage, LocalStorageRef, STORAGE};
pub use self::sub_recorder::cpu::CpuRecorder;
pub use self::sub_recorder::summary::{record_read_keys, record_write_keys, SummaryRecorder};

const RECORD_FREQUENCY: f64 = 99.0;
const RECORD_INTERVAL: Duration =
    Duration::from_micros((1_000.0 / RECORD_FREQUENCY * 1_000.0) as _);
const RECORD_LEN_THRESHOLD: usize = 20_000;
const CLEANUP_INTERVAL_SECS: u64 = 15 * 60;

impl Runnable for Recorder {
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::CollectorReg(reg) => self.handle_collector_registration(reg),
            Task::NewThread(lsr) => self.handle_thread_registration(lsr),
        }
    }

    fn shutdown(&mut self) {
        self.reset();
    }
}

impl RunnableWithTimer for Recorder {
    fn on_timeout(&mut self) {
        if self.collectors.is_empty() {
            self.pause();
            return;
        } else {
            self.resume();
        }

        self.tick();
        self.cleanup();
    }

    fn get_interval(&self) -> Duration {
        RECORD_INTERVAL
    }
}

/// Give `Recorder` a list of [SubRecorder]s and `Recorder` will make them work
/// correctly. That's it.
///
/// All SubRecorders run on the same recorder thread.
///
/// We cannot construct `Recorder` directly, but need to construct it through
/// [RecorderBuilder]. We can pass the `SubRecorder` (and other parameters)
/// that the `Recorder` needs to load through the `RecorderBuilder`.
pub struct Recorder {
    precision_ms: Arc<AtomicU64>,
    records: RawRecords,
    last_collect: Instant,
    last_cleanup: Instant,

    running: bool,
    recorders: Vec<Box<dyn SubRecorder>>,

    thread_stores: HashMap<usize, LocalStorage>,

    collectors: HashMap<CollectorId, Box<dyn Collector>>,
    observers: HashMap<CollectorId, Box<dyn Collector>>,
}

impl Recorder {
    fn handle_collector_registration(&mut self, reg: CollectorReg) {
        match reg {
            CollectorReg::Register {
                id,
                collector,
                as_observer,
            } => {
                if as_observer {
                    self.observers.insert(id, collector);
                } else {
                    self.collectors.insert(id, collector);
                }
            }
            CollectorReg::Deregister { id } => {
                self.collectors.remove(&id);
                self.observers.remove(&id);
            }
        }
    }

    fn handle_thread_registration(&mut self, lsr: LocalStorageRef) {
        self.thread_stores.insert(lsr.id, lsr.storage.clone());
        for r in &mut self.recorders {
            r.thread_created(lsr.id, &lsr.storage);
        }
    }

    fn tick(&mut self) {
        for r in &mut self.recorders {
            r.tick(&mut self.records, &mut self.thread_stores);
        }
        let duration = self.last_collect.saturating_elapsed();
        if duration.as_millis() >= self.precision_ms.load(Relaxed) as _ {
            for r in &mut self.recorders {
                r.collect(&mut self.records, &mut self.thread_stores);
            }
            let mut records = std::mem::take(&mut self.records);
            records.duration = duration;
            if !records.records.is_empty() {
                let records = Arc::new(records);
                for collector in self.collectors.values().chain(self.observers.values()) {
                    collector.collect(records.clone());
                }
            }
            self.last_collect = Instant::now();
        }
    }

    fn cleanup(&mut self) {
        if self.last_cleanup.saturating_elapsed().as_secs() > CLEANUP_INTERVAL_SECS {
            // Clean up the data of the destroyed threads.
            if let Some(ids) = utils::thread_ids() {
                self.thread_stores.retain(|k, v| {
                    let retain = ids.contains(k);
                    assert!(retain || v.attached_tag.load().is_none());
                    retain
                });
            }
            if self.records.records.capacity() > RECORD_LEN_THRESHOLD
                && self.records.records.len() < (RECORD_LEN_THRESHOLD / 2)
            {
                self.records.records.shrink_to(RECORD_LEN_THRESHOLD);
            }
            for r in &mut self.recorders {
                r.cleanup(&mut self.records, &mut self.thread_stores);
            }
            self.last_cleanup = Instant::now();
        }
    }

    fn pause(&mut self) {
        if !self.running {
            return;
        }
        self.running = false;

        for r in &mut self.recorders {
            r.pause(&mut self.records, &mut self.thread_stores);
        }
    }

    fn resume(&mut self) {
        if self.running {
            return;
        }
        self.running = true;

        let now = Instant::now();
        self.records = RawRecords::default();
        self.last_collect = now;
        self.last_cleanup = now;
        for r in &mut self.recorders {
            r.resume(&mut self.records, &mut self.thread_stores);
        }
    }

    fn reset(&mut self) {
        self.collectors.clear();
        self.observers.clear();
        self.thread_stores.clear();
        self.recorders.clear();
    }
}

pub enum Task {
    CollectorReg(CollectorReg),
    NewThread(LocalStorageRef),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::CollectorReg(_) => {
                write!(f, "CollectorReg")?;
            }
            Task::NewThread(_) => {
                write!(f, "NewThread")?;
            }
        }
        Ok(())
    }
}

/// Builder for [Recorder].
pub struct RecorderBuilder {
    precision_ms: Arc<AtomicU64>,
    recorders: Vec<Box<dyn SubRecorder>>,
}

impl Default for RecorderBuilder {
    fn default() -> Self {
        Self {
            precision_ms: Arc::new(AtomicU64::new(1000)),
            recorders: Vec::new(),
        }
    }
}

impl RecorderBuilder {
    /// Sets the precision_ms parameter of [Recorder].
    pub fn precision_ms(mut self, precision_ms: Arc<AtomicU64>) -> Self {
        self.precision_ms = precision_ms;
        self
    }

    /// Add a [SubRecorder] for the execution of [Recorder].
    pub fn add_sub_recorder(mut self, r: Box<dyn SubRecorder>) -> Self {
        self.recorders.push(r);
        self
    }

    pub fn build(self) -> (Recorder, RecorderHandle) {
        let precision_ms = self.precision_ms.clone();
        let now = Instant::now();
        let recorder = Recorder {
            precision_ms: precision_ms.clone(),
            records: RawRecords::default(),
            running: false,
            recorders: self.recorders,
            collectors: HashMap::default(),
            observers: HashMap::default(),
            thread_stores: HashMap::default(),
            last_collect: now,
            last_cleanup: now,
        };
        (recorder, RecorderHandle::new(precision_ms))
    }
}

/// This structure is returned by [RecorderBuilder::spawn] and can be used to
/// control the execution of [Recorder].
///
/// In addition, because the caller can only get an instance of `RecorderHandle`
/// and cannot directly manipulate `Recorder`, some parameters that need to be
/// controlled externally must also be stored in a thread-safe reference
/// in `RecorderHandle`.
#[derive(Clone, Default)]
pub struct RecorderHandle {
    inner: Option<Arc<RecorderHandleInner>>,
}

struct RecorderHandleInner {
    precision_ms: Arc<AtomicU64>,
}

impl RecorderHandle {
    /// Create a new `RecorderHandle`.
    ///
    /// The key pointer here is to pass a [JoinHandle], which means that `RecorderHandle`
    /// does not care about the specific execution logic, but is only responsible for
    /// controlling the execution logic from the outside.
    ///
    /// [JoinHandle]: std::thread::JoinHandle
    pub fn new(precision_ms: Arc<AtomicU64>) -> Self {
        Self {
            inner: Some(Arc::new(RecorderHandleInner { precision_ms })),
        }
    }

    /// Modify the value of the precision parameter.
    ///
    /// See [Config] for parameter usage.
    ///
    /// [Config]: crate::Config
    pub fn precision(&self, v: Duration) {
        if let Some(inner) = self.inner.as_ref() {
            inner.precision_ms.store(v.as_millis() as _, SeqCst);
        }
    }
}

/// Constructs a default [Recorder], spawn it and return the corresponding [RecorderHandle], [CollectorRegHandle],
/// [ResourceTagFactory] and [LazyWorker].
///
/// This function is intended to simplify external use.
pub fn init_recorder(
    precision_ms: u64,
) -> (
    RecorderHandle,
    CollectorRegHandle,
    ResourceTagFactory,
    Box<LazyWorker<Task>>,
) {
    let (recorder, recorder_handle) = RecorderBuilder::default()
        .precision_ms(Arc::new(AtomicU64::new(precision_ms)))
        .add_sub_recorder(Box::new(CpuRecorder::default()))
        .add_sub_recorder(Box::new(SummaryRecorder::default()))
        .build();
    let mut recorder_worker = WorkerBuilder::new("resource-metering-recorder")
        .pending_capacity(256)
        .create()
        .lazy_build("resource-metering-recorder");

    let collector_reg_handle = CollectorRegHandle::new(recorder_worker.scheduler());
    let resource_tag_factory = ResourceTagFactory::new(recorder_worker.scheduler());

    recorder_worker.start_with_timer(recorder);
    (
        recorder_handle,
        collector_reg_handle,
        resource_tag_factory,
        Box::new(recorder_worker),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::recorder::localstorage::{LocalStorage, LocalStorageRef};

    #[test]
    fn test_recorder_handle() {
        let precision_ms = Arc::new(AtomicU64::new(0));
        let handle = RecorderHandle::new(precision_ms.clone());
        let new_precision = Duration::from_secs(1);
        handle.precision(new_precision);
        assert_eq!(precision_ms.load(SeqCst), new_precision.as_millis() as u64);
    }

    #[derive(Clone, Default)]
    struct MockSubRecorder {
        op_count: Arc<AtomicUsize>,
    }

    impl SubRecorder for MockSubRecorder {
        fn tick(
            &mut self,
            _records: &mut RawRecords,
            _thread_stores: &mut HashMap<usize, LocalStorage>,
        ) {
            self.op_count.fetch_add(1, SeqCst);
        }

        fn resume(
            &mut self,
            _records: &mut RawRecords,
            _thread_stores: &mut HashMap<usize, LocalStorage>,
        ) {
            self.op_count.fetch_add(1, SeqCst);
        }

        fn thread_created(&mut self, _id: usize, _store: &LocalStorage) {
            self.op_count.fetch_add(1, SeqCst);
        }
    }

    #[test]
    fn test_recorder_basic() {
        let sub_recorder = MockSubRecorder::default();
        let (mut recorder, _) = RecorderBuilder::default()
            .add_sub_recorder(Box::new(sub_recorder.clone()))
            .build();

        recorder.tick();
        recorder.resume();
        recorder.run(Task::NewThread(LocalStorageRef {
            id: 0,
            storage: LocalStorage::default(),
        }));
        assert!(!recorder.thread_stores.is_empty());
        assert!(sub_recorder.op_count.load(SeqCst) >= 3);
    }


}

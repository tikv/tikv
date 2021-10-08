// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collector::{Collector, CollectorReg, COLLECTOR_REG_CHAN};
use crate::localstorage::{register_storage_chan_tx, LocalStorage, LocalStorageRef};
use crate::{utils, RawRecords, SharedTagPtr};

use std::io;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use collections::HashMap;
use crossbeam::channel::{unbounded, Receiver};
use tikv_util::time::Instant;

mod cpu;

pub use cpu::CpuRecorder;

const RECORD_FREQUENCY: f64 = 99.0;
const RECORD_LEN_THRESHOLD: usize = 20_000;
const CLEANUP_INTERVAL_SECS: u64 = 15 * 60;

/// This trait defines a general framework that works at a certain frequency. Typically,
/// it describes the recorder(sampler) framework for a specific resource.
///
/// [Recorder] will maintain a list of sub-recorders, driving all sub-recorders to work
/// according to the behavior described in this trait.
pub trait SubRecorder {
    /// This function is called at a fixed frequency. (A typical frequency is 99hz.)
    ///
    /// The [RawRecords] and [LocalStorage] map of all threads will be passed in through
    /// parameters. We need to collect resources (may be from each `LocalStorage`) and
    /// write them into `RawRecords`.
    ///
    /// The implementation needs to sample the resource in this function (in general).
    ///
    /// [RawRecords]: crate::model::RawRecords
    /// [LocalStorage]: crate::localstorage::LocalStorage
    fn tick(
        &mut self,
        _records: &mut RawRecords,
        _thread_stores: &mut HashMap<usize, LocalStorage>,
    ) {
    }

    /// This function is called every time before reporting to Collector.
    /// The default period is 1 second.
    ///
    /// The [RawRecords] and [LocalStorage] map of all threads will be passed in through parameters.
    ///
    /// [RawRecords]: crate::model::RawRecords
    /// [LocalStorage]: crate::localstorage::LocalStorage
    fn collect(
        &mut self,
        _records: &mut RawRecords,
        _thread_stores: &mut HashMap<usize, LocalStorage>,
    ) {
    }

    /// This function is called when we need to clean up data.
    /// The default period is 5 minutes.
    fn cleanup(&mut self) {}

    /// This function is called when a reset is required.
    ///
    /// The typical situation is when the [Recorder] thread is suspended.
    fn reset(&mut self) {}

    /// This function is called when a new thread accesses thread-local-storage.
    ///
    /// This function exists because the sampling work of `SubRecorder` may need
    /// to be performed on all functions, and `SubRecorder` may wish to maintain
    /// a thread-related data structure by itself.
    fn thread_created(&mut self, _id: usize, _tag: SharedTagPtr) {}
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
    pause: Arc<AtomicBool>,
    precision_ms: Arc<AtomicU64>,
    records: RawRecords,
    recorders: Vec<Box<dyn SubRecorder + Send>>,
    collectors: HashMap<u64, Box<dyn Collector>>,
    thread_rx: Receiver<LocalStorageRef>,
    thread_stores: HashMap<usize, LocalStorage>,
    last_collect: Instant,
    last_cleanup: Instant,
}

impl Recorder {
    // The main loop of recorder thread.
    fn run(&mut self) {
        let duration = 1_000.0 / RECORD_FREQUENCY * 1_000.0;
        loop {
            self.handle_pause();
            self.handle_collector_registration();
            self.handle_thread_registration();
            self.tick();
            self.cleanup();
            thread::sleep(Duration::from_micros(duration as _));
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
                for collector in self.collectors.values() {
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
                    assert!(retain || v.shared_ptr.take().is_none());
                    retain
                });
            }
            if self.records.records.capacity() > RECORD_LEN_THRESHOLD
                && self.records.records.len() < (RECORD_LEN_THRESHOLD / 2)
            {
                self.records.records.shrink_to(RECORD_LEN_THRESHOLD);
            }
            for r in &mut self.recorders {
                r.cleanup();
            }
            self.last_cleanup = Instant::now();
        }
    }

    fn reset(&mut self) {
        let now = Instant::now();
        self.records = RawRecords::default();
        self.last_collect = now;
        self.last_cleanup = now;
        for r in &mut self.recorders {
            r.reset();
        }
    }

    fn handle_pause(&mut self) {
        let mut should_reset = false;
        while self.pause.load(SeqCst) {
            thread::park();
            should_reset = true;
        }
        if should_reset {
            self.reset();
        }
    }

    fn handle_collector_registration(&mut self) {
        while let Ok(msg) = COLLECTOR_REG_CHAN.1.try_recv() {
            match msg {
                CollectorReg::Register { id, collector } => {
                    self.collectors.insert(id.0, collector);
                }
                CollectorReg::Unregister { id } => {
                    self.collectors.remove(&id.0);
                }
            }
        }
    }

    fn handle_thread_registration(&mut self) {
        while let Ok(lsr) = self.thread_rx.try_recv() {
            self.thread_stores.insert(lsr.id, lsr.storage.clone());
            for r in &mut self.recorders {
                r.thread_created(lsr.id, lsr.storage.shared_ptr.clone());
            }
        }
    }
}

/// Builder for [Recorder].
pub struct RecorderBuilder {
    enable: bool,
    precision_ms: Arc<AtomicU64>,
    recorders: Vec<Box<dyn SubRecorder + Send>>,
}

impl Default for RecorderBuilder {
    fn default() -> Self {
        Self {
            enable: false,
            precision_ms: Arc::new(AtomicU64::new(1000)),
            recorders: Vec::new(),
        }
    }
}

impl RecorderBuilder {
    /// Sets whether to start recorder.
    pub fn enable(mut self, enable: bool) -> Self {
        self.enable = enable;
        self
    }

    /// Sets the precision_ms parameter of [Recorder].
    pub fn precision_ms(mut self, precision_ms: Arc<AtomicU64>) -> Self {
        self.precision_ms = precision_ms;
        self
    }

    /// Add a [SubRecorder] for the execution of [Recorder].
    pub fn add_sub_recorder(mut self, r: Box<dyn SubRecorder + Send>) -> Self {
        self.recorders.push(r);
        self
    }

    /// Spawn a new thread that executes the main loop of the [Recorder].
    ///
    /// This function does not return the `Recorder` instance directly, instead
    /// it returns a [RecorderHandle] that is used to control the execution.
    pub fn spawn(self) -> io::Result<RecorderHandle> {
        let pause = Arc::new(AtomicBool::new(!self.enable));
        let precision_ms = self.precision_ms.clone();
        let (tx, rx) = unbounded();
        register_storage_chan_tx(tx);
        let now = Instant::now();
        let mut recorder = Recorder {
            pause: pause.clone(),
            precision_ms: precision_ms.clone(),
            records: RawRecords::default(),
            recorders: self.recorders,
            collectors: HashMap::default(),
            thread_rx: rx,
            thread_stores: HashMap::default(),
            last_collect: now,
            last_cleanup: now,
        };
        thread::Builder::new()
            .name("resource-metering-recorder".to_owned())
            .spawn(move || recorder.run())
            .map(move |h| RecorderHandle::new(h, pause, precision_ms))
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
    h: JoinHandle<()>,
    pause: Arc<AtomicBool>,
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
    pub fn new(h: JoinHandle<()>, pause: Arc<AtomicBool>, precision_ms: Arc<AtomicU64>) -> Self {
        Self {
            inner: Some(Arc::new(RecorderHandleInner {
                h,
                pause,
                precision_ms,
            })),
        }
    }

    /// Pause the execution of [Recorder].
    pub fn pause(&self) {
        if let Some(inner) = self.inner.as_ref() {
            inner.pause.store(true, SeqCst);
        }
    }

    /// Resume the execution of [Recorder].
    pub fn resume(&self) {
        if let Some(inner) = self.inner.as_ref() {
            inner.pause.store(false, SeqCst);
            inner.h.thread().unpark();
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

/// Constructs a default [Recorder], spawn it and return the corresponding [RecorderHandle].
///
/// This function is intended to simplify external use.
pub fn init_recorder(enable: bool, precision_ms: u64) -> RecorderHandle {
    RecorderBuilder::default()
        .enable(enable)
        .precision_ms(Arc::new(AtomicU64::new(precision_ms)))
        .add_sub_recorder(Box::new(CpuRecorder::default()))
        .spawn()
        .expect("failed to create resource metering thread")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::localstorage::STORAGE;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_recorder_handle() {
        let (tx, rx) = std::sync::mpsc::channel();
        let join_handle = std::thread::spawn(move || {
            thread::park();
            tx.send(true).unwrap();
        });
        let pause = Arc::new(AtomicBool::new(true));
        let precision_ms = Arc::new(AtomicU64::new(0));
        let handle = RecorderHandle::new(join_handle, pause.clone(), precision_ms.clone());
        let new_precision = Duration::from_secs(1);
        handle.precision(new_precision);
        assert_eq!(precision_ms.load(SeqCst), new_precision.as_millis() as u64);
        assert!(pause.load(SeqCst));
        handle.resume();
        assert!(!pause.load(SeqCst));
        assert!(rx.recv().unwrap());
        handle.pause();
        assert!(pause.load(SeqCst));
    }

    static OP_COUNT: AtomicUsize = AtomicUsize::new(0);

    struct MockSubRecorder;

    impl SubRecorder for MockSubRecorder {
        fn tick(
            &mut self,
            _records: &mut RawRecords,
            _thread_stores: &mut HashMap<usize, LocalStorage>,
        ) {
            OP_COUNT.fetch_add(1, SeqCst);
        }

        fn reset(&mut self) {
            OP_COUNT.fetch_add(1, SeqCst);
        }

        fn thread_created(&mut self, _id: usize, _tag: SharedTagPtr) {
            OP_COUNT.fetch_add(1, SeqCst);
        }
    }

    #[test]
    fn test_recorder() {
        let (tx, rx) = unbounded();
        register_storage_chan_tx(tx);
        std::thread::spawn(|| {
            STORAGE.with(|_| {});
        })
        .join()
        .unwrap();
        let now = Instant::now();
        let mut recorder = Recorder {
            pause: Arc::new(AtomicBool::new(false)),
            precision_ms: Arc::new(AtomicU64::new(1000)),
            records: RawRecords::default(),
            recorders: vec![Box::new(MockSubRecorder)],
            collectors: HashMap::default(),
            thread_rx: rx,
            thread_stores: HashMap::default(),
            last_collect: now,
            last_cleanup: now,
        };
        recorder.tick();
        recorder.reset();
        recorder.handle_thread_registration();
        assert!(!recorder.thread_stores.is_empty());
        assert!(OP_COUNT.load(SeqCst) >= 3);
    }
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::{CpuCollector, CpuRecorder};
use crate::localstorage::{register_storage_chan_sender, LocalStorage, LocalStorageRef};
use crate::reporter::Task;
use crate::summary::{SummaryCollector, SummaryRecorder};
use crate::{utils, SharedTagPtr};
use collections::HashMap;
use crossbeam::channel::{unbounded, Receiver};
use std::io;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tikv_util::time::Instant;
use tikv_util::worker::Scheduler;

const RECORD_FREQUENCY: f64 = 99.0;
const CLEANUP_INTERVAL_SECS: u64 = 1 * 60;

/// This trait defines a general framework that works at a certain frequency. Typically,
/// it describes the recorder(sampler) framework for a specific resource.
///
/// [Recorder] will maintain a list of sub-recorders, driving all sub-recorders to work
/// according to the behavior described in this trait.
pub trait SubRecorder {
    /// This function is called at a fixed frequency. (A typical frequency is 99hz.)
    ///
    /// The [LocalStorage] map of all threads will be passed in through parameters.
    ///
    /// The implementation needs to sample the resource in this function (in general).
    ///
    /// [LocalStorage]: crate::localstorage::LocalStorage
    fn tick(&mut self, _thread_stores: &mut HashMap<usize, LocalStorage>) {}

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
    thread_recv: Receiver<LocalStorageRef>,
    recorders: Vec<Box<dyn SubRecorder + Send>>,
    thread_stores: HashMap<usize, LocalStorage>,
    last_cleanup: Instant,
}

impl Recorder {
    // The main loop of recorder thread.
    fn run(&mut self) {
        let duration = 1_000.0 / RECORD_FREQUENCY * 1_000.0;
        loop {
            self.handle_pause();
            self.handle_thread_registration();
            self.tick();
            self.handle_cleanup_thread_stores();
            thread::sleep(Duration::from_micros(duration as _));
        }
    }

    fn tick(&mut self) {
        for r in &mut self.recorders {
            r.tick(&mut self.thread_stores);
        }
    }

    fn reset(&mut self) {
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

    fn handle_thread_registration(&mut self) {
        while let Ok(lsr) = self.thread_recv.try_recv() {
            self.thread_stores.insert(lsr.id, lsr.storage.clone());
            for r in &mut self.recorders {
                r.thread_created(lsr.id, lsr.storage.shared_ptr.clone());
            }
        }
    }

    fn handle_cleanup_thread_stores(&mut self) {
        if self.last_cleanup.saturating_elapsed().as_secs() > CLEANUP_INTERVAL_SECS {
            // Clean up the data of the destroyed threads.
            if let Some(ids) = utils::thread_ids() {
                self.thread_stores.retain(|k, v| {
                    let retain = ids.contains(k);
                    assert!(retain || v.shared_ptr.take().is_none());
                    retain
                });
            }
            self.last_cleanup = Instant::now();
        }
    }
}

/// Builder for [Recorder].
pub struct RecorderBuilder {
    precision_ms: Arc<AtomicU64>,
    recorders: Vec<Box<dyn SubRecorder + Send>>,
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
    pub fn add_sub_recorder(mut self, r: Box<dyn SubRecorder + Send>) -> Self {
        self.recorders.push(r);
        self
    }

    /// Spawn a new thread that executes the main loop of the [Recorder].
    ///
    /// This function does not return the `Recorder` instance directly, instead
    /// it returns a [RecorderHandle] that is used to control the execution.
    pub fn spawn(self) -> io::Result<RecorderHandle> {
        let pause = Arc::new(AtomicBool::new(true));
        let precision_ms = self.precision_ms.clone();
        let (sender, receiver) = unbounded();
        register_storage_chan_sender(sender);
        let mut recorder = Recorder {
            pause: pause.clone(),
            thread_recv: receiver,
            recorders: self.recorders,
            thread_stores: HashMap::default(),
            last_cleanup: Instant::now(),
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
pub fn build_default_recorder(scheduler: Scheduler<Task>) -> RecorderHandle {
    let precision_ms = Arc::new(AtomicU64::new(1000));
    RecorderBuilder::default()
        .precision_ms(precision_ms.clone())
        .add_sub_recorder(Box::new(CpuRecorder::new(
            CpuCollector::new(scheduler.clone()),
            precision_ms,
        )))
        .add_sub_recorder(Box::new(SummaryRecorder::new(SummaryCollector::new(
            scheduler,
        ))))
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
        let (sender, receiver) = std::sync::mpsc::channel();
        let join_handle = std::thread::spawn(move || {
            thread::park();
            sender.send(true).unwrap();
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
        assert!(receiver.recv().unwrap());
        handle.pause();
        assert!(pause.load(SeqCst));
    }

    static SUB_RECORDER_OP_COUNT: AtomicUsize = AtomicUsize::new(0);

    struct MockSubRecorder;

    impl SubRecorder for MockSubRecorder {
        fn tick(&mut self, _thread_stores: &mut HashMap<usize, LocalStorage>) {
            SUB_RECORDER_OP_COUNT.fetch_add(1, SeqCst);
        }

        fn reset(&mut self) {
            SUB_RECORDER_OP_COUNT.fetch_add(1, SeqCst);
        }

        fn thread_created(&mut self, _id: usize, _tag: SharedTagPtr) {
            SUB_RECORDER_OP_COUNT.fetch_add(1, SeqCst);
        }
    }

    #[test]
    fn test_recorder() {
        let (sender, receiver) = unbounded();
        register_storage_chan_sender(sender);
        std::thread::spawn(|| {
            STORAGE.with(|_| {});
        })
        .join()
        .unwrap();
        let mut recorder = Recorder {
            pause: Arc::new(AtomicBool::new(false)),
            thread_recv: receiver,
            recorders: vec![Box::new(MockSubRecorder)],
            thread_stores: HashMap::default(),
            last_cleanup: Instant::now(),
        };
        recorder.tick();
        recorder.reset();
        recorder.handle_thread_registration();
        assert!(recorder.thread_stores.len() >= 1);
        assert!(SUB_RECORDER_OP_COUNT.load(SeqCst) >= 3);
    }
}

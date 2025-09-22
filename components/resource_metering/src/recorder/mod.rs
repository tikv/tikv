// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    sync::{Arc, atomic::Ordering::Relaxed},
    time::Duration,
};

use collections::{HashMap, HashSet};
use tikv_util::{
    sys::thread::{self, Pid},
    time::Instant,
    warn,
    worker::{Builder as WorkerBuilder, LazyWorker, Runnable, RunnableWithTimer, Scheduler},
};

use self::{collector_reg::CollectorReg, sub_recorder::SubRecorder};
use crate::{
    Config, RawRecords, ResourceTagFactory, collector::Collector,
    config::ENABLE_NETWORK_IO_COLLECTION,
};
mod collector_reg;
mod localstorage;
mod sub_recorder;

pub use self::{
    collector_reg::{CollectorGuard, CollectorId, CollectorRegHandle},
    localstorage::{LocalStorage, LocalStorageRef, STORAGE},
    sub_recorder::{
        cpu::CpuRecorder,
        summary::{
            SummaryRecorder, record_logical_read_bytes, record_logical_write_bytes,
            record_network_in_bytes, record_network_out_bytes, record_read_keys, record_write_keys,
        },
    },
};

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
            Task::ThreadReg(lsr) => self.handle_thread_registration(lsr),
            Task::ConfigChange(cfg) => self.handle_config_change(cfg),
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
    precision_ms: u64,
    records: RawRecords,
    last_collect: Instant,
    last_cleanup: Instant,

    running: bool,
    recorders: Vec<Box<dyn SubRecorder>>,

    thread_stores: HashMap<Pid, LocalStorage>,

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

    fn handle_config_change(&mut self, config: Config) {
        self.precision_ms = config.precision.as_millis();
        ENABLE_NETWORK_IO_COLLECTION.store(config.enable_network_io_collection, Relaxed);
    }

    fn tick(&mut self) {
        for r in &mut self.recorders {
            r.tick(&mut self.records, &mut self.thread_stores);
        }
        let duration = self.last_collect.saturating_elapsed();
        if duration.as_millis() >= self.precision_ms as _ {
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
            if let Ok(ids) = thread::thread_ids::<HashSet<_>>(thread::process_id()) {
                self.thread_stores.retain(|k, v| {
                    let retain = ids.contains(&(*k as _));
                    debug_assert!(retain || v.attached_tag.swap(None).is_none());
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
    ThreadReg(LocalStorageRef),
    ConfigChange(Config),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::CollectorReg(_) => {
                write!(f, "CollectorReg")?;
            }
            Task::ThreadReg(_) => {
                write!(f, "NewThread")?;
            }
            Task::ConfigChange(_) => {
                write!(f, "ConfigChange")?;
            }
        }
        Ok(())
    }
}

/// Builder for [Recorder].
pub struct RecorderBuilder {
    precision_ms: u64,
    recorders: Vec<Box<dyn SubRecorder>>,
}

impl Default for RecorderBuilder {
    fn default() -> Self {
        Self {
            precision_ms: 1000,
            recorders: Vec::new(),
        }
    }
}

impl RecorderBuilder {
    /// Sets the precision_ms parameter of [Recorder].
    #[must_use]
    pub fn precision_ms(mut self, precision_ms: u64) -> Self {
        self.precision_ms = precision_ms;
        self
    }

    /// Add a [SubRecorder] for the execution of [Recorder].
    #[must_use]
    pub fn add_sub_recorder(mut self, r: Box<dyn SubRecorder>) -> Self {
        self.recorders.push(r);
        self
    }

    pub fn build(self) -> Recorder {
        let now = Instant::now();
        Recorder {
            precision_ms: self.precision_ms,
            records: RawRecords::default(),
            running: false,
            recorders: self.recorders,
            collectors: HashMap::default(),
            observers: HashMap::default(),
            thread_stores: HashMap::default(),
            last_collect: now,
            last_cleanup: now,
        }
    }
}

/// [ConfigChangeNotifier] for scheduling [Task::ConfigChange]
pub struct ConfigChangeNotifier {
    scheduler: Scheduler<Task>,
}

impl ConfigChangeNotifier {
    fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }

    pub fn notify(&self, config: Config) {
        if let Err(err) = self.scheduler.schedule(Task::ConfigChange(config)) {
            warn!("failed to schedule recorder::Task::ConfigChange"; "err" => ?err);
        }
    }
}

/// Constructs a default [Recorder], spawn it and return the corresponding
/// [ConfigChangeNotifier], [CollectorRegHandle], [ResourceTagFactory] and
/// [LazyWorker].
///
/// This function is intended to simplify external use.
pub fn init_recorder(
    precision_ms: u64,
) -> (
    ConfigChangeNotifier,
    CollectorRegHandle,
    ResourceTagFactory,
    Box<LazyWorker<Task>>,
) {
    let recorder = RecorderBuilder::default()
        .precision_ms(precision_ms)
        .add_sub_recorder(Box::<CpuRecorder>::default())
        .add_sub_recorder(Box::<SummaryRecorder>::default())
        .build();
    let mut recorder_worker = WorkerBuilder::new("resource-metering-recorder")
        .pending_capacity(256)
        .create()
        .lazy_build("resource-metering-recorder");

    let collector_reg_handle = CollectorRegHandle::new(recorder_worker.scheduler());
    let resource_tag_factory = ResourceTagFactory::new(recorder_worker.scheduler());
    let config_notifier = ConfigChangeNotifier::new(recorder_worker.scheduler());

    recorder_worker.start_with_timer(recorder);
    (
        config_notifier,
        collector_reg_handle,
        resource_tag_factory,
        Box::new(recorder_worker),
    )
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering::SeqCst},
        },
        thread::sleep,
    };

    use tikv_util::sys::thread::Pid;

    use super::*;
    use crate::{
        TagInfos,
        recorder::localstorage::{LocalStorage, LocalStorageRef},
    };

    #[derive(Clone, Default)]
    struct MockSubRecorder {
        tick_count: Arc<AtomicUsize>,
        resume_count: Arc<AtomicUsize>,
        thread_created_count: Arc<AtomicUsize>,
        pause_count: Arc<AtomicUsize>,
    }

    impl SubRecorder for MockSubRecorder {
        fn tick(
            &mut self,
            _records: &mut RawRecords,
            _thread_stores: &mut HashMap<Pid, LocalStorage>,
        ) {
            self.tick_count.fetch_add(1, SeqCst);
        }

        fn collect(
            &mut self,
            records: &mut RawRecords,
            _thread_stores: &mut HashMap<Pid, LocalStorage>,
        ) {
            let tag = Arc::new(TagInfos {
                store_id: 0,
                region_id: 0,
                peer_id: 0,
                key_ranges: vec![],
                extra_attachment: [1].to_vec().into(),
            });
            records.records.entry(tag).or_default().cpu_time = 2;
        }

        fn pause(
            &mut self,
            _records: &mut RawRecords,
            _thread_stores: &mut HashMap<Pid, LocalStorage>,
        ) {
            self.pause_count.fetch_add(1, SeqCst);
        }

        fn resume(
            &mut self,
            _records: &mut RawRecords,
            _thread_stores: &mut HashMap<Pid, LocalStorage>,
        ) {
            self.resume_count.fetch_add(1, SeqCst);
        }

        fn thread_created(&mut self, _id: Pid, _store: &LocalStorage) {
            self.thread_created_count.fetch_add(1, SeqCst);
        }
    }

    #[derive(Clone, Default)]
    struct MockCollector {
        records: Arc<Mutex<Option<Arc<RawRecords>>>>,
    }

    impl Collector for MockCollector {
        fn collect(&self, records: Arc<RawRecords>) {
            *self.records.lock().unwrap() = Some(records);
        }
    }

    #[test]
    fn test_recorder_basic() {
        let sub_recorder = MockSubRecorder::default();
        let mut recorder = RecorderBuilder::default()
            .precision_ms(20)
            .add_sub_recorder(Box::new(sub_recorder.clone()))
            .build();

        // register a new thread
        recorder.run(Task::ThreadReg(LocalStorageRef {
            id: 0,
            storage: LocalStorage::default(),
        }));
        recorder.on_timeout();
        assert!(!recorder.thread_stores.is_empty());
        assert_eq!(sub_recorder.pause_count.load(SeqCst), 0);
        assert_eq!(sub_recorder.resume_count.load(SeqCst), 0);
        assert_eq!(sub_recorder.tick_count.load(SeqCst), 0);
        assert_eq!(sub_recorder.thread_created_count.load(SeqCst), 1);

        // register a non-observed collector
        let collector = MockCollector::default();
        recorder.run(Task::CollectorReg(CollectorReg::Register {
            id: CollectorId(1),
            as_observer: false,
            collector: Box::new(collector.clone()),
        }));
        recorder.on_timeout();
        assert_eq!(sub_recorder.pause_count.load(SeqCst), 0);
        assert_eq!(sub_recorder.resume_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.tick_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.thread_created_count.load(SeqCst), 1);

        // trigger collection
        sleep(Duration::from_millis(recorder.precision_ms));
        recorder.on_timeout();
        assert_eq!(sub_recorder.pause_count.load(SeqCst), 0);
        assert_eq!(sub_recorder.resume_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.tick_count.load(SeqCst), 2);
        assert_eq!(sub_recorder.thread_created_count.load(SeqCst), 1);
        let records = { collector.records.lock().unwrap().take().unwrap() };
        assert_eq!(records.records.len(), 1);
        assert_eq!(
            &records.records.keys().next().unwrap().extra_attachment,
            &Arc::new([1].to_vec())
        );

        // deregister collector
        recorder.run(Task::CollectorReg(CollectorReg::Deregister {
            id: CollectorId(1),
        }));
        recorder.on_timeout();
        assert_eq!(sub_recorder.pause_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.resume_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.tick_count.load(SeqCst), 2);
        assert_eq!(sub_recorder.thread_created_count.load(SeqCst), 1);

        // nothing happens
        recorder.on_timeout();
        assert_eq!(sub_recorder.pause_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.resume_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.tick_count.load(SeqCst), 2);
        assert_eq!(sub_recorder.thread_created_count.load(SeqCst), 1);
    }

    #[test]
    fn test_recorder_multiple_collectors() {
        let sub_recorder = MockSubRecorder::default();
        let mut recorder = RecorderBuilder::default()
            .precision_ms(20)
            .add_sub_recorder(Box::new(sub_recorder.clone()))
            .build();

        // register 2 non-observed collectors and an observer
        let collector1 = MockCollector::default();
        let collector2 = MockCollector::default();
        let observer = MockCollector::default();
        recorder.run(Task::CollectorReg(CollectorReg::Register {
            id: CollectorId(1),
            as_observer: false,
            collector: Box::new(collector1.clone()),
        }));
        recorder.run(Task::CollectorReg(CollectorReg::Register {
            id: CollectorId(2),
            as_observer: false,
            collector: Box::new(collector2.clone()),
        }));
        recorder.run(Task::CollectorReg(CollectorReg::Register {
            id: CollectorId(3),
            as_observer: true,
            collector: Box::new(observer.clone()),
        }));
        recorder.on_timeout();
        assert_eq!(sub_recorder.pause_count.load(SeqCst), 0);
        assert_eq!(sub_recorder.resume_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.tick_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.thread_created_count.load(SeqCst), 0);

        // trigger collection
        sleep(Duration::from_millis(recorder.precision_ms));
        recorder.on_timeout();
        assert_eq!(sub_recorder.pause_count.load(SeqCst), 0);
        assert_eq!(sub_recorder.resume_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.tick_count.load(SeqCst), 2);
        assert_eq!(sub_recorder.thread_created_count.load(SeqCst), 0);
        let records = { collector1.records.lock().unwrap().take().unwrap() };
        assert_eq!(records.records.len(), 1);
        assert_eq!(
            &records.records.keys().next().unwrap().extra_attachment,
            &Arc::new([1].to_vec())
        );
        assert_eq!(records, {
            collector2.records.lock().unwrap().take().unwrap()
        });
        assert_eq!(records, {
            observer.records.lock().unwrap().take().unwrap()
        });

        // deregister all non-observed collectors
        recorder.run(Task::CollectorReg(CollectorReg::Deregister {
            id: CollectorId(1),
        }));
        recorder.run(Task::CollectorReg(CollectorReg::Deregister {
            id: CollectorId(2),
        }));
        recorder.on_timeout();
        assert_eq!(sub_recorder.pause_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.resume_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.tick_count.load(SeqCst), 2);
        assert_eq!(sub_recorder.thread_created_count.load(SeqCst), 0);

        // observer will not collect records
        sleep(Duration::from_millis(recorder.precision_ms));
        recorder.on_timeout();
        assert_eq!(sub_recorder.pause_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.resume_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.tick_count.load(SeqCst), 2);
        assert_eq!(sub_recorder.thread_created_count.load(SeqCst), 0);
        let records = { observer.records.lock().unwrap().take() };
        assert!(records.is_none());

        // reregister a non-observed collector
        recorder.run(Task::CollectorReg(CollectorReg::Register {
            id: CollectorId(4),
            as_observer: false,
            collector: Box::new(collector1.clone()),
        }));
        recorder.on_timeout();
        assert_eq!(sub_recorder.pause_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.resume_count.load(SeqCst), 2);
        assert_eq!(sub_recorder.tick_count.load(SeqCst), 3);
        assert_eq!(sub_recorder.thread_created_count.load(SeqCst), 0);

        // trigger collection
        sleep(Duration::from_millis(recorder.precision_ms));
        recorder.on_timeout();
        assert_eq!(sub_recorder.pause_count.load(SeqCst), 1);
        assert_eq!(sub_recorder.resume_count.load(SeqCst), 2);
        assert_eq!(sub_recorder.tick_count.load(SeqCst), 4);
        assert_eq!(sub_recorder.thread_created_count.load(SeqCst), 0);
        let records = { collector1.records.lock().unwrap().take().unwrap() };
        assert_eq!(records.records.len(), 1);
        assert_eq!(
            &records.records.keys().next().unwrap().extra_attachment,
            &Arc::new([1].to_vec())
        );
        assert_eq!(records, {
            observer.records.lock().unwrap().take().unwrap()
        });
    }
}

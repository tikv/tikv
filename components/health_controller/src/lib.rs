// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

pub mod reporters;
pub mod slow_score;
pub mod trend;
pub mod types;

use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use grpcio_health::HealthService;
use kvproto::pdpb::SlowTrend as SlowTrendPb;
use parking_lot::{Mutex, RwLock};
pub use types::{LatencyInspector, RaftstoreDuration};

struct ServingStatus {
    is_serving: bool,
    unhealthy_modules: HashSet<&'static str>,
}

impl ServingStatus {
    fn to_serving_status_pb(&self) -> grpcio_health::ServingStatus {
        match (self.is_serving, self.unhealthy_modules.is_empty()) {
            (true, true) => grpcio_health::ServingStatus::Serving,
            (true, false) => grpcio_health::ServingStatus::ServiceUnknown,
            (false, _) => grpcio_health::ServingStatus::NotServing,
        }
    }
}

struct HealthControllerInner {
    raftstore_slow_score: AtomicU64,
    raftstore_slow_trend: RollingRetriever<SlowTrendPb>,

    health_service: HealthService,
    current_serving_status: Mutex<ServingStatus>,
}

impl HealthControllerInner {
    fn new() -> Self {
        let health_service = HealthService::default();
        health_service.set_serving_status("", grpcio_health::ServingStatus::NotServing);
        Self {
            raftstore_slow_score: AtomicU64::new(1),
            raftstore_slow_trend: RollingRetriever::new(),

            health_service,
            current_serving_status: Mutex::new(ServingStatus {
                is_serving: false,
                unhealthy_modules: HashSet::default(),
            }),
        }
    }

    fn add_unhealthy_module(&self, module_name: &'static str) {
        let mut status = self.current_serving_status.lock();
        if !status.unhealthy_modules.insert(module_name) {
            // Nothing changed.
            return;
        }
        if status.unhealthy_modules.len() == 1 && status.is_serving {
            debug_assert_eq!(
                status.to_serving_status_pb(),
                grpcio_health::ServingStatus::ServiceUnknown
            );
            self.health_service
                .set_serving_status("", grpcio_health::ServingStatus::ServiceUnknown);
        }
    }

    fn remove_unhealthy_module(&self, module_name: &'static str) {
        let mut status = self.current_serving_status.lock();
        if !status.unhealthy_modules.remove(module_name) {
            // Nothing changed.
            return;
        }
        if status.unhealthy_modules.is_empty() && status.is_serving {
            debug_assert_eq!(
                status.to_serving_status_pb(),
                grpcio_health::ServingStatus::Serving
            );
            self.health_service
                .set_serving_status("", grpcio_health::ServingStatus::Serving);
        }
    }

    fn set_is_serving(&self, is_serving: bool) {
        let mut status = self.current_serving_status.lock();
        if is_serving == status.is_serving {
            // Nothing to do.
            return;
        }
        status.is_serving = is_serving;
        self.health_service
            .set_serving_status("", status.to_serving_status_pb());
    }

    fn get_serving_status(&self) -> grpcio_health::ServingStatus {
        let status = self.current_serving_status.lock();
        status.to_serving_status_pb()
    }

    fn update_raftstore_slow_score(&self, value: f64) {
        self.raftstore_slow_score
            .store(value.to_bits(), Ordering::Release);
    }

    fn get_raftstore_slow_score(&self) -> f64 {
        f64::from_bits(self.raftstore_slow_score.load(Ordering::Acquire))
    }

    fn update_raftstore_slow_trend(&self, slow_trend_pb: SlowTrendPb) {
        self.raftstore_slow_trend.put(slow_trend_pb);
    }

    fn get_raftstore_slow_trend(&self) -> SlowTrendPb {
        self.raftstore_slow_trend.get_cloned()
    }

    fn shutdown(&self) {
        self.health_service.shutdown();
    }
}

#[derive(Clone)]
pub struct HealthController {
    inner: Arc<HealthControllerInner>,
}

impl HealthController {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HealthControllerInner::new()),
        }
    }

    pub fn get_raftstore_slow_score(&self) -> f64 {
        self.inner.get_raftstore_slow_score()
    }

    pub fn get_raftstore_slow_trend(&self) -> SlowTrendPb {
        self.inner.get_raftstore_slow_trend()
    }

    pub fn get_grpc_health_service(&self) -> HealthService {
        self.inner.health_service.clone()
    }

    pub fn get_serving_status(&self) -> grpcio_health::ServingStatus {
        self.inner.get_serving_status()
    }

    pub fn set_is_serving(&self, is_serving: bool) {
        self.inner.set_is_serving(is_serving);
    }

    pub fn shutdown(&self) {
        self.inner.shutdown();
    }
}

// Make clippy happy.
impl Default for HealthControllerInner {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for HealthController {
    fn default() -> Self {
        Self::new()
    }
}

struct RollingRetriever<T> {
    content: [RwLock<T>; 2],
    current_index: AtomicUsize,
    write_mutex: Mutex<()>,
}

impl<T: Default> RollingRetriever<T> {
    pub fn new() -> Self {
        Self {
            content: [RwLock::new(T::default()), RwLock::new(T::default())],
            current_index: AtomicUsize::new(0),
            write_mutex: Mutex::new(()),
        }
    }
}

impl<T> RollingRetriever<T> {
    pub fn write<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let _write_guard = self.write_mutex.lock();
        // Update the item that is not the currently active one
        let index = self.current_index.load(Ordering::Acquire) ^ 1;

        let mut data_guard = self.content[index].write();
        let res = f(data_guard.deref_mut());

        drop(data_guard);
        self.current_index.store(index, Ordering::Release);
        res
    }

    pub fn put(&self, new_value: T) {
        self.write(|r| *r = new_value);
    }

    pub fn read<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let index = self.current_index.load(Ordering::Acquire);
        let guard = self.content[index].read();
        f(guard.deref())
    }
}

impl<T: Clone> RollingRetriever<T> {
    pub fn get_cloned(&self) -> T {
        self.read(|r| r.clone())
    }
}

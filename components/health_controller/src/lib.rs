// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

pub mod reporters;
pub mod slow_score;
pub mod trend;
pub mod types;

use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use grpcio_health::{HealthService, ServingStatus};
use kvproto::pdpb::SlowTrend as SlowTrendPb;
use parking_lot::{Mutex, RwLock};
use protobuf::ProtobufEnum;
pub use types::{LatencyInspector, RaftstoreDuration};

struct HealthControllerInner {
    raftstore_slow_score: AtomicU64,
    raftstore_slow_trend: RollingRetriever<SlowTrendPb>,

    health_service: HealthService,
    current_health_status: AtomicI32,
}

impl HealthControllerInner {
    fn get_server_health_status(&self) -> ServingStatus {
        ServingStatus::from_i32(self.current_health_status.load(Ordering::Acquire)).unwrap()
    }

    fn update_server_health_status(&self, status: ServingStatus) {
        self.current_health_status
            .store(status.value(), Ordering::Release);
        self.health_service.set_serving_status("", status);
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
}

pub struct HealthController {
    inner: Arc<HealthControllerInner>,
}

impl HealthController {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HealthControllerInner {
                raftstore_slow_score: AtomicU64::new(1),
                raftstore_slow_trend: RollingRetriever::new(),

                health_service: HealthService::default(),
                current_health_status: AtomicI32::new(ServingStatus::NotServing.value()),
            }),
        }
    }

    pub fn get_raftstore_slow_score(&self) -> f64 {
        self.inner.get_raftstore_slow_score()
    }

    pub fn get_raftstore_slow_trend(&self) -> SlowTrendPb {
        self.inner.get_raftstore_slow_trend()
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

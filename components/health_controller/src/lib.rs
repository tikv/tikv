// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

pub mod reporters;
pub mod slow_score;
pub mod trend;
pub mod types;

use std::sync::{
    atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering},
    Arc,
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
    pub fn get_server_health_status(&self) -> ServingStatus {
        ServingStatus::from_i32(self.current_health_status.load(Ordering::Acquire)).unwrap()
    }
    pub fn update_server_health_status(&self, status: ServingStatus) {
        self.current_health_status
            .store(status.value(), Ordering::Release);
        self.health_service.set_serving_status("", status);
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

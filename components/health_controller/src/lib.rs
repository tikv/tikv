// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

pub mod trend;
pub mod types;
pub mod slow_score;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use parking_lot::{Mutex, RwLock};

pub use types::{LatencyInspector, RaftstoreDuration};

struct HealthControllerInner {
}

pub struct HealthController {
    inner: Arc<HealthControllerInner>
}

struct RollingRetriever<T> {
    content: [RwLock<T>; 2],
    current_index: AtomicUsize,
    write_mutex: Mutex<()>,
}


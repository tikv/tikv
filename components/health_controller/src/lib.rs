// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains utilities to manage and retrieve the health status of
//! TiKV instance in a unified way.
//!
//! ## [`HealthController`]
//!
//! [`HealthController`] is the core of the module. It's a unified place where
//! the server's health status is managed and collected, including the [gRPC
//! `HealthService`](grpcio_health::HealthService). It provides interfaces to
//! retrieve the collected information, and actively setting whether
//! the gRPC `HealthService` should report a `Serving` or `NotServing` status.
//!
//! ## Reporters
//!
//! [`HealthController`] doesn't provide ways to update most of the states
//! directly. Instead, each module in TiKV tha need to report its health status
//! need to create a corresponding reporter.
//!
//! The reason why the reporters is split out from the `HealthController` is:
//!
//! * Reporters can have different designs to fit the special use patterns of
//!   different modules.
//! * `HealthController` internally contains states that are shared in different
//!   modules and threads. If some module need to store internal states to
//!   calculate the health status, they can be put in the reporter instead of
//!   the `HealthController`, which makes it possible to avoid unnecessary
//!   synchronization like mutexes.
//! * To avoid the `HealthController` itself contains too many different APIs
//!   that are specific to different modules, increasing the complexity and
//!   possibility to misuse of `HealthController`.

pub mod reporters;
pub mod slow_score;
pub mod trend;
pub mod types;

use std::{
    collections::HashSet,
    ops::Deref,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use futures::executor::block_on;
use kvproto::pdpb::SlowTrend as SlowTrendPb;
use parking_lot::{Mutex, RwLock};
use tonic_health::{
    server::{health_reporter, HealthReporter, HealthService},
    ServingStatus as GrpcServingStatus,
};
pub use types::{LatencyInspector, RaftstoreDuration};

struct ServingStatus {
    is_serving: bool,
    unhealthy_modules: HashSet<&'static str>,
}

impl ServingStatus {
    fn to_serving_status_pb(&self) -> GrpcServingStatus {
        match (self.is_serving, self.unhealthy_modules.is_empty()) {
            (true, true) => GrpcServingStatus::Serving,
            (true, false) => GrpcServingStatus::Unknown,
            (false, _) => GrpcServingStatus::NotServing,
        }
    }
}

struct HealthControllerInner {
    // Internally stores a `f64` type.
    raftstore_slow_score: AtomicU64,
    raftstore_slow_trend: RollingRetriever<SlowTrendPb>,

    /// gRPC's builtin `HealthService`.
    ///
    /// **Note**: DO NOT update its state directly. Only change its state while
    /// holding the mutex of `current_serving_status`, and keep consistent
    /// with value of `current_serving_status`, unless `health_service` is
    /// already shutdown.
    ///
    /// TiKV uses gRPC's builtin `HealthService` to provide information about
    /// whether the TiKV server is normally running. To keep its behavior
    /// consistent with earlier versions without the `HealthController`,
    /// it's used in such pattern:
    ///
    /// * Only an empty service name is used, representing the status of the
    ///   whole server.
    /// * When `current_serving_status.is_serving` is set to false (by calling
    ///   [`set_is_serving(false)`](HealthController::set_is_serving)), the
    ///   serving status is set to `NotServing`.
    /// * If `current_serving_status.is_serving` is true, but
    ///   `current_serving_status.unhealthy_modules` is not empty, the serving
    ///   status is set to `ServiceUnknown`.
    /// * Otherwise, the TiKV instance is regarded operational and the serving
    ///   status is set to `Serving`.
    health_service: HealthService,
    reporter: Mutex<HealthReporter>,
    current_serving_status: Mutex<ServingStatus>,
}

impl HealthControllerInner {
    fn new() -> Self {
        let (mut reporter, health_service) = health_reporter();
        let _ = block_on(reporter.set_service_status("", GrpcServingStatus::NotServing));

        Self {
            raftstore_slow_score: AtomicU64::new(f64::to_bits(1.0)),
            raftstore_slow_trend: RollingRetriever::new(),

            health_service,
            reporter: Mutex::new(reporter),
            current_serving_status: Mutex::new(ServingStatus {
                is_serving: false,
                unhealthy_modules: HashSet::default(),
            }),
        }
    }

    /// Marks a module (identified by name) to be unhealthy. Adding an unhealthy
    /// will make the serving status of the TiKV server, reported via the
    /// gRPC `HealthService`, to become `ServiceUnknown`.
    ///
    /// This is not an public API. This method is expected to be called only
    /// from reporters.
    fn add_unhealthy_module(&self, module_name: &'static str) {
        let mut status = self.current_serving_status.lock();
        if !status.unhealthy_modules.insert(module_name) {
            // Nothing changed.
            return;
        }
        if status.unhealthy_modules.len() == 1 && status.is_serving {
            debug_assert_eq!(status.to_serving_status_pb(), GrpcServingStatus::Unknown);
            let _ = block_on(
                self.reporter
                    .lock()
                    .set_service_status("", GrpcServingStatus::Unknown),
            );
        }
    }

    /// Removes a module (identified by name) that was marked unhealthy before.
    /// When the unhealthy modules are cleared, the serving status reported
    /// via the gRPC `HealthService` will change from `ServiceUnknown` to
    /// `Serving`.
    ///
    /// This is not an public API. This method is expected to be called only
    /// from reporters.
    fn remove_unhealthy_module(&self, module_name: &'static str) {
        let mut status = self.current_serving_status.lock();
        if !status.unhealthy_modules.remove(module_name) {
            // Nothing changed.
            return;
        }
        if status.unhealthy_modules.is_empty() && status.is_serving {
            debug_assert_eq!(status.to_serving_status_pb(), GrpcServingStatus::Serving);
            block_on(
                self.reporter
                    .lock()
                    .set_service_status("", GrpcServingStatus::Serving),
            );
        }
    }

    /// Sets whether the TiKV server is serving. This is currently used to pause
    /// the server, which has implementation in code but not commonly used.
    ///
    /// The effect of setting not serving overrides the effect of
    /// [`add_on_healthy_module`](Self::add_unhealthy_module).
    fn set_is_serving(&self, is_serving: bool) {
        let mut status = self.current_serving_status.lock();
        if is_serving == status.is_serving {
            // Nothing to do.
            return;
        }
        status.is_serving = is_serving;
        block_on(
            self.reporter
                .lock()
                .set_service_status("", status.to_serving_status_pb()),
        );
    }

    /// Gets the current serving status that is being reported by
    /// `health_service`, if it's not shutdown.
    fn get_serving_status(&self) -> GrpcServingStatus {
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

    /// Get the gRPC `HealthService`.
    ///
    /// Only use this when it's necessary to startup the gRPC server or for test
    /// purpose. Do not change the `HealthService`'s state manually.
    ///
    /// If it's necessary to update `HealthService`'s state, consider using
    /// [`set_is_serving`](Self::set_is_serving) or use a reporter to add an
    /// unhealthy module. An example:
    ///  [`RaftstoreReporter::set_is_healthy`](reporters::RaftstoreReporter::set_is_healthy).
    pub fn get_grpc_health_service(&self) -> HealthService {
        self.inner.health_service.clone()
    }

    pub fn get_serving_status(&self) -> GrpcServingStatus {
        self.inner.get_serving_status()
    }

    /// Set whether the TiKV server is serving. This controls the state reported
    /// by the gRPC `HealthService`.
    pub fn set_is_serving(&self, is_serving: bool) {
        self.inner.set_is_serving(is_serving);
    }

    pub fn shutdown(&self) {}
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

/// An alternative util to simple RwLock. It allows writing not blocking
/// reading, at the expense of linearizability between reads and writes.
///
/// This is suitable for use cases where atomic storing and loading is expected,
/// but atomic variables is not applicable due to the inner type larger than 8
/// bytes. When writing is in progress, readings will get the previous value.
/// Writes will block each other, and fast and frequent writes may also block or
/// be blocked by slow reads.
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
    #[inline]
    pub fn put(&self, new_value: T) {
        self.put_with(|| new_value)
    }

    fn put_with(&self, f: impl FnOnce() -> T) {
        let _write_guard = self.write_mutex.lock();
        // Update the item that is not the currently active one
        let index = self.current_index.load(Ordering::Acquire) ^ 1;

        let mut data_guard = self.content[index].write();
        *data_guard = f();

        drop(data_guard);
        self.current_index.store(index, Ordering::Release);
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

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc::{sync_channel, RecvTimeoutError},
        time::Duration,
    };

    use super::*;

    #[test]
    fn test_health_controller_update_service_status() {
        let h = HealthController::new();

        // Initial value of slow score
        assert_eq!(h.get_raftstore_slow_score(), 1.0);

        assert_eq!(
            h.get_serving_status(),
            grpcio_health::ServingStatus::NotServing
        );

        h.set_is_serving(true);
        assert_eq!(
            h.get_serving_status(),
            grpcio_health::ServingStatus::Serving
        );

        h.inner.add_unhealthy_module("A");
        assert_eq!(
            h.get_serving_status(),
            grpcio_health::ServingStatus::ServiceUnknown
        );
        h.inner.add_unhealthy_module("B");
        assert_eq!(
            h.get_serving_status(),
            grpcio_health::ServingStatus::ServiceUnknown
        );

        h.inner.remove_unhealthy_module("A");
        assert_eq!(
            h.get_serving_status(),
            grpcio_health::ServingStatus::ServiceUnknown
        );
        h.inner.remove_unhealthy_module("B");
        assert_eq!(
            h.get_serving_status(),
            grpcio_health::ServingStatus::Serving
        );

        h.set_is_serving(false);
        assert_eq!(
            h.get_serving_status(),
            grpcio_health::ServingStatus::NotServing
        );
        h.inner.add_unhealthy_module("A");
        assert_eq!(
            h.get_serving_status(),
            grpcio_health::ServingStatus::NotServing
        );

        h.set_is_serving(true);
        assert_eq!(
            h.get_serving_status(),
            grpcio_health::ServingStatus::ServiceUnknown
        );

        h.inner.remove_unhealthy_module("A");
        assert_eq!(
            h.get_serving_status(),
            grpcio_health::ServingStatus::Serving
        );
    }

    #[test]
    fn test_rolling_retriever() {
        let r = Arc::new(RollingRetriever::<u64>::new());
        assert_eq!(r.get_cloned(), 0);

        for i in 1..=10 {
            r.put(i);
            assert_eq!(r.get_cloned(), i);
        }

        // Writing doesn't block reading.
        let r1 = r.clone();
        let (write_continue_tx, rx) = sync_channel(0);
        let write_handle = std::thread::spawn(move || {
            r1.put_with(move || {
                rx.recv().unwrap();
                11
            })
        });
        for _ in 1..10 {
            std::thread::sleep(Duration::from_millis(5));
            assert_eq!(r.get_cloned(), 10)
        }
        write_continue_tx.send(()).unwrap();
        write_handle.join().unwrap();
        assert_eq!(r.get_cloned(), 11);

        // Writing block each other.
        let r1 = r.clone();
        let (write1_tx, rx1) = sync_channel(0);
        let write1_handle = std::thread::spawn(move || {
            r1.put_with(move || {
                // Receive once for notifying lock acquired.
                rx1.recv().unwrap();
                // Receive again to be notified ready to continue.
                rx1.recv().unwrap();
                12
            })
        });
        write1_tx.send(()).unwrap();
        let r1 = r.clone();
        let (write2_tx, rx2) = sync_channel(0);
        let write2_handle = std::thread::spawn(move || {
            r1.put_with(move || {
                write2_tx.send(()).unwrap();
                13
            })
        });
        // Write 2 cannot continue as blocked by write 1.
        assert_eq!(
            rx2.recv_timeout(Duration::from_millis(50)).unwrap_err(),
            RecvTimeoutError::Timeout
        );
        // Continue write1
        write1_tx.send(()).unwrap();
        write1_handle.join().unwrap();
        assert_eq!(r.get_cloned(), 12);
        // Continue write2
        rx2.recv().unwrap();
        write2_handle.join().unwrap();
        assert_eq!(r.get_cloned(), 13);
    }
}

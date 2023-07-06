use std::sync::{atomic::Ordering, Arc};

use atomic::Atomic;
use crossbeam::channel::SendError;
use tikv_util::mpsc;

use crate::service_event::ServiceEvent;

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq)]
enum GrpcServiceStatus {
    Init,
    Serving,
    NotServing,
}

#[derive(Clone)]
pub struct GrpcServiceManager {
    status: Arc<Atomic<GrpcServiceStatus>>,
    service_router: mpsc::Sender<ServiceEvent>,
}

impl GrpcServiceManager {
    fn build(router: mpsc::Sender<ServiceEvent>, status: GrpcServiceStatus) -> Self {
        Self {
            status: Arc::new(Atomic::new(status)),
            service_router: router,
        }
    }

    /// Generate a formal GrpcServiceManager.
    pub fn new(router: mpsc::Sender<ServiceEvent>) -> Self {
        Self::build(router, GrpcServiceStatus::Serving)
    }

    /// Only for test.
    /// Generate a dummy GrpcServiceManager.
    pub fn dummy() -> Self {
        let (router, _) = mpsc::unbounded();
        Self::build(router, GrpcServiceStatus::Init)
    }

    /// Send message to outer handler to notify PAUSE grpc server.
    pub fn pause(&mut self) -> Result<(), SendError<ServiceEvent>> {
        if self.is_paused() {
            // Already in PAUSE.
            return Ok(());
        }
        let result = self.service_router.send(ServiceEvent::PauseGrpc);
        if result.is_ok() {
            self.status
                .store(GrpcServiceStatus::NotServing, Ordering::Relaxed);
        }
        result
    }

    /// Send message to outer handler to notify RESUME grpc server.
    pub fn resume(&mut self) -> Result<(), SendError<ServiceEvent>> {
        if self.is_serving() {
            // Already in RESUME.
            return Ok(());
        }
        let result = self.service_router.send(ServiceEvent::ResumeGrpc);
        if result.is_ok() {
            self.status
                .store(GrpcServiceStatus::Serving, Ordering::Relaxed);
        }
        result
    }

    #[inline]
    pub fn is_paused(&self) -> bool {
        self.status.load(Ordering::Relaxed) == GrpcServiceStatus::NotServing
    }

    #[inline]
    fn is_serving(&self) -> bool {
        self.status.load(Ordering::Relaxed) == GrpcServiceStatus::Serving
    }
}

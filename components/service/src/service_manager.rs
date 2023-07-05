use std::sync::{atomic::Ordering, Arc};

use atomic::Atomic;
use crossbeam::channel::SendError;
use tikv_util::mpsc;

use crate::service_event::ServiceEvent;

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq)]
enum GrpcServiceStatus {
    Serving,
    NotServing,
}

#[derive(Clone)]
pub struct GrpcServiceManager {
    status: Arc<Atomic<GrpcServiceStatus>>,
    service_router: mpsc::Sender<ServiceEvent>,
}

impl GrpcServiceManager {
    pub fn new(router: mpsc::Sender<ServiceEvent>) -> Self {
        Self {
            status: Arc::new(Atomic::new(GrpcServiceStatus::Serving)),
            service_router: router,
        }
    }

    /// Only for test.
    /// Generate a dummy GrpcServiceManager.
    pub fn dummy() -> Self {
        let (sender, _) = mpsc::unbounded();
        GrpcServiceManager::new(sender)
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
        if self.status.load(Ordering::Relaxed) == GrpcServiceStatus::Serving {
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
}

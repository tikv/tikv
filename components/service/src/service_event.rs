// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;

/// Service Status enum
pub enum ServiceEvent {
    // For grpc service.
    PauseGrpc,
    ResumeGrpc,
    // For graceful shutdown.
    GracefulShutdown,
    Exit,
}

impl fmt::Debug for ServiceEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceEvent::PauseGrpc => f.debug_tuple("PauseGrpc").finish(),
            ServiceEvent::ResumeGrpc => f.debug_tuple("ResumeGrpc").finish(),
            ServiceEvent::GracefulShutdown => f.debug_tuple("GracefulShutdown").finish(),
            ServiceEvent::Exit => f.debug_tuple("Exit").finish(),
        }
    }
}

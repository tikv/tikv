// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use grpcio_health::proto::HealthCheckResponse;

use super::Result;

pub trait HealthClient: Send + Sync {
    fn check(&self) -> Result<HealthCheckResponse>;
}

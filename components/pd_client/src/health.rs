// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use grpcio_health::{HealthCheckRequest,HealthCheckResponse};
use grpcio::Result;

pub trait HealthClient {
    fn check(
        &self,
        req: &HealthCheckRequest,
    ) -> Result<HealthCheckResponse>;
}
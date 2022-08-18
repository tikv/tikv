// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use file_system::{get_io_rate_limiter, get_io_type, IoOp, IoRateLimiter};

use crate::Result;

pub trait FileSystemInspector: Sync + Send {
    fn read(&self, len: usize) -> Result<usize>;
    fn write(&self, len: usize) -> Result<usize>;
}

pub struct EngineFileSystemInspector {
    limiter: Option<Arc<IoRateLimiter>>,
}

impl EngineFileSystemInspector {
    #[allow(dead_code)]
    pub fn new() -> Self {
        EngineFileSystemInspector {
            limiter: get_io_rate_limiter(),
        }
    }

    pub fn from_limiter(limiter: Option<Arc<IoRateLimiter>>) -> Self {
        EngineFileSystemInspector { limiter }
    }
}

impl Default for EngineFileSystemInspector {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystemInspector for EngineFileSystemInspector {
    fn read(&self, len: usize) -> Result<usize> {
        if let Some(limiter) = &self.limiter {
            let io_type = get_io_type();
            Ok(limiter.request(io_type, IoOp::Read, len))
        } else {
            Ok(len)
        }
    }

    fn write(&self, len: usize) -> Result<usize> {
        if let Some(limiter) = &self.limiter {
            let io_type = get_io_type();
            Ok(limiter.request(io_type, IoOp::Write, len))
        } else {
            Ok(len)
        }
    }
}

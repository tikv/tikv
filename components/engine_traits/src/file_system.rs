// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use file_system::{get_io_rate_limiter, IOOp, IORateLimiter};

pub trait FileSystemInspector: Sync + Send {
    fn read(&self, len: usize) -> Result<usize, String>;
    fn write(&self, len: usize) -> Result<usize, String>;
}

pub struct EngineFileSystemInspector {
    limiter: Option<Arc<IORateLimiter>>,
}

impl EngineFileSystemInspector {
    #[allow(dead_code)]
    pub fn new() -> Self {
        EngineFileSystemInspector {
            limiter: get_io_rate_limiter(),
        }
    }

    pub fn from_limiter(limiter: Option<Arc<IORateLimiter>>) -> Self {
        EngineFileSystemInspector { limiter }
    }
}

impl Default for EngineFileSystemInspector {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystemInspector for EngineFileSystemInspector {
    fn read(&self, len: usize) -> Result<usize, String> {
        if let Some(limiter) = &self.limiter {
            Ok(limiter.request(IOOp::Read, len))
        } else {
            Ok(len)
        }
    }

    fn write(&self, len: usize) -> Result<usize, String> {
        if let Some(limiter) = &self.limiter {
            Ok(limiter.request(IOOp::Write, len))
        } else {
            Ok(len)
        }
    }
}

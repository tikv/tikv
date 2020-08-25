// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use futures::sync::oneshot;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Full {
    pub current_tasks: usize,
    pub max_tasks: usize,
}

impl std::fmt::Display for Full {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "future pool is full")
    }
}

impl std::error::Error for Full {
    fn description(&self) -> &str {
        "future pool is full"
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum ReadPoolError {
        FuturePoolFull(err: Full) {
            from()
            cause(err)
            display("{}", err)
        }
        UnifiedReadPoolFull {
            display("Unified read pool is full")
        }
        Canceled(err: oneshot::Canceled) {
            from()
            cause(err)
            display("{}", err)
        }
    }
}

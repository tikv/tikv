// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pd;

pub use raftstore2::errors::{Error, Result, DiscardReason, RAFTSTORE_IS_BUSY};

quick_error! {
    #[derive(Debug)]
    pub enum Error2 {
        Inner(err: raftstore2::errors::Error) {
            from()
            cause(err)
            description(err.description())
            display("Raftstore {}", err)
        }
        Pd(err: pd::Error) {
            from()
            cause(err)
            description(err.description())
            display("Pd {}", err)
        }
    }
}

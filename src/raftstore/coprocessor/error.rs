// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error as StdError;
use std::result::Result as StdResult;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<dyn StdError + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{}", err)
        }
    }
}

pub type Result<T> = StdResult<T, Error>;

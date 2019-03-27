// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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

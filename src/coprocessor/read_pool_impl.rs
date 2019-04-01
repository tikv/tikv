// Copyright 2019 PingCAP, Inc.
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

use crate::pd::PdTask;
use crate::server::readpool::{self, Builder, ReadPool};
use crate::util::worker::FutureScheduler;

pub struct ReadPoolImpl;

impl ReadPoolImpl {
    pub fn build_read_pool(
        config: &readpool::Config,
        _pd_sender: FutureScheduler<PdTask>,
    ) -> ReadPool {
        Builder::from_config(config).name_prefix("cop").build()
        // TODO: Storage metrics flush
    }
}

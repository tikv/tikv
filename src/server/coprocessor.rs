// Copyright 2016 PingCAP, Inc.
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

use std::sync::Arc;
use mio::Token;
use storage::{Engine, SnapshotStore};
use kvproto::kvrpcpb::Context;
use kvproto::coprocessor::Request;
use super::{Result, SendCh};

#[allow(dead_code)]
pub struct SnapshotCoprocessor {
    engine: Arc<Box<Engine>>,
    ch: SendCh,
}

impl SnapshotCoprocessor {
    pub fn new(engine: Arc<Box<Engine>>, ch: SendCh) -> SnapshotCoprocessor {
        // TODO: Spawn a new thread for handling requests asynchronously.
        SnapshotCoprocessor {
            engine: engine,
            ch: ch,
        }
    }

    #[allow(unused_variables)]
    pub fn on_request(&self, req: Request, token: Token, msg_id: u64) -> Result<()> {
        unimplemented!();
    }

    #[allow(dead_code)]
    fn new_snapshot<'a>(&'a self, ctx: &Context, start_ts: u64) -> Result<SnapshotStore<'a>> {
        let snapshot = try!(self.engine.snapshot(ctx));
        Ok(SnapshotStore::new(snapshot, start_ts))
    }
}

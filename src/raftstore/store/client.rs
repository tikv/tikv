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

use std::boxed::{Box, FnBox};

use kvproto::msgpb::Message;

use raftstore::Result;

// Sends messages to specified tikv store and call callback on responses.

pub type Callback = Box<FnBox(Result<Message>) + Send>;

// TikvClient sends requests to tikv and receives responses.
pub trait Client: Send + Clone {
    fn send(&self, store_id: u64, msg: Message, cb: Callback);
}

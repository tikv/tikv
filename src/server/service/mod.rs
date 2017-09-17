// Copyright 2017 PingCAP, Inc.
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

use std::boxed::FnBox;
use std::fmt::Debug;
use futures::sync::oneshot;

mod kv;
mod debug;

pub use self::kv::Service as KvService;
pub use self::debug::{Request as DebugTask, Runner as DebugRunner, Service as DebugService};

fn make_callback<T: Debug + Send + 'static>() -> (Box<FnBox(T) + Send>, oneshot::Receiver<T>) {
    let (tx, rx) = oneshot::channel();
    let callback = move |resp| { tx.send(resp).unwrap(); };
    (box callback, rx)
}

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

use std::fmt::{self, Formatter, Display};
use std::{io, fs};
use std::boxed::{Box, FnBox};

use super::{Result, Error};
use util::worker::{Runnable, Worker};
use bytes::ByteBuf;

pub type Callback = Box<FnBox(Result<u64>) + Send>;

// TODO make it zero copy
struct Task {
    buf: ByteBuf,
    cb: Callback,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Snapshot File Receiver Task")
    }
}

struct Runner {
    file: fs::File,
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        let mut buf = task.buf;
        let resp = io::copy(&mut buf, &mut self.file);
        task.cb.call_box((resp.map_err(|e| Error::Io(e)),))
    }
}

struct SnapshotReceiver {
    worker: Worker<Task>,
    buf: ByteBuf,
    more: bool,
}

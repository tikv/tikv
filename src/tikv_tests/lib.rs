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

#![feature(box_syntax)]

#[macro_use]
extern crate tikv;
extern crate futures;
extern crate grpcio as grpc;
extern crate kvproto;
extern crate protobuf;
extern crate raft;
extern crate rand;
extern crate rocksdb;
extern crate tempdir;
extern crate tokio_timer;
#[macro_use]
extern crate log;
#[macro_use(slog_kv, slog_o)]
extern crate slog;
extern crate time;

pub mod raftstore;
pub mod storage;
pub mod util;

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

use std::io;
use std::result;
use std::path::PathBuf;

use grpc;
use uuid::{ParseError, Uuid};
use futures::sync::oneshot::Canceled;

use kvproto::errorpb;

use pd;
use storage;
use util::codec;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Grpc(err: grpc::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Uuid(err: ParseError) {
            from()
            cause(err)
            description(err.description())
        }
        Future(err: Canceled) {
            from()
            cause(err)
        }
        RocksDB(msg: String) {
            from()
            display("RocksDB {}", msg)
        }
        Codec(err: codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Storage(err: storage::Error) {
            from()
            cause(err)
            description(err.description())
        }
        PdRPC(err: pd::Error) {
            from()
            cause(err)
            description(err.description())
        }
        TikvRPC(err: errorpb::Error) {
            display("TikvRPC {:?}", err)
        }
        SplitRegion(err: errorpb::Error) {
            display("SplitRegion {:?}", err)
        }
        FileExists(path: PathBuf) {
            display("File {} exists", path.to_str().unwrap())
        }
        FileCorrupted(path: PathBuf) {
            display("File {} corrupted", path.to_str().unwrap())
        }
        FileNotExists(path: PathBuf) {
            display("File {} not exists", path.to_str().unwrap())
        }
        TokenNotFound(token: usize) {
            display("Token {} not found", token)
        }
        EngineInUse(uuid: Uuid) {
            display("Engine {} is in use", uuid)
        }
        EngineNotFound(uuid: Uuid) {
            display("Engine {} not found", uuid)
        }
        Timeout {}
        SSTFileOutOfRange {}
    }
}

pub type Result<T> = result::Result<T, Error>;

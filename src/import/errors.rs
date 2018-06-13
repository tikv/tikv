// Copyright 2018 PingCAP, Inc.
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

use std::io::Error as IoError;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::result;

use futures::sync::oneshot::Canceled;
use grpc::Error as GrpcError;
use uuid::{ParseError, Uuid};

use raftstore::errors::Error as RaftStoreError;
use util::codec::Error as CodecError;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        Grpc(err: GrpcError) {
            from()
            cause(err)
            description(err.description())
        }
        Uuid(err: ParseError) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: CodecError) {
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
        RaftStore(err: RaftStoreError) {
            from()
            cause(err)
            description(err.description())
        }
        ParseIntError(err: ParseIntError) {
            from()
            cause(err)
            description(err.description())
        }
        FileExists(path: PathBuf) {
            display("File {:?} exists", path)
        }
        FileNotExists(path: PathBuf) {
            display("File {:?} not exists", path)
        }
        FileCorrupted(path: PathBuf, reason: String) {
            display("File {:?} corrupted: {}", path, reason)
        }
        InvalidSSTPath(path: PathBuf) {
            display("Invalid SST path {:?}", path)
        }
        EngineInUse(uuid: Uuid) {
            display("Engine {} is in use", uuid)
        }
        EngineNotFound(uuid: Uuid) {
            display("Engine {} not found", uuid)
        }
        InvalidProtoMessage(reason: String) {
            display("Invalid proto message {}", reason)
        }
        InvalidChunk {}
    }
}

pub type Result<T> = result::Result<T, Error>;

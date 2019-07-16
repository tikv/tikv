// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Error as IoError;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::result;

use futures::sync::oneshot::Canceled;
use grpcio::Error as GrpcError;
use uuid::ParseError;

use crate::raftstore::errors::Error as RaftStoreError;

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
        Future(err: Canceled) {
            from()
            cause(err)
        }
        RocksDB(msg: String) {
            from()
            display("RocksDB {}", msg)
        }
        Engine(err: engine::Error) {
            from()
            description("Engine error")
            display("Engine {:?}", err)
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
        FileCorrupted(path: PathBuf, reason: String) {
            display("File {:?} corrupted: {}", path, reason)
        }
        InvalidSSTPath(path: PathBuf) {
            display("Invalid SST path {:?}", path)
        }
        InvalidChunk {}
    }
}

pub type Result<T> = result::Result<T, Error>;

// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Error as IoError;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::result;

use crate::grpc::Error as GrpcError;
use futures::sync::oneshot::Canceled;
use kvproto::errorpb;
use kvproto::metapb::*;
use uuid::{ParseError, Uuid};

use crate::pd::{Error as PdError, RegionInfo};
use crate::raftstore::errors::Error as RaftStoreError;
use crate::util::codec::Error as CodecError;

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
        PdRPC(err: PdError) {
            from()
            cause(err)
            description(err.description())
        }
        TikvRPC(err: errorpb::Error) {
            display("TikvRPC {:?}", err)
        }
        NotLeader(new_leader: Option<Peer>) {}
        EpochNotMatch(current_regions: Vec<Region>) {}
        UpdateRegion(new_region: RegionInfo) {}
        ImportJobFailed(tag: String) {
            display("{}", tag)
        }
        ImportSSTJobFailed(tag: String) {
            display("{}", tag)
        }
        PrepareRangeJobFailed(tag: String) {
            display("{}", tag)
        }
        ResourceTemporarilyUnavailable(msg: String) {
            display("{}", msg)
        }
    }
}

impl From<errorpb::Error> for Error {
    fn from(mut err: errorpb::Error) -> Self {
        if err.has_not_leader() {
            let mut error = err.take_not_leader();
            if error.has_leader() {
                Error::NotLeader(Some(error.take_leader()))
            } else {
                Error::NotLeader(None)
            }
        } else if err.has_epoch_not_match() {
            let mut error = err.take_epoch_not_match();
            Error::EpochNotMatch(error.take_current_regions().to_vec())
        } else {
            Error::TikvRPC(err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

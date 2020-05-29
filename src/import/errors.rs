// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Error as IoError;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::result;

use futures::sync::oneshot::Canceled;
use grpcio::Error as GrpcError;
use kvproto::errorpb;
use kvproto::import_sstpb;
use kvproto::metapb::*;
use uuid::{ParseError, Uuid};

use crate::pd::{Error as PdError, RegionInfo};
use crate::raftstore::errors::Error as RaftStoreError;
use crate::storage::mvcc::Error as MvccError;
use tikv_util::codec::Error as CodecError;

use super::metrics::*;

pub fn error_inc(err: &Error) {
    let label = match err {
        Error::Io(..) => "io",
        Error::Grpc(..) => "grpc",
        Error::Uuid(..) => "uuid",
        Error::RocksDB(..) => "rocksdb",
        Error::ParseIntError(..) => "parse_int",
        Error::FileExists(..) => "file_exists",
        Error::FileCorrupted(..) => "file_corrupt",
        Error::InvalidSSTPath(..) => "invalid_sst",
        Error::Engine(..) => "engine",
        Error::CannotReadExternalStorage(..) => "read_external_storage",
        Error::WrongKeyPrefix(..) => "wrong_prefix",
        _ => return,
    };
    IMPORTER_ERROR_VEC.with_label_values(&[label]).inc();
}

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
        MvccError(err: MvccError) {
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
        CannotReadExternalStorage(url: String, name: String, local_path: PathBuf, err: IoError) {
            cause(err)
            display("Cannot read {}/{} into {}: {}", url, name, local_path.display(), err)
        }
        WrongKeyPrefix(what: &'static str, key: Vec<u8>, prefix: Vec<u8>) {
            display("\
                {} has wrong prefix: key {} does not start with {}",
                what,
                hex::encode_upper(&key),
                hex::encode_upper(&prefix),
            )
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

impl From<Error> for import_sstpb::Error {
    fn from(e: Error) -> import_sstpb::Error {
        let mut err = import_sstpb::Error::default();
        err.set_message(format!("{}", e));
        err
    }
}

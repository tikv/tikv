// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error as StdError;
use std::io::Error as IoError;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::result;

use encryption::Error as EncryptionError;
use grpcio::Error as GrpcError;
use kvproto::import_sstpb;
use tokio_sync::oneshot::error::RecvError;
use uuid::Error as UuidError;

use crate::metrics::*;

pub fn error_inc(err: &Error) {
    let label = match err {
        Error::Io(..) => "io",
        Error::Grpc(..) => "grpc",
        Error::Uuid(..) => "uuid",
        Error::RocksDB(..) => "rocksdb",
        Error::EngineTraits(..) => "engine_traits",
        Error::ParseIntError(..) => "parse_int",
        Error::FileExists(..) => "file_exists",
        Error::FileCorrupted(..) => "file_corrupt",
        Error::InvalidSSTPath(..) => "invalid_sst",
        Error::Engine(..) => "engine",
        Error::CannotReadExternalStorage(..) => "read_external_storage",
        Error::WrongKeyPrefix(..) => "wrong_prefix",
        Error::BadFormat(..) => "bad_format",
        Error::Encryption(..) => "encryption",
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
        Uuid(err: UuidError) {
            from()
            cause(err)
            description(err.description())
        }
        Future(err: RecvError) {
            from()
            cause(err)
        }
        // FIXME: Remove concrete 'rocks' type
        RocksDB(msg: String) {
            from()
            display("RocksDB {}", msg)
        }
        EngineTraits(err: engine_traits::Error) {
            from()
            description("Engine error")
            display("Engine {:?}", err)
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
        Engine(err: Box<dyn StdError + Send + Sync + 'static>) {
            display("{}", err)
        }
        CannotReadExternalStorage(url: String, name: String, err: IoError) {
            cause(err)
            display("Cannot read {}/{}", url, name)
        }
        WrongKeyPrefix(what: &'static str, key: Vec<u8>, prefix: Vec<u8>) {
            display("\
                {} has wrong prefix: key {} does not start with {}",
                what,
                hex::encode_upper(&key),
                hex::encode_upper(&prefix),
            )
        }
        BadFormat(msg: String) {
            display("bad format {}", msg)
        }
        Encryption(err: EncryptionError) {
            from()
            description("encryption error")
            display("Encryption {:?}", err)
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

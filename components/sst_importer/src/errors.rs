// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error as StdError;
use std::io::Error as IoError;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::result;

use encryption::Error as EncryptionError;
use error_code::{self, ErrorCode, ErrorCodeExt};
use futures::channel::oneshot::Canceled;
use grpcio::Error as GrpcError;
use kvproto::import_sstpb;
use tikv_util::codec::Error as CodecError;
use uuid::Error as UuidError;

use crate::metrics::*;

pub fn error_inc(type_: &str, err: &Error) {
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
        Error::CodecError(..) => "codec",
        _ => return,
    };
    IMPORTER_ERROR_VEC.with_label_values(&[type_, label]).inc();
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: IoError) {
            from()
            cause(err)
            display("{}", err)
        }
        Grpc(err: GrpcError) {
            from()
            cause(err)
            display("{}", err)
        }
        Uuid(err: UuidError) {
            from()
            cause(err)
            display("{}", err)
        }
        Future(err: Canceled) {
            from()
            cause(err)
            display("{}", err)
        }
        // FIXME: Remove concrete 'rocks' type
        RocksDB(msg: String) {
            from()
            display("RocksDB {}", msg)
        }
        EngineTraits(err: engine_traits::Error) {
            from()
            display("Engine {:?}", err)
        }
        ParseIntError(err: ParseIntError) {
            from()
            cause(err)
            display("{}", err)
        }
        FileExists(path: PathBuf, action: &'static str) {
            display("File {:?} exists, cannot {}", path, action)
        }
        FileCorrupted(path: PathBuf, reason: String) {
            display("File {:?} corrupted: {}", path, reason)
        }
        InvalidSSTPath(path: PathBuf) {
            display("Invalid SST path {:?}", path)
        }
        InvalidChunk {
            display("invalid chunk")
        }
        Engine(err: Box<dyn StdError + Send + Sync + 'static>) {
            display("{}", err)
        }
        CannotReadExternalStorage(url: String, name: String, local_path: PathBuf, err: IoError) {
            cause(err)
            display("Cannot read {}/{} into {}: {}", url, name, local_path.display(), err)
        }
        WrongKeyPrefix(what: &'static str, key: Vec<u8>, prefix: Vec<u8>) {
            display("\
                {} has wrong prefix: key {} does not start with {}",
                what,
                log_wrappers::Value::key(&key),
                log_wrappers::Value::key(&prefix),
            )
        }
        BadFormat(msg: String) {
            display("bad format {}", msg)
        }
        Encryption(err: EncryptionError) {
            from()
            display("Encryption {:?}", err)
        }
        CodecError(err: CodecError) {
            from()
            cause(err)
            display("Codec {}", err)
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

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Io(_) => error_code::sst_importer::IO,
            Error::Grpc(_) => error_code::sst_importer::GRPC,
            Error::Uuid(_) => error_code::sst_importer::UUID,
            Error::Future(_) => error_code::sst_importer::FUTURE,
            Error::RocksDB(_) => error_code::sst_importer::ROCKSDB,
            Error::EngineTraits(e) => e.error_code(),
            Error::ParseIntError(_) => error_code::sst_importer::PARSE_INT_ERROR,
            Error::FileExists(..) => error_code::sst_importer::FILE_EXISTS,
            Error::FileCorrupted(_, _) => error_code::sst_importer::FILE_CORRUPTED,
            Error::InvalidSSTPath(_) => error_code::sst_importer::INVALID_SST_PATH,
            Error::InvalidChunk => error_code::sst_importer::INVALID_CHUNK,
            Error::Engine(_) => error_code::sst_importer::ENGINE,
            Error::CannotReadExternalStorage(_, _, _, _) => {
                error_code::sst_importer::CANNOT_READ_EXTERNAL_STORAGE
            }
            Error::WrongKeyPrefix(_, _, _) => error_code::sst_importer::WRONG_KEY_PREFIX,
            Error::BadFormat(_) => error_code::sst_importer::BAD_FORMAT,
            Error::Encryption(e) => e.error_code(),
            Error::CodecError(e) => e.error_code(),
        }
    }
}

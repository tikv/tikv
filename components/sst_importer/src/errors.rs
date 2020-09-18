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
use thiserror::Error;
use tikv_util::codec::Error as CodecError;
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
        Error::CodecError(..) => "codec",
        _ => return,
    };
    IMPORTER_ERROR_VEC.with_label_values(&[label]).inc();
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] IoError),
    #[error("{0}")]
    Grpc(#[from] GrpcError),
    #[error("{0}")]
    Uuid(#[from] UuidError),
    #[error("{0}")]
    Future(#[from] Canceled),
    // FIXME: Remove concrete 'rocks' type
    #[error("RocksDB {0}")]
    RocksDB(String),
    #[error("Engine {0:?}")]
    EngineTraits(#[from] engine_traits::Error),
    #[error("{0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("File {0:?} exists")]
    FileExists(PathBuf),
    #[error("File {0:?} corrupted: {1}")]
    FileCorrupted(PathBuf, String),
    #[error("Invalid SST path {0:?}")]
    InvalidSSTPath(PathBuf),
    #[error("invalid chunk")]
    InvalidChunk,
    #[error("{0}")]
    Engine(Box<dyn StdError + Send + Sync + 'static>),
    #[error("Cannot read {}/{} into {}: {}", .0, .1, .2.display(), .3)]
    CannotReadExternalStorage(String, String, PathBuf, #[source] IoError),
    #[error("{0} has wrong prefix: key {} does not start with {}", hex::encode_upper(.1), hex::encode_upper(.2))]
    WrongKeyPrefix(&'static str, Vec<u8>, Vec<u8>),
    #[error("bad format {0}")]
    BadFormat(String),
    #[error("Encryption {0:?}")]
    Encryption(#[from] EncryptionError),
    #[error("Codec {0}")]
    CodecError(#[from] CodecError),
}

impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::RocksDB(err)
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
            Error::FileExists(_) => error_code::sst_importer::FILE_EXISTS,
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

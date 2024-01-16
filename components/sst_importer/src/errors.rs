// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError, io::Error as IoError, num::ParseIntError, path::PathBuf, result,
    time::Duration,
};

use encryption::Error as EncryptionError;
use error_code::{self, ErrorCode, ErrorCodeExt};
use futures::channel::oneshot::Canceled;
use grpcio::Error as GrpcError;
use kvproto::{errorpb, import_sstpb, kvrpcpb::ApiVersion};
use tikv_util::codec::Error as CodecError;
use uuid::Error as UuidError;

use crate::{metrics::*, sst_writer::SstWriterType};

pub fn error_inc(type_: &str, err: &Error) {
    let label = match err {
        Error::Io(..) => "io",
        Error::Grpc(..) => "grpc",
        Error::Uuid(..) => "uuid",
        Error::RocksDb(..) => "rocksdb",
        Error::EngineTraits(..) => "engine_traits",
        Error::ParseIntError(..) => "parse_int",
        Error::FileExists(..) => "file_exists",
        Error::FileCorrupted(..) => "file_corrupt",
        Error::InvalidSstPath(..) => "invalid_sst",
        Error::Engine(..) => "engine",
        Error::CannotReadExternalStorage { .. } => "read_external_storage",
        Error::WrongKeyPrefix { .. } => "wrong_prefix",
        Error::BadFormat(..) => "bad_format",
        Error::Encryption(..) => "encryption",
        Error::CodecError(..) => "codec",
        Error::Suspended { .. } => "suspended",
        _ => return,
    };
    IMPORTER_ERROR_VEC.with_label_values(&[type_, label]).inc();
}

#[derive(Debug, thiserror::Error)]
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
    RocksDb(String),

    #[error("Engine {0:?}")]
    EngineTraits(#[from] engine_traits::Error),

    #[error("{0}")]
    ParseIntError(#[from] ParseIntError),

    #[error("File {0:?} exists, cannot {1}")]
    FileExists(PathBuf, &'static str),

    #[error("File {0:?} corrupted: {1}")]
    FileCorrupted(PathBuf, String),

    #[error("Invalid SST path {0:?}")]
    InvalidSstPath(PathBuf),

    #[error("invalid chunk")]
    InvalidChunk,

    #[error("{0}")]
    Engine(Box<dyn StdError + Send + Sync + 'static>),

    #[error("Cannot read {url}/{name} into {}: {err}", local_path.display())]
    CannotReadExternalStorage {
        url: String,
        name: String,
        local_path: PathBuf,
        #[source]
        err: IoError,
    },

    #[error(
        "{what} has wrong prefix: key {} does not start with {}",
        log_wrappers::Value::key(key),
        log_wrappers::Value::key(prefix)
    )]
    WrongKeyPrefix {
        what: &'static str,
        key: Vec<u8>,
        prefix: Vec<u8>,
    },

    #[error("bad format {0}")]
    BadFormat(String),

    #[error("Encryption {0:?}")]
    Encryption(#[from] EncryptionError),

    #[error("Codec {0}")]
    CodecError(#[from] CodecError),

    #[error("ingest file conflict")]
    FileConflict,

    #[error("ttl is not enabled")]
    TtlNotEnabled,

    #[error("The length of ttls does not equal to the length of pairs")]
    TtlLenNotEqualsToPairs,

    #[error("Importing a SST file with imcompatible api version")]
    IncompatibleApiVersion,

    #[error("Key mode mismatched with the request mode, writer: {:?}, storage: {:?}, key: {}", .writer, .storage_api_version, .key)]
    InvalidKeyMode {
        writer: SstWriterType,
        storage_api_version: ApiVersion,
        key: String,
    },

    #[error("resource is not enough {0}")]
    ResourceNotEnough(String),

    #[error("imports are suspended for {time_to_lease_expire:?}")]
    Suspended { time_to_lease_expire: Duration },
}

impl Error {
    pub fn invalid_key_mode(
        writer: SstWriterType,
        storage_api_version: ApiVersion,
        key: &[u8],
    ) -> Self {
        Error::InvalidKeyMode {
            writer,
            storage_api_version,
            key: log_wrappers::hex_encode_upper(key),
        }
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Self::RocksDb(msg)
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for import_sstpb::Error {
    fn from(e: Error) -> import_sstpb::Error {
        let mut err = import_sstpb::Error::default();
        match e {
            Error::ResourceNotEnough(ref msg) => {
                let mut import_err = errorpb::Error::default();
                import_err.set_message(msg.clone());
                import_err.set_server_is_busy(errorpb::ServerIsBusy::default());
                err.set_store_error(import_err);
                err.set_message(format!("{}", e));
            }
            Error::Suspended {
                time_to_lease_expire,
            } => {
                let mut store_err = errorpb::Error::default();
                let mut server_is_busy = errorpb::ServerIsBusy::default();
                server_is_busy.set_backoff_ms(time_to_lease_expire.as_millis() as _);
                store_err.set_server_is_busy(server_is_busy);
                store_err.set_message(format!("{}", e));
                err.set_store_error(store_err);
                err.set_message(format!("{}", e));
            }
            _ => {
                err.set_message(format!("{}", e));
            }
        }

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
            Error::RocksDb(_) => error_code::sst_importer::ROCKSDB,
            Error::EngineTraits(e) => e.error_code(),
            Error::ParseIntError(_) => error_code::sst_importer::PARSE_INT_ERROR,
            Error::FileExists(..) => error_code::sst_importer::FILE_EXISTS,
            Error::FileCorrupted(..) => error_code::sst_importer::FILE_CORRUPTED,
            Error::InvalidSstPath(_) => error_code::sst_importer::INVALID_SST_PATH,
            Error::InvalidChunk => error_code::sst_importer::INVALID_CHUNK,
            Error::Engine(_) => error_code::sst_importer::ENGINE,
            Error::CannotReadExternalStorage { .. } => {
                error_code::sst_importer::CANNOT_READ_EXTERNAL_STORAGE
            }
            Error::WrongKeyPrefix { .. } => error_code::sst_importer::WRONG_KEY_PREFIX,
            Error::BadFormat(_) => error_code::sst_importer::BAD_FORMAT,
            Error::Encryption(e) => e.error_code(),
            Error::CodecError(e) => e.error_code(),
            Error::FileConflict => error_code::sst_importer::FILE_CONFLICT,
            Error::TtlNotEnabled => error_code::sst_importer::TTL_NOT_ENABLED,
            Error::TtlLenNotEqualsToPairs => error_code::sst_importer::TTL_LEN_NOT_EQUALS_TO_PAIRS,
            Error::IncompatibleApiVersion => error_code::sst_importer::INCOMPATIBLE_API_VERSION,
            Error::InvalidKeyMode { .. } => error_code::sst_importer::INVALID_KEY_MODE,
            Error::ResourceNotEnough(_) => error_code::sst_importer::RESOURCE_NOT_ENOUTH,
            Error::Suspended { .. } => error_code::sst_importer::SUSPENDED,
        }
    }
}

// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use error_code::{self, ErrorCode, ErrorCodeExt};
use raft::{Error as RaftError, StorageError};
use std::{error, result};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        // Engine uses plain string as the error.
        Engine(msg: String) {
            from()
            display("Storage Engine {}", msg)
        }
        // FIXME: It should not know Region.
        NotInRange( key: Vec<u8>, region_id: u64, start: Vec<u8>, end: Vec<u8>) {
            display(
                "Key {} is out of [region {}] [{}, {})",
                hex::encode_upper(&key), region_id, hex::encode_upper(&start), hex::encode_upper(&end)
            )
        }
        Protobuf(err: protobuf::ProtobufError) {
            from()
            cause(err)
            display("Protobuf {}", err)
        }
        Io(err: std::io::Error) {
            from()
            cause(err)
            display("Io {}", err)
        }
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            display("{:?}", err)
        }
        CFName(name: String) {
            display("CF {} not found", name)
        }
        Codec(err: tikv_util::codec::Error) {
            from()
            cause(err)
            display("Codec {}", err)
        }
        DataUnavailable(region_id: u64) {
            from()
            display("The entries of region {} is unavailable", region_id)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Engine(_) => error_code::engine::ENGINE,
            Error::NotInRange(_, _, _, _) => error_code::engine::NOT_IN_RANGE,
            Error::Protobuf(_) => error_code::engine::PROTOBUF,
            Error::Io(_) => error_code::engine::IO,
            Error::CFName(_) => error_code::engine::CF_NAME,
            Error::Codec(_) => error_code::engine::CODEC,
            Error::Other(_) => error_code::engine::UNKNOWN,
            Error::DataUnavailable(_) => error_code::engine::DATALOSS,
        }
    }
}

impl From<Error> for RaftError {
    fn from(e: Error) -> RaftError {
        match e {
            Error::DataUnavailable(_) => RaftError::Store(StorageError::Unavailable),
            e => {
                let boxed = Box::new(e) as Box<dyn std::error::Error + Sync + Send>;
                raft::Error::Store(StorageError::Other(boxed))
            }
        }
    }
}

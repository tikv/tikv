// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError, io::Error as IoError, net::AddrParseError, result};

use engine_traits::Error as EngineTraitError;
use futures::channel::{mpsc::SendError, oneshot::Canceled};
use hyper::Error as HttpError;
use openssl::error::ErrorStack as OpenSslError;
use pd_client::Error as PdError;
use protobuf::ProtobufError;
use raftstore::Error as RaftServerError;
use thiserror::Error;
use tikv_util::{codec::Error as CodecError, worker::ScheduleError};

use super::snap::Task as SnapTask;
use crate::storage::{kv::Error as EngineError, Error as StorageError};

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),

    // Following is for From other errors.
    #[error("{0:?}")]
    Io(#[from] IoError),

    #[error("{0}")]
    Protobuf(#[from] ProtobufError),

    #[error("{0:?}")]
    Grpc(#[from] tonic::Status),

    #[error("{0:?}")]
    Ssl(#[from] tonic::transport::Error),

    #[error("{0:?}")]
    Codec(#[from] CodecError),

    #[error("{0:?}")]
    AddrParse(#[from] AddrParseError),

    #[error("{0:?}")]
    RaftServer(#[from] RaftServerError),

    #[error("{0:?}")]
    Engine(#[from] EngineError),

    #[error("{0:?}")]
    EngineTrait(#[from] EngineTraitError),

    #[error("{0:?}")]
    Storage(#[from] StorageError),

    #[error("{0:?}")]
    Pd(#[from] PdError),

    #[error("{0:?}")]
    SnapWorkerStopped(#[from] ScheduleError<SnapTask>),

    #[error("failed to poll from mpsc receiver")]
    Sink,

    #[error("{0:?}")]
    RecvError(#[from] Canceled),

    #[error("{0:?}")]
    Http(#[from] HttpError),

    #[error("{0:?}")]
    OpenSsl(#[from] OpenSslError),

    #[error("{0:?}")]
    StreamDisconnect(#[from] SendError),

    #[error("cluster of request={request_id} does not match TiKV cluster id={cluster_id}")]
    ClusterIDMisMatch { request_id: u64, cluster_id: u64 },
}

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        match err {
            Error::Grpc(s) => s,
            e @ _ => tonic::Status::unknown(format!("{:?}", e)),
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

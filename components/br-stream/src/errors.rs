// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use etcd_client::Error as EtcdError;
use protobuf::ProtobufError;
use std::io::Error as IoError;
use std::result::Result as StdResult;
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Etcd meet error {0}")]
    Etcd(#[from] EtcdError),
    #[error("Protobuf meet error {0}")]
    Protobuf(#[from] ProtobufError),
    #[error("No such task {task_name:?}")]
    NoSuchTask { task_name: String },
    #[error("Malformed metadata {0}")]
    MalformedMetadata(String),
    #[error("I/O Error: {0}")]
    Io(#[from] IoError),
}

pub type Result<T> = StdResult<T, Error>;

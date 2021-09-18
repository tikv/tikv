// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{dfs, table};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    KeyNotFound,
    ShardNotFound,
    ShardNotMatch,
    WrongSplitStage,
    ErrAllocID(String),
    ErrOpen(String),
    TableError(table::Error),
    DFSError(dfs::Error),
    Io(std::io::Error),
}

impl From<table::Error> for Error {
    fn from(e: table::Error) -> Self {
        Error::TableError(e)
    }
}

impl From<dfs::Error> for Error {
    fn from(e: dfs::Error) -> Self {
        Error::DFSError(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

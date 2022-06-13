// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
// Bytes as map key
#![allow(clippy::mutable_key_type)]

#[cfg(test)]
extern crate test;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;
#[macro_use]
extern crate serde_derive;

pub mod engine;
pub mod iterator;
pub mod load;
mod log_batch;
mod metrics;
pub mod traits;
pub mod worker;
mod write_batch;
pub mod writer;

use std::num::ParseIntError;

pub use engine::*;
use iterator::*;
use load::*;
use metrics::*;
use thiserror::Error as ThisError;
pub use traits::*;
use worker::*;
pub use write_batch::WriteBatch;
pub use writer::*;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("IO error: {0:?}")]
    Io(std::io::Error),
    #[error("EOF")]
    EOF,
    #[error("parse error")]
    ParseError,
    #[error("Open error: {0}")]
    Open(String),
    #[error("Corruption: {0}")]
    Corruption(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Error::EOF;
        }
        Error::Io(e)
    }
}

impl From<ParseIntError> for Error {
    fn from(_: ParseIntError) -> Self {
        Error::ParseError
    }
}

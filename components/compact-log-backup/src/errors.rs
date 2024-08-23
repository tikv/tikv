// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{fmt::Display, panic::Location};

use thiserror::Error as ThisError;
use tikv_util::codec;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub notes: String,
    pub attached_frames: Vec<Location<'static>>,
}

#[derive(ThisError, Debug)]
pub enum ErrorKind {
    #[error("I/O {0}")]
    Io(#[from] std::io::Error),
    #[error("Protobuf {0}")]
    Protobuf(#[from] protobuf::error::ProtobufError),
    #[error("Engine {0}")]
    Engine(#[from] engine_traits::Error),
    #[error("Codec {0}")]
    Codec(#[from] codec::Error),
    #[error("Uncategorised Error {0}")]
    Other(String),
}

impl<T: Into<ErrorKind>> From<T> for Error {
    #[track_caller]
    fn from(value: T) -> Self {
        Error {
            kind: value.into(),
            notes: String::new(),
            attached_frames: vec![*Location::caller()],
        }
    }
}

pub trait TraceResultExt {
    fn trace_err(self) -> Self;
    fn annotate(self, message: impl Display) -> Self;
}

impl<T> TraceResultExt for Result<T> {
    #[track_caller]
    fn trace_err(self) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(err) => Err(err.attach_current_frame()),
        }
    }

    #[track_caller]
    fn annotate(self, message: impl Display) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(mut err) => {
                err.notes = message.to_string();
                Err(err.attach_current_frame())
            }
        }
    }
}

pub trait OtherErrExt<T> {
    fn adapt_err(self) -> Result<T>;
}

impl<T, E: Display> OtherErrExt<T> for std::result::Result<T, E> {
    #[track_caller]
    fn adapt_err(self) -> Result<T> {
        match self {
            Ok(t) => Ok(t),
            Err(err) => Err(Error {
                kind: ErrorKind::Other(err.to_string()),
                notes: String::new(),
                attached_frames: vec![*Location::caller()],
            }),
        }
    }
}

impl Error {
    #[track_caller]
    pub fn attach_current_frame(mut self) -> Self {
        self.attached_frames.push(*Location::caller());
        self
    }

    pub fn message(mut self, m: impl Display) -> Self {
        if self.notes.is_empty() {
            self.notes = m.to_string();
        } else {
            self.notes = format!("{}: {}", self.notes, m);
        }
        self
    }
}

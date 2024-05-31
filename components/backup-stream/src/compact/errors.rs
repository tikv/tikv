use std::{fmt::Display, panic::Location};

use thiserror::Error as ThisError;

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

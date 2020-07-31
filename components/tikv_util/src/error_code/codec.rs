// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::ErrorCodeExt;
use crate::codec::Error;
use codec::Error as CodecError;

define_error_codes!(
    "KV:Codec:",

    IO => ("Io", "", ""),
    BAD_PADDING => ("BadPadding", "", ""),
    KEY_LENGTH => ("KeyLength", "", ""),
    KEY_NOT_FOUND => ("KeyNotFound", "", "")
);

// Implement ErrorCodeExt here for avoiding cyclic dependency.
impl ErrorCodeExt for CodecError {
    fn error_code(&self) -> ErrorCode {
        match self.0 {
            box codec::ErrorInner::Io(_) => IO,
            box codec::ErrorInner::BadPadding => BAD_PADDING,
        }
    }
}

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Io(_) => IO,
            Error::KeyLength => KEY_LENGTH,
            Error::KeyPadding => BAD_PADDING,
            Error::KeyNotFound => KEY_NOT_FOUND,
        }
    }
}

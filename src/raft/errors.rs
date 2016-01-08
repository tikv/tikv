use std::{result, io, fmt};
use std::error;

use protobuf::ProtobufError;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    NotFound,
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotFound => write!(f, "not found"),
            Error::Io(ref error) => fmt::Display::fmt(error, f),
            _ => fmt::Display::fmt(self, f),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            // not sure that cause should be included in message
            &Error::Io(ref e) => e.description(),
            &Error::NotFound => "not found",
            &Error::Other(ref e) => &e,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            &Error::Io(ref e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<ProtobufError> for Error {
    fn from(err: ProtobufError) -> Error {
        Error::Other(error::Error::description(&err).to_string())
    }
}

pub type Result<T> = result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use std::convert::From;
    use std::error;
    use protobuf::ProtobufError;

    use super::Error;

    #[test]
    fn test_error() {
        let e = ProtobufError::WireError("a error".to_string());
        let err: Error = From::from(e);

        assert!(error::Error::cause(&err).is_none());
    }
}

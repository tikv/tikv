use std::error;
use std::result;
use std::time::Duration;

use kvproto::{errorpb, kvrpcpb};
use tipb;

// TODO: It's only a wrapper for Coprocessor Error. The dag need a better Error.
quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Region(err: errorpb::Error) {
            description("region related failure")
            display("region {:?}", err)
        }
        Locked(l: kvrpcpb::LockInfo) {
            description("key is locked")
            display("locked {:?}", l)
        }
        Outdated(elapsed: Duration, tag: &'static str) {
            description("request is outdated")
        }
        Full {
            description("Coprocessor end-point thread pool is full")
        }
        Eval(err: tipb::select::Error) {
            from()
            description("eval failed")
            display("Eval error: {}", err.get_msg())
        }
        Other(err: Box<dyn error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<crate::codec::error::Error> for Error {
    fn from(e: crate::codec::error::Error) -> Error {
        Error::from(<crate::codec::error::Error as Into<tipb::select::Error>>::into(e))
    }
}

impl From<crate::storage::store::Error> for Error {
    fn from(e: crate::storage::store::Error) -> Error {
        match e {
            _ => Error::Other(Box::new(e)),
        }
    }
}

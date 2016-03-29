use std::error;
use std::boxed::Box;
use std::result;
use std::io;
use std::net;

use protobuf::ProtobufError;

use util::codec;
use raftserver;
use storage;

quick_error!{
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
        // Following is for From other errors.
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Protobuf(err: ProtobufError) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        AddrParse(err: net::AddrParseError) {
            from()
            cause(err)
            description(err.description())
        }
        RaftServer(err: raftserver::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Storage(err: storage::Error) {
            from()
            cause(err)
            description(err.description())
        }
    }
}


pub type Result<T> = result::Result<T, Error>;

pub fn other<T>(err: T) -> Error
    where T: Into<Box<error::Error + Sync + Send + 'static>>
{
    Error::Other(err.into())
}

use std::error;
use std::boxed::Box;
use std::result;
use std::io::Error as IoError;
use std::net::AddrParseError;

use protobuf::ProtobufError;

use util::codec::Error as CodecError;
use raftstore::Error as RaftServerError;
use storage::Error as StorageError;
use pd::Error as PdError;

quick_error!{
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
        // Following is for From other errors.
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        Protobuf(err: ProtobufError) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: CodecError) {
            from()
            cause(err)
            description(err.description())
        }
        AddrParse(err: AddrParseError) {
            from()
            cause(err)
            description(err.description())
        }
        RaftServer(err: RaftServerError) {
            from()
            cause(err)
            description(err.description())
        }
        Storage(err: StorageError) {
            from()
            cause(err)
            description(err.description())
        }
        Pd(err: PdError) {
            from()
            cause(err)
            description(err.description())
        }
    }
}


pub type Result<T> = result::Result<T, Error>;


use std::error;
use std::io::Error as IoError;
use std::num;
use util::codec;
use protobuf;


pub mod util;
pub mod log_batch;
pub mod pipe_log;
pub mod mem_entries;
pub mod engine;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
        }
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: codec::Error) {
            from()
            cause(err)
            description(err.description())
            display("Codec {}", err)
        }
        Protobuf(err: protobuf::ProtobufError) {
            from()
            cause(err)
            description(err.description())
            display("protobuf error {:?}", err)
        }
        ParseError(err: num::ParseIntError) {
            from()
            cause(err)
            description(err.description())
            display("Parse int error {:?}", err)
        }
        CheckSumError {
            description("checksum is not correct")
        }
        TooShort {
            description("content too short")
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

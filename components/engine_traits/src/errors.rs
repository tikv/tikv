// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error, result};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        // Engine uses plain string as the error.
        Engine(msg: String) {
            from()
            description("Storage Engine error")
            display("Storage Engine {}", msg)
        }
        // FIXME: It should not know Region.
        NotInRange( key: Vec<u8>, region_id: u64, start: Vec<u8>, end: Vec<u8>) {
            description("Key is out of range")
            display(
                "Key {} is out of [region {}] [{}, {})",
                hex::encode_upper(&key), region_id, hex::encode_upper(&start), hex::encode_upper(&end)
            )
        }
        Protobuf(err: protobuf::ProtobufError) {
            from()
            cause(err)
            description(err.description())
            display("Protobuf {}", err)
        }
        Io(err: std::io::Error) {
            from()
            cause(err)
            description(err.description())
            display("Io {}", err)
        }
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
        CFName(name: String) {
            description("CF name not found")
            display("CF {} not found", name)
        }
        Codec(err: tikv_util::codec::Error) {
            from()
            cause(err)
            description("Codec error")
            display("Codec {}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

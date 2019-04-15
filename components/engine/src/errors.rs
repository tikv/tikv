// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error, result};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        // RocksDb uses plain string as the error.
        RocksDb(msg: String) {
            from()
            description("RocksDb error")
            display("RocksDb {}", msg)
        }
        // FIXME: It should not know Region.
        NotInRange( key: Vec<u8>, regoin_id: u64, start: Vec<u8>, end: Vec<u8>) {
            description("Key is out of range")
            display(
                "Key {:?} is out of [region {}] [{:?}, {:?})",
                escape(&key), regoin_id, escape(&start), escape(&end)
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
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for raft::Error {
    fn from(err: Error) -> raft::Error {
        raft::Error::Store(raft::StorageError::Other(err.into()))
    }
}

impl From<Error> for kvproto::errorpb::Error {
    fn from(err: Error) -> kvproto::errorpb::Error {
        let mut errorpb = kvproto::errorpb::Error::new();
        errorpb.set_message(error::Error::description(&err).to_owned());

        if let Error::NotInRange(key, region_id, start_key, end_key) = err {
            errorpb.mut_key_not_in_region().set_key(key);
            errorpb.mut_key_not_in_region().set_region_id(region_id);
            errorpb.mut_key_not_in_region().set_start_key(start_key);
            errorpb.mut_key_not_in_region().set_end_key(end_key);
        }

        errorpb
    }
}

// A copy of `tikv_util::escape`.
// TODO: remove it once util becomes a component.
fn escape(data: &[u8]) -> String {
    let mut escaped = Vec::with_capacity(data.len() * 4);
    for &c in data {
        match c {
            b'\n' => escaped.extend_from_slice(br"\n"),
            b'\r' => escaped.extend_from_slice(br"\r"),
            b'\t' => escaped.extend_from_slice(br"\t"),
            b'"' => escaped.extend_from_slice(b"\\\""),
            b'\\' => escaped.extend_from_slice(br"\\"),
            _ => {
                if c >= 0x20 && c < 0x7f {
                    // c is printable
                    escaped.push(c);
                } else {
                    escaped.push(b'\\');
                    escaped.push(b'0' + (c >> 6));
                    escaped.push(b'0' + ((c >> 3) & 7));
                    escaped.push(b'0' + (c & 7));
                }
            }
        }
    }
    escaped.shrink_to_fit();
    unsafe { String::from_utf8_unchecked(escaped) }
}

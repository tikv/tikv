// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error;
use std::boxed::Box;
use std::result;
use std::io::Error as IoError;
use std::net::AddrParseError;
use std::time::Duration;

use protobuf::ProtobufError;
use hyper::Error as HyperError;
use hyper::StatusCode;

use util::codec::Error as CodecError;
use raftstore::Error as RaftServerError;
use storage::engine::Error as EngineError;
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
        Engine(err: EngineError) {
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
        Hyper(err: HyperError) {
            from()
            cause(err)
            description(err.description())
        }
        Timeout(d: Duration) {
            description("request timeout")
            display("timeout after {:?}", d)
        }
        // TODO: add header and error message.
        HttpResponse(code: StatusCode) {
            description("invalid HTTP response")
            display("invalid HTTP response, status: {:?}", code)
        }
    }
}


pub type Result<T> = result::Result<T, Error>;

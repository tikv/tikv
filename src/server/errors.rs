// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;
use std::io::Error as IoError;
use std::net::AddrParseError;
use std::result;

use grpcio::Error as GrpcError;
use hyper::Error as HttpError;
use protobuf::ProtobufError;
use tokio_sync::oneshot::error::RecvError;

use super::snap::Task as SnapTask;
use crate::raftstore::Error as RaftServerError;
use crate::storage::kv::Error as EngineError;
use crate::storage::Error as StorageError;
use pd_client::Error as PdError;
use tikv_util::codec::Error as CodecError;
use tikv_util::worker::ScheduleError;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
        // Following is for From other errors.
        Io(err: IoError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        Protobuf(err: ProtobufError) {
            from()
            cause(err)
            description(err.description())
        }
        Grpc(err: GrpcError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        Codec(err: CodecError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        AddrParse(err: AddrParseError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        RaftServer(err: RaftServerError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        Engine(err: EngineError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        Storage(err: StorageError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        RealEngine(err: engine::Error) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        Pd(err: PdError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        SnapWorkerStopped(err: ScheduleError<SnapTask>) {
            from()
            display("{:?}", err)
        }
        Sink {
            description("failed to poll from mpsc receiver")
        }
        RecvError(err: RecvError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        Http(err: HttpError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

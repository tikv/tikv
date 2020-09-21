mod fresh;
mod legacy;

use grpcio::{RpcStatus, RpcStatusCode};

fn grpc_error_is_unimplemented(e: &grpcio::Error) -> bool {
    if let grpcio::Error::RpcFailure(RpcStatus { ref status, .. }) = e {
        let x = *status == RpcStatusCode::UNIMPLEMENTED;
        return x;
    }
    false
}

pub use legacy::RaftClient;

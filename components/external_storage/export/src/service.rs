use crate::export::create_storage;
use external_storage::ExternalStorage;
use std::io::ErrorKind;

use grpcio::{self, *};
use kvproto::backup as proto;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Storages = Arc<Mutex<HashMap<String, Box<dyn ExternalStorage>>>>;

/// Service handles the RPC messages for the `ExternalStorage` service.
#[derive(Clone)]
pub struct Service {
    storages: Storages,
}

impl Service {
    /// Create a new backup service.
    pub fn new(storages: Storages) -> Service {
        Service { storages }
    }
}

struct Socket(&'static str);

impl Drop for Socket {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.0);
    }
}

pub fn new_service() -> Result<Server> {
    let socket = Socket("external_storage");
    let env = Arc::new(EnvBuilder::new().build());
    let storage_service = Service::new(Arc::new(Mutex::new(HashMap::new())));
    let builder =
        ServerBuilder::new(env).register_service(proto::create_external_storage(storage_service));
    let socket_addr = format!("unix:{}", socket.0);
    let mut server = builder.bind(socket_addr, 0).build().unwrap();
    server.start();
    let (_, _) = server.bind_addrs().next().unwrap();
    Ok(server)
    // let channel = ChannelBuilder::new(env).connect(&socket_addr);
    // let client = proto::ExternalStorageClient::new(channel);
    // Ok((server, client))
}

impl proto::ExternalStorage for Service {
    fn start(
        &mut self,
        _ctx: RpcContext,
        req: proto::ExternalStorageStartRequest,
        sink: UnarySink<proto::ExternalStorageStartResponse>,
    ) {
        match create_storage(req.get_storage_backend()) {
            Ok(storage) => {
                let name = storage.name().to_string();
                // This allows just one aws storage
                // But we could allow multiple
                // This insert will update any existing storage
                self.storages.lock().unwrap().insert(name.clone(), storage);
                let mut rsp = proto::ExternalStorageStartResponse::default();
                rsp.set_storage_id(name);
                sink.success(rsp);
            }
            Err(e) => {
                sink.fail(make_rpc_error(e));
            }
        }
    }

    fn finish(
        &mut self,
        _ctx: RpcContext,
        req: proto::ExternalStorageFinishRequest,
        sink: UnarySink<proto::ExternalStorageFinishResponse>,
    ) {
        let storage_id = req.get_storage_id();
        self.storages.lock().unwrap().remove(storage_id);
        sink.success(proto::ExternalStorageFinishResponse::default());
    }
}

pub fn make_rpc_error(err: std::io::Error) -> RpcStatus {
    RpcStatus::new(
        match err.kind() {
            ErrorKind::NotFound => RpcStatusCode::NOT_FOUND,
            ErrorKind::InvalidInput => RpcStatusCode::INVALID_ARGUMENT,
            ErrorKind::PermissionDenied => RpcStatusCode::UNAUTHENTICATED,
            _ => RpcStatusCode::UNKNOWN,
        },
        Some(format!("{:?}", err)),
    )
}

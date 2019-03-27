// Copyright 2018 TiKV Project Authors.
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

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use crate::grpc::{ChannelBuilder, EnvBuilder, Server as GrpcServer, ServerBuilder};
use kvproto::import_kvpb_grpc::create_import_kv;

use crate::config::TiKvConfig;

use super::{ImportKVService, KVImporter};

const MAX_GRPC_MSG_LEN: i32 = 32 * 1024 * 1024;

/// ImportKVServer is a gRPC server that provides service to write key-value
/// pairs into RocksDB engines for later ingesting into tikv-server.
pub struct ImportKVServer {
    grpc_server: GrpcServer,
}

impl ImportKVServer {
    pub fn new(tikv: &TiKvConfig) -> ImportKVServer {
        let cfg = &tikv.server;
        let addr = SocketAddr::from_str(&cfg.addr).unwrap();

        let importer = KVImporter::new(
            tikv.import.clone(),
            tikv.rocksdb.clone(),
            tikv.security.clone(),
        )
        .unwrap();
        let import_service = ImportKVService::new(tikv.import.clone(), Arc::new(importer));

        let env = Arc::new(
            EnvBuilder::new()
                .name_prefix(thd_name!("import-server"))
                .cq_count(cfg.grpc_concurrency)
                .build(),
        );

        let channel_args = ChannelBuilder::new(Arc::clone(&env))
            .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as i32)
            .max_concurrent_stream(cfg.grpc_concurrent_stream)
            .max_send_message_len(MAX_GRPC_MSG_LEN)
            .max_receive_message_len(MAX_GRPC_MSG_LEN)
            .build_args();

        let grpc_server = ServerBuilder::new(Arc::clone(&env))
            .bind(format!("{}", addr.ip()), addr.port())
            .channel_args(channel_args)
            .register_service(create_import_kv(import_service))
            .build()
            .unwrap();

        ImportKVServer { grpc_server }
    }

    pub fn start(&mut self) {
        self.grpc_server.start();
    }

    pub fn shutdown(&mut self) {
        self.grpc_server.shutdown();
    }

    pub fn bind_addrs(&self) -> &[(String, u16)] {
        self.grpc_server.bind_addrs()
    }
}

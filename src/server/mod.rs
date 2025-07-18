// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

pub(crate) mod metrics;
mod raft_client;

pub mod config;
pub mod debug;
pub mod debug2;
mod engine_factory;
pub mod errors;
pub mod gc_worker;
pub mod load_statistics;
pub mod lock_manager;
mod proxy;
pub mod raft_server;
pub mod raftkv;
mod raftkv2;
mod reset_to_version;
pub mod resolve;
pub mod server;
pub mod service;
pub mod snap;
pub mod status_server;
pub mod tablet_snap;
pub mod transport;
pub mod ttl;

pub use engine_factory::{KvEngineFactory, KvEngineFactoryBuilder};

#[cfg(any(test, feature = "testexport"))]
pub use self::server::test_router::TestRaftStoreRouter;
pub use self::{
    config::{Config, DEFAULT_CLUSTER_ID, DEFAULT_LISTENING_ADDR, ServerConfigManager},
    errors::{Error, Result},
    metrics::{
        CONFIG_FLOW_CONTROL_GAUGE, CONFIG_ROCKSDB_CF_GAUGE, CONFIG_ROCKSDB_DB_GAUGE,
        CPU_CORES_QUOTA_GAUGE, MEM_TRACE_SUM_GAUGE, MEMORY_LIMIT_GAUGE,
    },
    proxy::{Proxy, build_forward_option, get_target_address},
    raft_client::{ConnectionBuilder, MetadataSourceStoreId, RaftClient},
    raft_server::MultiRaftServer,
    raftkv::RaftKv,
    raftkv2::{Extension, NodeV2, RaftKv2},
    resolve::{PdStoreAddrResolver, StoreAddrResolver},
    server::{GRPC_THREAD_PREFIX, RAFT_CLIENT_THREAD_PREFIX, Server},
    transport::ServerTransport,
};

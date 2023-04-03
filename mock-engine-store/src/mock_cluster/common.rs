// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub use engine_store_ffi::TiFlashEngine;
pub use proxy_server::config::ProxyConfig;

// TODO align with engine_test when they are stable. However, we don't directly
// use RaftTestEngine, since we have PSLogEngine. Will be refered be mock_store.
#[cfg(feature = "test-engine-raft-raft-engine")]
pub type ProxyRaftEngine = raft_log_engine::RaftLogEngine;
#[cfg(not(feature = "test-engine-raft-raft-engine"))]
pub type ProxyRaftEngine = engine_rocks::RocksEngine;

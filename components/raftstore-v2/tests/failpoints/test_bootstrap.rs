// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::assert_matches::assert_matches;

use engine_traits::RaftEngineReadOnly;
use kvproto::metapb::Store;
use raftstore_v2::Bootstrap;
use slog::o;
use tempfile::TempDir;

#[test]
fn test_bootstrap_half_way_failure() {
    let server = test_pd::Server::new(1);
    let eps = server.bind_addrs();
    let pd_client = test_pd::util::new_client(eps, None);
    let path = TempDir::new().unwrap();
    let engines = engine_test::new_temp_engine(&path);
    let bootstrap = || {
        let logger = slog_global::borrow_global().new(o!());
        let mut bootstrap = Bootstrap::new(&engines.raft, 0, &pd_client, logger);
        match bootstrap.bootstrap_store() {
            Ok(store_id) => {
                let mut store = Store::default();
                store.set_id(store_id);
                bootstrap.bootstrap_first_region(&store, store_id)
            }
            Err(e) => Err(e),
        }
    };

    // Try to start this node, return after persisted some keys.
    fail::cfg("node_after_bootstrap_store", "return").unwrap();
    let s = format!("{}", bootstrap().unwrap_err());
    assert!(s.contains("node_after_bootstrap_store"), "{}", s);
    assert_matches!(engines.raft.get_prepare_bootstrap_region(), Ok(None));

    let ident = engines.raft.get_store_ident().unwrap().unwrap();
    assert_ne!(ident.get_store_id(), 0);

    // Check whether it can bootstrap cluster successfully.
    fail::remove("node_after_bootstrap_store");
    fail::cfg("node_after_prepare_bootstrap_cluster", "return").unwrap();
    let s = format!("{}", bootstrap().unwrap_err());
    assert!(s.contains("node_after_prepare_bootstrap_cluster"), "{}", s);
    assert_matches!(engines.raft.get_prepare_bootstrap_region(), Ok(Some(_)));

    fail::remove("node_after_prepare_bootstrap_cluster");
    fail::cfg("node_after_bootstrap_cluster", "return").unwrap();
    let s = format!("{}", bootstrap().unwrap_err());
    assert!(s.contains("node_after_bootstrap_cluster"), "{}", s);
    assert_matches!(engines.raft.get_prepare_bootstrap_region(), Ok(Some(_)));

    // Although aborted by error, rebootstrap should continue.
    bootstrap().unwrap().unwrap();
    assert_matches!(engines.raft.get_prepare_bootstrap_region(), Ok(None));

    // Second bootstrap should be noop.
    assert_eq!(bootstrap().unwrap(), None);

    assert_matches!(engines.raft.get_prepare_bootstrap_region(), Ok(None));
}

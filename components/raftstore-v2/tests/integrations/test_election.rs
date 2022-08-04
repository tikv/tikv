// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use raftstore_v2::PeerMsg;

// TODO: finish test case when callback is
// TODO: enable the case when deadlock is resolved in tablet factory
// #[test]
// fn test_smoke() {
//     let (_node, _transport, router) = super::setup_default_cluster();
//     router.send(2, PeerMsg::Noop).unwrap();
// }

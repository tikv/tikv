// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use causal_ts::{CausalTsProvider, TsoSimpleProvider};
use test_raftstore::TestPdClient;

#[test]
fn test_tso_simple_provider() {
    let pd_cli = Arc::new(TestPdClient::new(1, false));

    let provider = TsoSimpleProvider::new(pd_cli.clone());

    pd_cli.set_tso(100.into());
    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 100.into(), "ts: {:?}", ts);
}

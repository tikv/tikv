// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use std::sync::Arc;
use std::time::Duration;

use causal_ts::{BatchTsoProvider, CausalTsProvider, SimpleTsoProvider};
use test_raftstore::TestPdClient;

#[test]
fn test_simple_tso_provider() {
    let pd_cli = Arc::new(TestPdClient::new(1, false));

    let provider = SimpleTsoProvider::new(pd_cli.clone());

    pd_cli.set_tso(100.into());
    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 101.into(), "ts: {:?}", ts);
}

#[test]
fn test_batch_tso_provider() {
    let pd_cli = Arc::new(TestPdClient::new(1, false));

    pd_cli.set_tso(10.into());
    // Set `renew_interval` to 0 to disable background renew. Invoke `flush()` to renew manually.
    let provider = block_on(BatchTsoProvider::new_opt(pd_cli, Duration::ZERO, 100)).unwrap();
    // allocated: [11, 110]
    assert_eq!(provider.batch_size(), 100);

    let mut ts = provider.get_ts().unwrap();
    assert_eq!(ts, 11.into());

    provider.flush().unwrap(); // allocated: [111, 210]
    assert_eq!(provider.batch_size(), 100);
    ts = provider.get_ts().unwrap();
    assert_eq!(ts, 111.into());

    // used > 75%
    for _ in 0..75 {
        let _ = provider.get_ts().unwrap();
    }
    provider.flush().unwrap(); // allocated: [211, 410]
    assert_eq!(provider.batch_size(), 200);
    ts = provider.get_ts().unwrap();
    assert_eq!(ts, 211.into());

    // used 50%
    for _ in 0..100 {
        let _ = provider.get_ts().unwrap();
    }
    provider.flush().unwrap(); // allocated: [411, 610]
    assert_eq!(provider.batch_size(), 200);
    ts = provider.get_ts().unwrap();
    assert_eq!(ts, 411.into());

    // used < 25%
    for _ in 0..48 {
        let _ = provider.get_ts().unwrap();
    }
    provider.flush().unwrap(); // allocated: [611, 710]
    assert_eq!(provider.batch_size(), 100);
    ts = provider.get_ts().unwrap();
    assert_eq!(ts, 611.into());

    // used 50%
    for _ in 0..50 {
        let _ = provider.get_ts().unwrap();
    }
    provider.flush().unwrap(); // allocated: [711, 810]
    assert_eq!(provider.batch_size(), 100);
    ts = provider.get_ts().unwrap();
    assert_eq!(ts, 711.into());
}

#[test]
fn test_batch_tso_provider_on_failure() {
    // let pd_cli = Arc::new(TestPdClient::new(1, false));
    //
    // let tso_refresh_interval = 200;
    //
    // pd_cli.set_tso(200.into());
    // let provider = block_on(HlcProvider::new_opt(
    //     pd_cli.clone(),
    //     Duration::from_millis(tso_refresh_interval),
    // ))
    //     .unwrap();
    //
    // let ts = provider.get_ts().unwrap();
    // assert_eq!(ts, 200.into(), "ts: {:?}", ts);
    //
    // pd_cli.set_tso(250.into());
    //
    // sleep_ms(tso_refresh_interval + tso_refresh_interval / 2);
    // let ts = provider.get_ts().unwrap();
    // assert_eq!(ts, 251.into(), "ts: {:?}", ts);
    //
    // // tso fail
    // pd_cli.set_tso(300.into());
    // pd_cli.trigger_tso_failure();
    //
    // sleep_ms(tso_refresh_interval);
    // let ts = provider.get_ts().unwrap();
    // assert_eq!(ts, 252.into(), "ts: {:?}", ts);
    // pd_cli.trigger_tso_failure();
    //
    // sleep_ms(tso_refresh_interval);
    // let ts = provider.get_ts().unwrap();
    // assert_eq!(ts, 253.into(), "ts: {:?}", ts);
    //
    // sleep_ms(tso_refresh_interval);
    // let ts = provider.get_ts().unwrap();
    // assert_eq!(ts, 300.into(), "ts: {:?}", ts);
}

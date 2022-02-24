// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use causal_ts::{CausalTsProvider, HlcProvider};
use futures::executor::block_on;
use test_raftstore::sleep_ms;
use test_raftstore::TestPdClient;

#[test]
fn test_hlc_tso_provider() {
    let pd_cli = Arc::new(TestPdClient::new(1, false));

    pd_cli.set_tso(100.into());
    let provider = HlcProvider::new(pd_cli);
    block_on(provider.init()).unwrap();

    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 100.into(), "ts: {:?}", ts);

    let ts1 = ts.into_inner() + 10;
    provider.advance(ts1.into()).unwrap();
    let ts2 = provider.get_ts().unwrap();
    assert_eq!(ts2, ts1.into());
}

#[test]
fn test_hlc_tso_provider_on_failure() {
    let pd_cli = Arc::new(TestPdClient::new(1, false));

    let tso_refresh_interval = 200;

    pd_cli.set_tso(200.into());
    let provider =
        HlcProvider::new_opt(pd_cli.clone(), Duration::from_millis(tso_refresh_interval));
    assert!(provider.get_ts().is_err());

    block_on(provider.init()).unwrap();

    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 200.into(), "ts: {:?}", ts);

    pd_cli.set_tso(250.into());

    sleep_ms(tso_refresh_interval + tso_refresh_interval / 2);
    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 251.into(), "ts: {:?}", ts);

    // tso fail
    pd_cli.set_tso(300.into());
    pd_cli.trigger_tso_failure();

    sleep_ms(tso_refresh_interval);
    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 252.into(), "ts: {:?}", ts);

    sleep_ms(tso_refresh_interval);
    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 253.into(), "ts: {:?}", ts);

    sleep_ms(tso_refresh_interval);
    let res = provider.get_ts();
    assert!(res.is_err(), "res: {:?}", res);

    sleep_ms(tso_refresh_interval);
    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 300.into(), "ts: {:?}", ts);
}

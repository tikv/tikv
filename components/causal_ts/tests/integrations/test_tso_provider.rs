// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use std::sync::Arc;
use std::time::Duration;

use causal_ts::{BatchTsoProvider, CausalTsProvider, SimpleTsoProvider};
use test_raftstore::TestPdClient;
use txn_types::TimeStamp;

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
    pd_cli.set_tso(1000.into());

    // Set `renew_interval` to 0 to disable background renew. Invoke `flush()` to renew manually.
    // allocated: [1001, 1100]
    let provider = block_on(BatchTsoProvider::new_opt(pd_cli, Duration::ZERO, 100)).unwrap();
    assert_eq!(provider.batch_size(), 100);
    for ts in 1001..=1010u64 {
        assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
    }

    provider.flush().unwrap(); // allocated: [1101, 1200]
    assert_eq!(provider.batch_size(), 100);
    // used up
    for ts in 1101..=1200u64 {
        assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
    }
    assert!(provider.get_ts().is_err());

    provider.flush().unwrap(); // allocated: [1201, 1400]
    assert_eq!(provider.batch_size(), 200);

    // used < 20%
    for ts in 1201..=1249u64 {
        assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
    }

    provider.flush().unwrap(); // allocated: [1401, 1500]
    assert_eq!(provider.batch_size(), 100);

    for ts in 1401..=1500u64 {
        assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
    }
    assert!(provider.get_ts().is_err());
}

#[test]
fn test_batch_tso_provider_on_failure() {
    let pd_cli = Arc::new(TestPdClient::new(1, false));
    pd_cli.set_tso(1000.into());

    {
        pd_cli.trigger_tso_failure();
        assert!(
            block_on(BatchTsoProvider::new_opt(
                pd_cli.clone(),
                Duration::ZERO,
                100
            ))
            .is_err()
        );
    }

    // Set `renew_interval` to 0 to disable background renew. Invoke `flush()` to renew manually.
    // allocated: [1001, 1100]
    let provider = block_on(BatchTsoProvider::new_opt(
        pd_cli.clone(),
        Duration::ZERO,
        100,
    ))
    .unwrap();
    assert_eq!(provider.batch_size(), 100);
    for ts in 1001..=1010u64 {
        assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
    }

    pd_cli.trigger_tso_failure();
    for ts in 1011..=1020u64 {
        assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
    }

    assert!(provider.flush().is_err());
    assert!(provider.get_ts().is_err());

    pd_cli.trigger_tso_failure();
    assert!(provider.flush().is_err());
    assert!(provider.get_ts().is_err());

    provider.flush().unwrap(); // allocated: [1101, 1300]
    for ts in 1101..=1300u64 {
        assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
    }
    assert!(provider.get_ts().is_err());
}

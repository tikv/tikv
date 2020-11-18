// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This file contains tests and testing tools which affects multiple actions

use super::*;
use crate::storage::kv::WriteData;
use crate::storage::mvcc::tests::write;
use crate::storage::mvcc::{Error, Key, Mutation, MvccTxn, TimeStamp};
use crate::storage::{txn, Engine};
use concurrency_manager::ConcurrencyManager;
use kvproto::kvrpcpb::Context;
use prewrite::{pessimistic_prewrite, prewrite};

pub fn must_prewrite_put_impl<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: TimeStamp,
    is_pessimistic_lock: bool,
    lock_ttl: u64,
    for_update_ts: TimeStamp,
    txn_size: u64,
    min_commit_ts: TimeStamp,
    max_commit_ts: TimeStamp,
) {
    let ctx = Context::default();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let cm = ConcurrencyManager::new(ts);
    let mut txn = MvccTxn::new(snapshot, ts, true, cm);

    let mutation = Mutation::Put((Key::from_raw(key), value.to_vec()));
    if for_update_ts.is_zero() {
        prewrite(
            &mut txn,
            mutation,
            pk,
            &secondary_keys,
            false,
            lock_ttl,
            txn_size,
            min_commit_ts,
            max_commit_ts,
            false,
        )
        .unwrap();
    } else {
        pessimistic_prewrite(
            &mut txn,
            mutation,
            pk,
            &secondary_keys,
            is_pessimistic_lock,
            lock_ttl,
            for_update_ts,
            txn_size,
            min_commit_ts,
            max_commit_ts,
            false,
        )
        .unwrap();
    }
    write(engine, &ctx, txn.into_modifies());
}

pub fn must_prewrite_put<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) {
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts.into(),
        false,
        0,
        TimeStamp::default(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
    );
}

pub fn must_pessimistic_prewrite_put<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_lock: bool,
) {
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts.into(),
        is_pessimistic_lock,
        0,
        for_update_ts.into(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
    );
}

pub fn must_pessimistic_prewrite_put_with_ttl<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_lock: bool,
    lock_ttl: u64,
) {
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts.into(),
        is_pessimistic_lock,
        lock_ttl,
        for_update_ts.into(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
    );
}

pub fn must_prewrite_put_for_large_txn<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    ttl: u64,
    for_update_ts: impl Into<TimeStamp>,
) {
    let lock_ttl = ttl;
    let ts = ts.into();
    let min_commit_ts = (ts.into_inner() + 1).into();
    let for_update_ts = for_update_ts.into();
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts,
        !for_update_ts.is_zero(),
        lock_ttl,
        for_update_ts,
        0,
        min_commit_ts,
        TimeStamp::default(),
    );
}

pub fn must_prewrite_put_async_commit<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: impl Into<TimeStamp>,
    min_commit_ts: impl Into<TimeStamp>,
) {
    assert!(secondary_keys.is_some());
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        secondary_keys,
        ts.into(),
        false,
        100,
        TimeStamp::default(),
        0,
        min_commit_ts.into(),
        TimeStamp::default(),
    );
}

pub fn must_pessimistic_prewrite_put_async_commit<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_lock: bool,
    min_commit_ts: impl Into<TimeStamp>,
) {
    assert!(secondary_keys.is_some());
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        secondary_keys,
        ts.into(),
        is_pessimistic_lock,
        100,
        for_update_ts.into(),
        0,
        min_commit_ts.into(),
        TimeStamp::default(),
    );
}

fn must_prewrite_put_err_impl<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_lock: bool,
) -> Error {
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let for_update_ts = for_update_ts.into();
    let cm = ConcurrencyManager::new(for_update_ts);
    let mut txn = MvccTxn::new(snapshot, ts.into(), true, cm);
    let mutation = Mutation::Put((Key::from_raw(key), value.to_vec()));

    if for_update_ts.is_zero() {
        prewrite(
            &mut txn,
            mutation,
            pk,
            &None,
            false,
            0,
            0,
            TimeStamp::default(),
            TimeStamp::default(),
            false,
        )
        .unwrap_err()
    } else {
        pessimistic_prewrite(
            &mut txn,
            mutation,
            pk,
            &None,
            is_pessimistic_lock,
            0,
            for_update_ts,
            0,
            TimeStamp::default(),
            TimeStamp::default(),
            false,
        )
        .unwrap_err()
    }
}

pub fn must_prewrite_put_err<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) -> Error {
    must_prewrite_put_err_impl(engine, key, value, pk, ts, TimeStamp::zero(), false)
}

pub fn must_pessimistic_prewrite_put_err<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_lock: bool,
) -> Error {
    must_prewrite_put_err_impl(
        engine,
        key,
        value,
        pk,
        ts,
        for_update_ts,
        is_pessimistic_lock,
    )
}

fn must_prewrite_delete_impl<E: Engine>(
    engine: &E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_lock: bool,
) {
    let ctx = Context::default();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let for_update_ts = for_update_ts.into();
    let cm = ConcurrencyManager::new(for_update_ts);
    let mut txn = MvccTxn::new(snapshot, ts.into(), true, cm);
    let mutation = Mutation::Delete(Key::from_raw(key));

    if for_update_ts.is_zero() {
        prewrite(
            &mut txn,
            mutation,
            pk,
            &None,
            false,
            0,
            0,
            TimeStamp::default(),
            TimeStamp::default(),
            false,
        )
        .unwrap();
    } else {
        pessimistic_prewrite(
            &mut txn,
            mutation,
            pk,
            &None,
            is_pessimistic_lock,
            0,
            for_update_ts,
            0,
            TimeStamp::default(),
            TimeStamp::default(),
            false,
        )
        .unwrap();
    }
    engine
        .write(&ctx, WriteData::from_modifies(txn.into_modifies()))
        .unwrap();
}

pub fn must_prewrite_delete<E: Engine>(
    engine: &E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) {
    must_prewrite_delete_impl(engine, key, pk, ts, TimeStamp::zero(), false);
}

pub fn must_pessimistic_prewrite_delete<E: Engine>(
    engine: &E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_lock: bool,
) {
    must_prewrite_delete_impl(engine, key, pk, ts, for_update_ts, is_pessimistic_lock);
}

fn must_prewrite_lock_impl<E: Engine>(
    engine: &E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_lock: bool,
) {
    let ctx = Context::default();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let for_update_ts = for_update_ts.into();
    let cm = ConcurrencyManager::new(for_update_ts);
    let mut txn = MvccTxn::new(snapshot, ts.into(), true, cm);

    let mutation = Mutation::Lock(Key::from_raw(key));
    if for_update_ts.is_zero() {
        prewrite(
            &mut txn,
            mutation,
            pk,
            &None,
            false,
            0,
            0,
            TimeStamp::default(),
            TimeStamp::default(),
            false,
        )
        .unwrap();
    } else {
        pessimistic_prewrite(
            &mut txn,
            mutation,
            pk,
            &None,
            is_pessimistic_lock,
            0,
            for_update_ts,
            0,
            TimeStamp::default(),
            TimeStamp::default(),
            false,
        )
        .unwrap();
    }
    engine
        .write(&ctx, WriteData::from_modifies(txn.into_modifies()))
        .unwrap();
}

pub fn must_prewrite_lock<E: Engine>(engine: &E, key: &[u8], pk: &[u8], ts: impl Into<TimeStamp>) {
    must_prewrite_lock_impl(engine, key, pk, ts, TimeStamp::zero(), false);
}

pub fn must_prewrite_lock_err<E: Engine>(
    engine: &E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) {
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let ts = ts.into();
    let cm = ConcurrencyManager::new(ts);
    let mut txn = MvccTxn::new(snapshot, ts, true, cm);

    assert!(prewrite(
        &mut txn,
        Mutation::Lock(Key::from_raw(key)),
        pk,
        &None,
        false,
        0,
        0,
        TimeStamp::default(),
        TimeStamp::default(),
        false,
    )
    .is_err());
}

pub fn must_pessimistic_prewrite_lock<E: Engine>(
    engine: &E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_lock: bool,
) {
    must_prewrite_lock_impl(engine, key, pk, ts, for_update_ts, is_pessimistic_lock);
}

pub fn must_rollback<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) {
    let ctx = Context::default();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let start_ts = start_ts.into();
    let cm = ConcurrencyManager::new(start_ts);
    let mut txn = MvccTxn::new(snapshot, start_ts, true, cm);
    txn.collapse_rollback(false);
    txn::cleanup(&mut txn, Key::from_raw(key), TimeStamp::zero(), false).unwrap();
    write(engine, &ctx, txn.into_modifies());
}

pub fn must_rollback_collapsed<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) {
    let ctx = Context::default();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let start_ts = start_ts.into();
    let cm = ConcurrencyManager::new(start_ts);
    let mut txn = MvccTxn::new(snapshot, start_ts, true, cm);
    txn::cleanup(&mut txn, Key::from_raw(key), TimeStamp::zero(), false).unwrap();
    write(engine, &ctx, txn.into_modifies());
}

pub fn must_rollback_err<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) {
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let start_ts = start_ts.into();
    let cm = ConcurrencyManager::new(start_ts);
    let mut txn = MvccTxn::new(snapshot, start_ts, true, cm);
    assert!(txn::cleanup(&mut txn, Key::from_raw(key), TimeStamp::zero(), false).is_err());
}

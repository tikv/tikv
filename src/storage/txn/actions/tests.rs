// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This file contains tests and testing tools which affects multiple actions

use super::*;
use crate::storage::kv::WriteData;
use crate::storage::mvcc::tests::write;
use crate::storage::mvcc::{Error, Key, Mutation, MvccTxn, TimeStamp};
use crate::storage::{txn, Engine};
use concurrency_manager::ConcurrencyManager;
use kvproto::kvrpcpb::Context;
use prewrite::{prewrite, CommitKind, TransactionKind, TransactionProperties};

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
    let txn_kind = if for_update_ts.is_zero() {
        TransactionKind::Optimistic(false)
    } else {
        TransactionKind::Pessimistic(for_update_ts)
    };
    let commit_kind = if secondary_keys.is_some() {
        CommitKind::Async(max_commit_ts)
    } else {
        CommitKind::TwoPc
    };
    prewrite(
        &TransactionProperties {
            start_ts: ts,
            kind: txn_kind,
            commit_kind,
            primary: pk,
            txn_size,
            lock_ttl,
            min_commit_ts,
        },
        &mut txn,
        mutation,
        &secondary_keys,
        is_pessimistic_lock,
    )
    .unwrap();
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
    let ts = ts.into();
    let mut txn = MvccTxn::new(snapshot, ts, true, cm);
    let mutation = Mutation::Put((Key::from_raw(key), value.to_vec()));

    let txn_kind = if for_update_ts.is_zero() {
        TransactionKind::Optimistic(false)
    } else {
        TransactionKind::Pessimistic(for_update_ts)
    };
    prewrite(
        &TransactionProperties {
            start_ts: ts,
            kind: txn_kind,
            commit_kind: CommitKind::TwoPc,
            primary: pk,
            txn_size: 0,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
        },
        &mut txn,
        mutation,
        &None,
        is_pessimistic_lock,
    )
    .unwrap_err()
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
    let ts = ts.into();
    let mut txn = MvccTxn::new(snapshot, ts, true, cm);
    let mutation = Mutation::Delete(Key::from_raw(key));

    let txn_kind = if for_update_ts.is_zero() {
        TransactionKind::Optimistic(false)
    } else {
        TransactionKind::Pessimistic(for_update_ts)
    };
    prewrite(
        &TransactionProperties {
            start_ts: ts,
            kind: txn_kind,
            commit_kind: CommitKind::TwoPc,
            primary: pk,
            txn_size: 0,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
        },
        &mut txn,
        mutation,
        &None,
        is_pessimistic_lock,
    )
    .unwrap();

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
    let ts = ts.into();
    let mut txn = MvccTxn::new(snapshot, ts, true, cm);

    let mutation = Mutation::Lock(Key::from_raw(key));
    let txn_kind = if for_update_ts.is_zero() {
        TransactionKind::Optimistic(false)
    } else {
        TransactionKind::Pessimistic(for_update_ts)
    };
    prewrite(
        &TransactionProperties {
            start_ts: ts,
            kind: txn_kind,
            commit_kind: CommitKind::TwoPc,
            primary: pk,
            txn_size: 0,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
        },
        &mut txn,
        mutation,
        &None,
        is_pessimistic_lock,
    )
    .unwrap();

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
        &TransactionProperties {
            start_ts: ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: pk,
            txn_size: 0,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
        },
        &mut txn,
        Mutation::Lock(Key::from_raw(key)),
        &None,
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

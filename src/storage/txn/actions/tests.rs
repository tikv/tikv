// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This file contains tests and testing tools which affects multiple actions

use super::*;
use crate::storage::kv::WriteData;
use crate::storage::mvcc::tests::write;
use crate::storage::mvcc::{Error, Key, Mutation, MvccTxn, SnapshotReader, TimeStamp};
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
    is_retry_request: bool,
) {
    let ctx = Context::default();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let cm = ConcurrencyManager::new(ts);
    let mut txn = MvccTxn::new(ts, cm);
    let mut reader = SnapshotReader::new(ts, snapshot, true);

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
        &mut txn,
        &mut reader,
        &TransactionProperties {
            start_ts: ts,
            kind: txn_kind,
            commit_kind,
            primary: pk,
            txn_size,
            lock_ttl,
            min_commit_ts,
            need_old_value: false,
            is_retry_request,
        },
        mutation,
        secondary_keys,
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
        false,
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
        false,
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
        false,
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
        false,
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
        false,
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
        false,
    );
}

fn default_txn_props(
    start_ts: TimeStamp,
    primary: &[u8],
    for_update_ts: TimeStamp,
) -> TransactionProperties {
    let kind = if for_update_ts.is_zero() {
        TransactionKind::Optimistic(false)
    } else {
        TransactionKind::Pessimistic(for_update_ts)
    };

    TransactionProperties {
        start_ts,
        kind,
        commit_kind: CommitKind::TwoPc,
        primary,
        txn_size: 0,
        lock_ttl: 0,
        min_commit_ts: TimeStamp::default(),
        need_old_value: false,
        is_retry_request: false,
    }
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
    let mut txn = MvccTxn::new(ts, cm);
    let mut reader = SnapshotReader::new(ts, snapshot, true);
    let mutation = Mutation::Put((Key::from_raw(key), value.to_vec()));

    prewrite(
        &mut txn,
        &mut reader,
        &default_txn_props(ts, pk, for_update_ts),
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
    let mut txn = MvccTxn::new(ts, cm);
    let mut reader = SnapshotReader::new(ts, snapshot, true);
    let mutation = Mutation::Delete(Key::from_raw(key));

    prewrite(
        &mut txn,
        &mut reader,
        &default_txn_props(ts, pk, for_update_ts),
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
    let mut txn = MvccTxn::new(ts, cm);
    let mut reader = SnapshotReader::new(ts, snapshot, true);

    let mutation = Mutation::Lock(Key::from_raw(key));
    prewrite(
        &mut txn,
        &mut reader,
        &default_txn_props(ts, pk, for_update_ts),
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
    let mut txn = MvccTxn::new(ts, cm);
    let mut reader = SnapshotReader::new(ts, snapshot, true);

    assert!(
        prewrite(
            &mut txn,
            &mut reader,
            &default_txn_props(ts, pk, TimeStamp::zero()),
            Mutation::Lock(Key::from_raw(key)),
            &None,
            false,
        )
        .is_err()
    );
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

pub fn must_rollback<E: Engine>(
    engine: &E,
    key: &[u8],
    start_ts: impl Into<TimeStamp>,
    protect_rollback: bool,
) {
    let ctx = Context::default();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let start_ts = start_ts.into();
    let cm = ConcurrencyManager::new(start_ts);
    let mut txn = MvccTxn::new(start_ts, cm);
    let mut reader = SnapshotReader::new(start_ts, snapshot, true);
    txn::cleanup(
        &mut txn,
        &mut reader,
        Key::from_raw(key),
        TimeStamp::zero(),
        protect_rollback,
    )
    .unwrap();
    write(engine, &ctx, txn.into_modifies());
}

pub fn must_rollback_err<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) {
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let start_ts = start_ts.into();
    let cm = ConcurrencyManager::new(start_ts);
    let mut txn = MvccTxn::new(start_ts, cm);
    let mut reader = SnapshotReader::new(start_ts, snapshot, true);
    assert!(
        txn::cleanup(
            &mut txn,
            &mut reader,
            Key::from_raw(key),
            TimeStamp::zero(),
            false,
        )
        .is_err()
    );
}

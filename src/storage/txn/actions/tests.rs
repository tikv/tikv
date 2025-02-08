// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This file contains tests and testing tools which affects multiple actions

use std::sync::Arc;

use concurrency_manager::ConcurrencyManager;
use kvproto::kvrpcpb::{
    Assertion, AssertionLevel, Context, ExtraOp,
    PrewriteRequestPessimisticAction::{self, *},
};
use prewrite::{prewrite, CommitKind, TransactionKind, TransactionProperties};
use tikv_kv::{SnapContext, Statistics};

use super::*;
use crate::storage::{
    kv::WriteData,
    lock_manager::MockLockManager,
    mvcc::{tests::write, Error, Key, Mutation, MvccTxn, SnapshotReader, TimeStamp},
    txn,
    txn::{
        commands::{Flush, WriteContext, WriteResult},
        txn_status_cache::TxnStatusCache,
    },
    Engine,
};

pub fn must_prewrite_put_impl<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: TimeStamp,
    pessimistic_action: PrewriteRequestPessimisticAction,
    lock_ttl: u64,
    for_update_ts: TimeStamp,
    txn_size: u64,
    min_commit_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    is_retry_request: bool,
    assertion: Assertion,
    assertion_level: AssertionLevel,
) -> TimeStamp {
    must_prewrite_put_impl_with_should_not_exist(
        engine,
        key,
        value,
        pk,
        secondary_keys,
        ts,
        pessimistic_action,
        None,
        lock_ttl,
        for_update_ts,
        txn_size,
        min_commit_ts,
        max_commit_ts,
        is_retry_request,
        assertion,
        assertion_level,
        false,
        None,
        0,
    )
}

pub fn must_prewrite_insert_impl<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: TimeStamp,
    pessimistic_action: PrewriteRequestPessimisticAction,
    lock_ttl: u64,
    for_update_ts: TimeStamp,
    txn_size: u64,
    min_commit_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    is_retry_request: bool,
    assertion: Assertion,
    assertion_level: AssertionLevel,
) {
    must_prewrite_put_impl_with_should_not_exist(
        engine,
        key,
        value,
        pk,
        secondary_keys,
        ts,
        pessimistic_action,
        None,
        lock_ttl,
        for_update_ts,
        txn_size,
        min_commit_ts,
        max_commit_ts,
        is_retry_request,
        assertion,
        assertion_level,
        true,
        None,
        0,
    );
}

pub fn must_prewrite_put_impl_with_should_not_exist<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: TimeStamp,
    pessimistic_action: PrewriteRequestPessimisticAction,
    expected_for_update_ts: Option<TimeStamp>,
    lock_ttl: u64,
    for_update_ts: TimeStamp,
    txn_size: u64,
    min_commit_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    is_retry_request: bool,
    assertion: Assertion,
    assertion_level: AssertionLevel,
    should_not_exist: bool,
    region_id: Option<u64>,
    txn_source: u64,
) -> TimeStamp {
    let mut ctx = Context::default();
    ctx.set_txn_source(txn_source);
    if let Some(region_id) = region_id {
        ctx.region_id = region_id;
    }
    let snap_ctx = SnapContext {
        pb_ctx: &ctx,
        ..Default::default()
    };
    let snapshot = engine.snapshot(snap_ctx).unwrap();
    let cm = ConcurrencyManager::new(ts);
    let mut txn = MvccTxn::new(ts, cm);
    let mut reader = SnapshotReader::new(ts, snapshot, true);

    let mutation = if should_not_exist {
        Mutation::Insert((Key::from_raw(key), value.to_vec()), assertion)
    } else {
        Mutation::Put((Key::from_raw(key), value.to_vec()), assertion)
    };
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
    let (min_commit_ts, _) = prewrite(
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
            assertion_level,
            txn_source,
        },
        mutation,
        secondary_keys,
        pessimistic_action,
        expected_for_update_ts,
    )
    .unwrap();
    write(engine, &ctx, txn.into_modifies());
    min_commit_ts
}

pub fn must_prewrite_put<E: Engine>(
    engine: &mut E,
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
        SkipPessimisticCheck,
        0,
        TimeStamp::default(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    );
}

pub fn flush_put_impl<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: impl Into<Vec<u8>>,
    pk: impl Into<Vec<u8>>,
    start_ts: impl Into<TimeStamp>,
    generation: u64,
    should_not_exist: bool,
) -> txn::Result<WriteResult> {
    flush_put_impl_with_assertion(
        engine,
        key,
        value,
        pk,
        start_ts,
        generation,
        should_not_exist,
        Assertion::None,
    )
}

pub fn flush_put_impl_with_assertion<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: impl Into<Vec<u8>>,
    pk: impl Into<Vec<u8>>,
    start_ts: impl Into<TimeStamp>,
    generation: u64,
    should_not_exist: bool,
    assertion: Assertion,
) -> txn::Result<WriteResult> {
    let key = Key::from_raw(key);
    let start_ts = start_ts.into();
    let mut m = if should_not_exist {
        Mutation::make_insert(key, value.into())
    } else {
        Mutation::make_put(key, value.into())
    };
    m.set_assertion(assertion);
    let cmd = Flush::new(
        start_ts,
        pk.into(),
        vec![m],
        generation,
        3000,
        AssertionLevel::Strict,
        Context::new(),
    );
    let mut statistics = Statistics::default();
    let cm = ConcurrencyManager::new(start_ts);
    let context = WriteContext {
        lock_mgr: &MockLockManager::new(),
        concurrency_manager: cm.clone(),
        extra_op: ExtraOp::Noop,
        statistics: &mut statistics,
        async_apply_prewrite: false,
        raw_ext: None,
        txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
    };
    let snapshot = engine.snapshot(Default::default()).unwrap();
    cmd.cmd.process_write(snapshot.clone(), context)
}

pub fn must_flush_put<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: impl Into<Vec<u8>>,
    pk: impl Into<Vec<u8>>,
    start_ts: impl Into<TimeStamp>,
    generation: u64,
) {
    let res = flush_put_impl(engine, key, value, pk, start_ts, generation, false);
    assert!(res.is_ok());
    let res = res.unwrap();
    let to_be_write = res.to_be_write;
    if to_be_write.modifies.is_empty() {
        return;
    }
    engine.write(&Context::new(), to_be_write).unwrap();
}

pub fn must_prewrite_put_on_region<E: Engine>(
    engine: &mut E,
    region_id: u64,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) {
    must_prewrite_put_impl_with_should_not_exist(
        engine,
        key,
        value,
        pk,
        &None,
        ts.into(),
        SkipPessimisticCheck,
        None,
        0,
        TimeStamp::default(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
        false,
        Some(region_id),
        0,
    );
}

pub fn must_prewrite_put_with_txn_soucre<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    txn_source: u64,
) {
    must_prewrite_put_impl_with_should_not_exist(
        engine,
        key,
        value,
        pk,
        &None,
        ts.into(),
        SkipPessimisticCheck,
        None,
        0,
        TimeStamp::default(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
        false,
        None,
        txn_source,
    );
}

pub fn must_pessimistic_prewrite_put<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
) {
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts.into(),
        pessimistic_action,
        0,
        for_update_ts.into(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    );
}

pub fn must_pessimistic_prewrite_insert<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
) {
    must_prewrite_insert_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts.into(),
        pessimistic_action,
        0,
        for_update_ts.into(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    );
}

pub fn must_pessimistic_prewrite_put_with_ttl<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
    lock_ttl: u64,
) {
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts.into(),
        pessimistic_action,
        lock_ttl,
        for_update_ts.into(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    );
}

pub fn must_prewrite_put_for_large_txn<E: Engine>(
    engine: &mut E,
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
    let pessimistic_action = if !for_update_ts.is_zero() {
        DoPessimisticCheck
    } else {
        SkipPessimisticCheck
    };
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts,
        pessimistic_action,
        lock_ttl,
        for_update_ts,
        0,
        min_commit_ts,
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    );
}

pub fn must_prewrite_put_async_commit<E: Engine>(
    engine: &mut E,
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
        SkipPessimisticCheck,
        100,
        TimeStamp::default(),
        0,
        min_commit_ts.into(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    );
}

pub fn must_pessimistic_prewrite_put_async_commit<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
    min_commit_ts: impl Into<TimeStamp>,
) -> TimeStamp {
    assert!(secondary_keys.is_some());
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        secondary_keys,
        ts.into(),
        pessimistic_action,
        100,
        for_update_ts.into(),
        0,
        min_commit_ts.into(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    )
}

pub fn must_pessimistic_prewrite_put_check_for_update_ts<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    expected_for_update_ts: Option<u64>,
) {
    must_prewrite_put_impl_with_should_not_exist(
        engine,
        key,
        value,
        pk,
        &None,
        ts.into(),
        DoPessimisticCheck,
        expected_for_update_ts.map(Into::into),
        0,
        for_update_ts.into(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
        false,
        None,
        0,
    );
}

fn default_txn_props(
    start_ts: TimeStamp,
    primary: &[u8],
    for_update_ts: TimeStamp,
) -> TransactionProperties<'_> {
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
        assertion_level: AssertionLevel::Off,
        txn_source: 0,
    }
}

pub fn must_prewrite_put_err_impl<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
    max_commit_ts: impl Into<TimeStamp>,
    is_retry_request: bool,
    assertion: Assertion,
    assertion_level: AssertionLevel,
) -> Error {
    must_prewrite_put_err_impl_with_should_not_exist(
        engine,
        key,
        value,
        pk,
        secondary_keys,
        ts.into(),
        for_update_ts.into(),
        pessimistic_action,
        None,
        0,
        max_commit_ts.into(),
        is_retry_request,
        assertion,
        assertion_level,
        false,
    )
}

pub fn must_prewrite_insert_err_impl<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
    max_commit_ts: impl Into<TimeStamp>,
    is_retry_request: bool,
    assertion: Assertion,
    assertion_level: AssertionLevel,
) -> Error {
    must_prewrite_put_err_impl_with_should_not_exist(
        engine,
        key,
        value,
        pk,
        secondary_keys,
        ts.into(),
        for_update_ts.into(),
        pessimistic_action,
        None,
        0,
        max_commit_ts.into(),
        is_retry_request,
        assertion,
        assertion_level,
        true,
    )
}

pub fn must_prewrite_put_err_impl_with_should_not_exist<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
    expected_for_update_ts: Option<TimeStamp>,
    min_commit_ts: impl Into<TimeStamp>,
    max_commit_ts: impl Into<TimeStamp>,
    is_retry_request: bool,
    assertion: Assertion,
    assertion_level: AssertionLevel,
    should_not_exist: bool,
) -> Error {
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let for_update_ts = for_update_ts.into();
    let cm = ConcurrencyManager::new(for_update_ts);
    let ts = ts.into();
    let mut txn = MvccTxn::new(ts, cm);
    let mut reader = SnapshotReader::new(ts, snapshot, true);
    let mutation = if should_not_exist {
        Mutation::Insert((Key::from_raw(key), value.to_vec()), assertion)
    } else {
        Mutation::Put((Key::from_raw(key), value.to_vec()), assertion)
    };
    let commit_kind = if secondary_keys.is_some() {
        CommitKind::Async(max_commit_ts.into())
    } else {
        CommitKind::TwoPc
    };
    let mut props = default_txn_props(ts, pk, for_update_ts);
    props.is_retry_request = is_retry_request;
    props.commit_kind = commit_kind;
    props.assertion_level = assertion_level;
    props.min_commit_ts = min_commit_ts.into();

    prewrite(
        &mut txn,
        &mut reader,
        &props,
        mutation,
        secondary_keys,
        pessimistic_action,
        expected_for_update_ts,
    )
    .unwrap_err()
}

pub fn must_prewrite_put_err<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) -> Error {
    must_prewrite_put_err_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts,
        TimeStamp::zero(),
        SkipPessimisticCheck,
        0,
        false,
        Assertion::None,
        AssertionLevel::Off,
    )
}

pub fn must_pessimistic_prewrite_put_err<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
) -> Error {
    must_prewrite_put_err_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts,
        for_update_ts,
        pessimistic_action,
        0,
        false,
        Assertion::None,
        AssertionLevel::Off,
    )
}

pub fn must_pessimistic_prewrite_insert_err<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
) -> Error {
    must_prewrite_insert_err_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts,
        for_update_ts,
        pessimistic_action,
        0,
        false,
        Assertion::None,
        AssertionLevel::Off,
    )
}

pub fn must_pessimistic_prewrite_put_check_for_update_ts_err<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    expected_for_update_ts: Option<u64>,
) -> Error {
    must_prewrite_put_err_impl_with_should_not_exist(
        engine,
        key,
        value,
        pk,
        &None,
        ts,
        for_update_ts,
        DoPessimisticCheck,
        expected_for_update_ts.map(Into::into),
        0,
        0,
        false,
        Assertion::None,
        AssertionLevel::Off,
        false,
    )
}

pub fn must_retry_pessimistic_prewrite_put_err<E: Engine>(
    engine: &mut E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
    max_commit_ts: impl Into<TimeStamp>,
) -> Error {
    must_prewrite_put_err_impl(
        engine,
        key,
        value,
        pk,
        secondary_keys,
        ts,
        for_update_ts,
        pessimistic_action,
        max_commit_ts,
        true,
        Assertion::None,
        AssertionLevel::Off,
    )
}

fn must_prewrite_delete_impl<E: Engine>(
    engine: &mut E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
    region_id: Option<u64>,
) {
    let mut ctx = Context::default();
    if let Some(region_id) = region_id {
        ctx.region_id = region_id;
    }
    let snap_ctx = SnapContext {
        pb_ctx: &ctx,
        ..Default::default()
    };
    let snapshot = engine.snapshot(snap_ctx).unwrap();
    let for_update_ts = for_update_ts.into();
    let cm = ConcurrencyManager::new(for_update_ts);
    let ts = ts.into();
    let mut txn = MvccTxn::new(ts, cm);
    let mut reader = SnapshotReader::new(ts, snapshot, true);
    let mutation = Mutation::make_delete(Key::from_raw(key));

    prewrite(
        &mut txn,
        &mut reader,
        &default_txn_props(ts, pk, for_update_ts),
        mutation,
        &None,
        pessimistic_action,
        None,
    )
    .unwrap();

    engine
        .write(&ctx, WriteData::from_modifies(txn.into_modifies()))
        .unwrap();
}

pub fn must_prewrite_delete<E: Engine>(
    engine: &mut E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) {
    must_prewrite_delete_impl(
        engine,
        key,
        pk,
        ts,
        TimeStamp::zero(),
        SkipPessimisticCheck,
        None,
    );
}

pub fn must_prewrite_delete_on_region<E: Engine>(
    engine: &mut E,
    region_id: u64,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) {
    must_prewrite_delete_impl(
        engine,
        key,
        pk,
        ts,
        TimeStamp::zero(),
        SkipPessimisticCheck,
        Some(region_id),
    );
}

pub fn must_pessimistic_prewrite_delete<E: Engine>(
    engine: &mut E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
) {
    must_prewrite_delete_impl(engine, key, pk, ts, for_update_ts, pessimistic_action, None);
}

fn must_prewrite_lock_impl<E: Engine>(
    engine: &mut E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
) {
    let ctx = Context::default();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let for_update_ts = for_update_ts.into();
    let cm = ConcurrencyManager::new(for_update_ts);
    let ts = ts.into();
    let mut txn = MvccTxn::new(ts, cm);
    let mut reader = SnapshotReader::new(ts, snapshot, true);

    let mutation = Mutation::make_lock(Key::from_raw(key));
    prewrite(
        &mut txn,
        &mut reader,
        &default_txn_props(ts, pk, for_update_ts),
        mutation,
        &None,
        pessimistic_action,
        None,
    )
    .unwrap();

    engine
        .write(&ctx, WriteData::from_modifies(txn.into_modifies()))
        .unwrap();
}

pub fn must_prewrite_lock<E: Engine>(
    engine: &mut E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) {
    must_prewrite_lock_impl(engine, key, pk, ts, TimeStamp::zero(), SkipPessimisticCheck);
}

pub fn must_prewrite_lock_err<E: Engine>(
    engine: &mut E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) {
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let ts = ts.into();
    let cm = ConcurrencyManager::new(ts);
    let mut txn = MvccTxn::new(ts, cm);
    let mut reader = SnapshotReader::new(ts, snapshot, true);

    prewrite(
        &mut txn,
        &mut reader,
        &default_txn_props(ts, pk, TimeStamp::zero()),
        Mutation::make_lock(Key::from_raw(key)),
        &None,
        SkipPessimisticCheck,
        None,
    )
    .unwrap_err();
}

pub fn must_pessimistic_prewrite_lock<E: Engine>(
    engine: &mut E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    pessimistic_action: PrewriteRequestPessimisticAction,
) {
    must_prewrite_lock_impl(engine, key, pk, ts, for_update_ts, pessimistic_action);
}

pub fn must_rollback<E: Engine>(
    engine: &mut E,
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

pub fn must_rollback_err<E: Engine>(engine: &mut E, key: &[u8], start_ts: impl Into<TimeStamp>) {
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
        false,
    )
    .unwrap_err();
}

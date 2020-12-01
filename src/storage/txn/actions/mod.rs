// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This file contains the "actions" we perform on a [`MvccTxn`] and related tests
//! "Actions" here means a group of more basic operations,
//! eg. [`MvccTxn::load_lock`], [`MvccTxn::put_write`], which are methods on [`MvccTxn`],
//! for archiving a certain target

pub(crate) mod shared;

pub mod acquire_pessimistic_lock;
pub mod check_data_constraint;
pub mod check_txn_status;
pub mod cleanup;
pub mod commit;
pub mod gc;
pub mod pessimistic_prewrite;
pub mod prewrite;
pub mod tests;

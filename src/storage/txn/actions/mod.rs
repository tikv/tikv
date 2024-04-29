// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This file contains the "actions" we perform on a
//! [`crate::storage::mvcc::MvccTxn`] and related tests. "Actions" here means a
//! group of more basic operations, eg.
//! [`crate::storage::mvcc::MvccReader::load_lock`],
//! [`crate::storage::mvcc::MvccTxn::put_write`], which are methods on
//! [`crate::storage::mvcc::MvccTxn`], for archiving a certain target.

pub mod acquire_pessimistic_lock;
pub mod check_data_constraint;
pub mod check_txn_status;
pub mod cleanup;
pub mod commit;
pub mod common;
pub mod flashback_to_version;
pub mod gc;
pub mod prewrite;
pub mod tests;

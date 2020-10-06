// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This file contains the "actions" we perform on a [`MvccTxn`] and related tests
//! "Actions" here means a group of more basic operations,
//! eg. [`MvccTxn::load_lock`], [`MvccTxn::put_write`], which are methods on [`MvccTxn`],
//! for archiving a certain target

pub mod commit;
pub mod prewrite;
pub mod shared;

#[cfg(test)]
pub mod tests {
    // Todo: move tests in mvcc/txn.rs which tests cooperation of different actions here
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Handles requests that are dispatched to a user-supplied coprocessor plugin.
//!
//! Note: While there exists a coprocessor [TODO: reference] that can handle simple SQL queries,
//! this new coprocessor can handle arbitrary user requests, similar to the coprocessors in HBase and Google's BigQuery.
//!
//! Most TiDB read queries are processed by a coprocessor instead of the KV interface.
//! By doing so, the CPU of TiKV nodes can be utilized for computing and the
//! amount of data to transfer can be reduced (i.e. filtered at TiKV side).
// TODO: write more documentation here

mod endpoint;

pub use self::endpoint::CoprV2Endpoint;

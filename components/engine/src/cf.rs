// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub type CFName = &'static str;
pub const CF_DEFAULT: CFName = "default";
pub const CF_LOCK: CFName = "lock";
pub const CF_WRITE: CFName = "write";
pub const CF_RAFT: CFName = "raft";
// Cfs that should be very large generally.
pub const LARGE_CFS: &[CFName] = &[CF_DEFAULT, CF_WRITE];
pub const ALL_CFS: &[CFName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT];
pub const DATA_CFS: &[CFName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];

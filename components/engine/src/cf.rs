// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_LOCK: CfName = "lock";
pub const CF_RAFT: CfName = "raft";
pub const CF_LATEST: CfName = "latest"; // store the latest version
pub const CF_HISTORY: CfName = "history"; // store history versions
pub const CF_ROLLBACK: CfName = "rollback"; // store rollbacks

pub const CF_WRITE: CfName = "write"; // deprecated

// Cfs that should be very large generally.
pub const LARGE_CFS: &[CfName] = &[CF_DEFAULT, CF_LATEST, CF_HISTORY];
pub const ALL_CFS: &[CfName] = &[
    CF_DEFAULT,
    CF_LOCK,
    CF_LATEST,
    CF_HISTORY,
    CF_ROLLBACK,
    CF_RAFT,
    CF_WRITE,
];
pub const DATA_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_LATEST, CF_HISTORY, CF_ROLLBACK];

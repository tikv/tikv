// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_LOCK: CfName = "lock";
pub const CF_WRITE: CfName = "write";
pub const CF_RAFT: CfName = "raft";
pub const CF_VER_DEFAULT: CfName = "ver_default";
// Cfs that should be very large generally.
pub const LARGE_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];
pub const ALL_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT];
pub const DATA_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];
pub const ALL_CFS_IN_ENGINE: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT, CF_VER_DEFAULT];

pub fn name_to_cf(name: &str) -> Option<CfName> {
    if name.is_empty() {
        return Some(CF_DEFAULT);
    }
    for c in ALL_CFS {
        if name == *c {
            return Some(c);
        }
    }

    None
}

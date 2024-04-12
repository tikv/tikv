// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_LOCK: CfName = "lock";
pub const CF_WRITE: CfName = "write";
pub const CF_RAFT: CfName = "raft";
// Cfs that should be very large generally.
pub const LARGE_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];
pub const ALL_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT];
pub const DATA_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];
pub const DATA_CFS_LEN: usize = DATA_CFS.len();

pub fn data_cf_offset(cf: &str) -> usize {
    let cf = if cf.is_empty() { CF_DEFAULT } else { cf };
    DATA_CFS.iter().position(|c| *c == cf).expect(cf)
}

pub fn offset_to_cf(off: usize) -> &'static str {
    DATA_CFS[off]
}

pub fn name_to_cf(name: &str) -> Option<CfName> {
    if name.is_empty() {
        return Some(CF_DEFAULT);
    }
    ALL_CFS.iter().copied().find(|c| name == *c)
}

pub fn is_data_cf(cf: &str) -> bool {
    DATA_CFS.iter().any(|c| *c == cf)
}

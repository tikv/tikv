// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::cf_defs::*;

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

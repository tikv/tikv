// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::NUM_CFS;
use super::SkipList;

#[derive(Clone)]
pub struct CFTable {
    tbls: [SkipList; NUM_CFS],
}
// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use super::endpoint::RowColsDict;
use tipb::executor::{TableScan};
use tipb::select::ByItem;
use kvproto::coprocessor::KeyRange;
use storage::SnapshotStore;

pub trait Executor{
    fn SetSrcExec(e:Exector);
    fn Next()->Result<(u64,RowColsDict)>
};

struct TableScanExec<E:Exector> {
     executor:TableScan,
     col_ids:HashSet<i64>,
     key_ranges:Vec<KeyRange>,
     start_ts:u64,
     store:SnapshotStore,
     cursor:usize,
     seek_key:Vec<u8>,
     region_start:Vec<u8>,
     region_end:Vec<u8>,
     src:E,
};
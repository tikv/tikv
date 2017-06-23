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

use util::codec::table::RowColsDict;
use server::coprocessor::Result;

mod scanner;
pub mod table_scan;
pub mod selection;
pub mod index_scan;

// TODO:remove it
#[allow(dead_code)]
#[derive(Debug)]
pub struct Row {
    pub handle: i64,
    pub data: RowColsDict,
}

#[allow(dead_code)] // TODO:remove it
impl Row {
    pub fn new(handle: i64, data: RowColsDict) -> Row {
        Row {
            handle: handle,
            data: data,
        }
    }
}

pub trait Executor {
    fn next(&mut self) -> Result<Option<Row>>;
}

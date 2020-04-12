// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod point_getter;
mod reader;
mod scanner;

pub use self::point_getter::{PointGetter, PointGetterBuilder};
pub use self::reader::{check_need_gc, check_region_need_gc, MvccReader};
pub use self::scanner::test_util;
pub use self::scanner::{has_data_in_range, DeltaScanner, EntryScanner, Scanner, ScannerBuilder};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum NewerTsCheckState {
    Unknown,
    Met,
    NotMetYet,
}

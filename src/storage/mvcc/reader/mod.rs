// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod point_getter;
mod reader;
mod scanner;

pub use self::point_getter::{PointGetter, PointGetterBuilder};
#[cfg(test)]
pub use self::reader::tests as reader_tests;
pub use self::reader::{MvccReader, OverlappedWrite, SnapshotReader, TxnCommitRecord};
pub use self::scanner::test_util;
pub use self::scanner::{
    has_data_in_range, seek_for_valid_write, DeltaScanner, EntryScanner, Scanner, ScannerBuilder,
};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum NewerTsCheckState {
    Unknown,
    Met,
    NotMetYet,
}

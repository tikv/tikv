// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod reader;
mod scanner;
pub use self::reader::{extract_mvcc_props, need_gc, MvccReader};
pub use self::scanner::EntryScanner;
pub use self::scanner::{Scanner, ScannerBuilder};

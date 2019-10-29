// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::iterable::Iterable;

pub trait SstExt {
    type SstReader: SstReader;
}

/// SstReader is used to read an SST file.
pub trait SstReader: Iterable + Sized {
    fn open(path: &str) -> Result<Self>;
    fn verify_checksum(&self) -> Result<()>;
    fn iter(&self) -> Self::Iterator;
}

// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    ops::Deref,
};

use engine_traits::DbVector;
use rocksdb::DBVector as RawDBVector;

pub struct RocksDbVector(RawDBVector);

impl RocksDbVector {
    pub fn from_raw(raw: RawDBVector) -> RocksDbVector {
        RocksDbVector(raw)
    }
}

impl DbVector for RocksDbVector {}

impl Deref for RocksDbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for RocksDbVector {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a> PartialEq<&'a [u8]> for RocksDbVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    ops::Deref,
};

use engine_traits::DbVector;

pub struct PsDbVector(Vec<u8>);

impl PsDbVector {
    pub fn from_raw(raw: Vec<u8>) -> PsDbVector {
        PsDbVector(raw)
    }
}

impl DbVector for PsDbVector {}

impl Deref for PsDbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for PsDbVector {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a> PartialEq<&'a [u8]> for PsDbVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}

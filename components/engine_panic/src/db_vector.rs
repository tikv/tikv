// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;

use engine_traits::DbVector;

#[derive(Debug)]
pub struct PanicDbVector;

impl DbVector for PanicDbVector {}

impl Deref for PanicDbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        panic!()
    }
}

impl<'a> PartialEq<&'a [u8]> for PanicDbVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}

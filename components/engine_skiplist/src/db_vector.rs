// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::DBVector;
use std::ops::Deref;

#[derive(Debug)]
pub struct SkiplistDBVector(pub Vec<u8>);

impl DBVector for SkiplistDBVector {}

impl Deref for SkiplistDBVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.deref()
    }
}

impl<'a> PartialEq<&'a [u8]> for SkiplistDBVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}

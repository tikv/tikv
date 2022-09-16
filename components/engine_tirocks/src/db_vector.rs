// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    ops::Deref,
};

use tirocks::PinSlice;

#[derive(Default)]
pub struct RocksPinSlice(pub(crate) PinSlice);

impl engine_traits::DbVector for RocksPinSlice {}

impl Deref for RocksPinSlice {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for RocksPinSlice {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a> PartialEq<&'a [u8]> for RocksPinSlice {
    #[inline]
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}

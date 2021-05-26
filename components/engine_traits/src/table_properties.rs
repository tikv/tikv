// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::properties::DecodeProperties;

pub trait UserCollectedProperties: DecodeProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

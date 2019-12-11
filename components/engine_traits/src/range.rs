// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

/// A range of keys, `start_key` is included, but not `end_key`.
///
/// You should make sure `end_key` is not less than `start_key`.
pub struct Range<'a> {
    pub start_key: &'a [u8],
    pub end_key: &'a [u8],
}

impl<'a> Range<'a> {
    pub fn new(start_key: &'a [u8], end_key: &'a [u8]) -> Range<'a> {
        Range { start_key, end_key }
    }
}

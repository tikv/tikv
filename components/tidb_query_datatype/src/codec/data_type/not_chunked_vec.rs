// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

impl<'a, T: Evaluable> ChunkRef<'a, &'a T> for Vec<Option<T>> {
    fn get_option_ref(&'a self, idx: usize) -> Option<&'a T> {
        self[idx].as_ref()
    }
}

impl<'a> ChunkRef<'a, BytesRef<'a>> for Vec<Option<Bytes>> {
    fn get_option_ref(&'a self, idx: usize) -> Option<BytesRef<'a>> {
        self[idx].as_deref()
    }
}

impl<'a> ChunkRef<'a, JsonRef<'a>> for Vec<Option<Json>> {
    fn get_option_ref(&'a self, idx: usize) -> Option<JsonRef<'a>> {
        self[idx].as_ref().map(|x| x.as_ref())
    }
}

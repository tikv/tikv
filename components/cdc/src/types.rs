// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

static CONNECTION_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

/// A unique identifier of a CDC event feed connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnId(usize);

impl ConnId {
    pub fn new() -> ConnId {
        ConnId(CONNECTION_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

impl Default for ConnId {
    fn default() -> Self {
        Self::new()
    }
}

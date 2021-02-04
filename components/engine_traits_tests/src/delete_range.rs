// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{default_engine};
use engine_traits::{SyncMutable};
use std::panic::{self, AssertUnwindSafe};

#[test]
fn delete_range_cf_bad_cf() {
    let db = default_engine();
    assert!(panic::catch_unwind(AssertUnwindSafe(|| {
        db.engine.delete_range_cf("bogus", b"a", b"b").unwrap();
    })).is_err());
}

// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

pub trait SnapshotMiscExt {
    fn sequence_number(&self) -> u64;
}

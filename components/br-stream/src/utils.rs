// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use txn_types::{Key, TimeStamp};

/// wrap a user key with encoded data key.
pub fn wrap_key(mut v: Vec<u8>) -> Vec<u8> {
    v.insert(0, b'z');
    let key = Key::from_raw(v.as_slice());
    key.append_ts(TimeStamp::new(TimeStamp::physical_now()))
        .into_encoded()
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use txn_types::Key;

/// wrap a user key with encoded data key.
pub fn wrap_key(mut v: Vec<u8>) -> Vec<u8> {
    // TODO: encode in place.
    v.insert(0, b'z');
    let key = Key::from_raw(v.as_slice()).into_encoded();
    key
}

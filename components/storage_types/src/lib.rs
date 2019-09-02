#[macro_use]
extern crate quick_error;

pub mod mvcc;

use keys::{Key, Value};

// Short value max len must <= 255.
pub const SHORT_VALUE_MAX_LEN: usize = 64;
pub const SHORT_VALUE_PREFIX: u8 = b'v';
pub const FOR_UPDATE_TS_PREFIX: u8 = b'f';
pub const TXN_SIZE_PREFIX: u8 = b't';

/// A row mutation.
#[derive(Debug, Clone)]
pub enum Mutation {
    /// Put `Value` into `Key`, overwriting any existing value.
    Put((Key, Value)),
    /// Delete `Key`.
    Delete(Key),
    /// Set a lock on `Key`.
    Lock(Key),
    /// Put `Value` into `Key` if `Key` does not yet exist.
    ///
    /// Returns [`KeyError::AlreadyExists`](kvproto::kvrpcpb::KeyError::AlreadyExists) if the key already exists.
    Insert((Key, Value)),
}

impl Mutation {
    pub fn key(&self) -> &Key {
        match self {
            Mutation::Put((ref key, _)) => key,
            Mutation::Delete(ref key) => key,
            Mutation::Lock(ref key) => key,
            Mutation::Insert((ref key, _)) => key,
        }
    }

    pub fn into_key_value(self) -> (Key, Option<Value>) {
        match self {
            Mutation::Put((key, value)) => (key, Some(value)),
            Mutation::Delete(key) => (key, None),
            Mutation::Lock(key) => (key, None),
            Mutation::Insert((key, value)) => (key, Some(value)),
        }
    }

    pub fn is_insert(&self) -> bool {
        match self {
            Mutation::Insert(_) => true,
            _ => false,
        }
    }
}

use std::marker::PhantomData;

use engine_traits::{Error, Result};
use tikv_util::box_err;

use super::*;

const KEYSPACE_PREFIX_LEN: usize = 4;

pub trait Keyspace {
    fn parse_keyspace(key: &[u8]) -> Result<(Option<KeyspaceId>, &[u8])> {
        Ok((None, key))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct KeyspaceId(u32);

impl Keyspace for ApiV1 {}

impl Keyspace for ApiV1Ttl {}

impl Keyspace for ApiV2 {
    fn parse_keyspace(key: &[u8]) -> Result<(Option<KeyspaceId>, &[u8])> {
        if key.len() < KEYSPACE_PREFIX_LEN || ApiV2::parse_key_mode(key) == KeyMode::Unknown {
            return Err(Error::Other(box_err!(
                "invalid API V2 key: {}",
                log_wrappers::Value(key)
            )));
        }
        let id = u32::from_be_bytes([0, key[1], key[2], key[3]]);
        Ok((Some(KeyspaceId(id)), &key[KEYSPACE_PREFIX_LEN..]))
    }
}

pub struct KeyspaceKv<F> {
    keyspace: Option<KeyspaceId>,
    k: Vec<u8>,
    v: Vec<u8>,
    offset: usize,
    _phantom: PhantomData<F>,
}

impl<F: KvFormat> KeyspaceKv<F> {
    pub fn from_kv_pair((k, v): (Vec<u8>, Vec<u8>)) -> Result<Self> {
        let (keyspace, _) = F::parse_keyspace(&k)?;
        Ok(Self {
            keyspace,
            k,
            v,
            offset: if keyspace.is_some() {
                KEYSPACE_PREFIX_LEN
            } else {
                0
            },
            _phantom: PhantomData,
        })
    }

    pub fn key(&self) -> &[u8] {
        &self.k[self.offset..]
    }

    pub fn value(&self) -> &[u8] {
        &self.v
    }

    pub fn kv(&self) -> (&[u8], &[u8]) {
        (self.key(), self.value())
    }

    pub fn keyspace(&self) -> Option<KeyspaceId> {
        self.keyspace
    }
}

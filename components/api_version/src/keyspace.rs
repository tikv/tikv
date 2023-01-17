use std::fmt::Debug;

use engine_traits::{Error, Result};
use tikv_util::box_err;

use super::*;

const KEYSPACE_PREFIX_LEN: usize = 4;

pub trait KvPair {
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
    fn kv(&self) -> (&[u8], &[u8]) {
        (self.key(), self.value())
    }
}

impl KvPair for (Vec<u8>, Vec<u8>) {
    fn key(&self) -> &[u8] {
        &self.0
    }
    fn value(&self) -> &[u8] {
        &self.1
    }
}

pub trait Keyspace {
    type KvPair: KvPair = (Vec<u8>, Vec<u8>);
    fn make_kv_pair(p: (Vec<u8>, Vec<u8>)) -> Result<Self::KvPair>;
    fn parse_keyspace(key: &[u8]) -> Result<(Option<KeyspaceId>, &[u8])> {
        Ok((None, key))
    }
}

#[derive(PartialEq, Clone, Copy, Debug)]
pub struct KeyspaceId(u32);

impl From<u32> for KeyspaceId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl Keyspace for ApiV1 {
    fn make_kv_pair(p: (Vec<u8>, Vec<u8>)) -> Result<Self::KvPair> {
        Ok(p)
    }
}

impl Keyspace for ApiV1Ttl {
    fn make_kv_pair(p: (Vec<u8>, Vec<u8>)) -> Result<Self::KvPair> {
        Ok(p)
    }
}

impl Keyspace for ApiV2 {
    type KvPair = KeyspaceKv;

    fn make_kv_pair(p: (Vec<u8>, Vec<u8>)) -> Result<Self::KvPair> {
        let (k, v) = p;
        let (keyspace, _) = Self::parse_keyspace(&k)?;
        Ok(KeyspaceKv {
            k,
            v,
            keyspace: keyspace.unwrap(),
        })
    }

    fn parse_keyspace(key: &[u8]) -> Result<(Option<KeyspaceId>, &[u8])> {
        let mode = ApiV2::parse_key_mode(key);
        if key.len() < KEYSPACE_PREFIX_LEN || (mode != KeyMode::Raw && mode != KeyMode::Txn) {
            return Err(Error::Other(box_err!(
                "invalid API V2 key: {}",
                log_wrappers::Value(key)
            )));
        }
        let id = u32::from_be_bytes([0, key[1], key[2], key[3]]);
        Ok((Some(KeyspaceId::from(id)), &key[KEYSPACE_PREFIX_LEN..]))
    }
}

pub struct KeyspaceKv {
    k: Vec<u8>,
    v: Vec<u8>,
    keyspace: KeyspaceId,
}

impl KvPair for KeyspaceKv {
    fn key(&self) -> &[u8] {
        &self.k[KEYSPACE_PREFIX_LEN..]
    }

    fn value(&self) -> &[u8] {
        &self.v
    }
}

impl KeyspaceKv {
    pub fn keyspace(&self) -> KeyspaceId {
        self.keyspace
    }
}

impl PartialEq<(Vec<u8>, Vec<u8>)> for KeyspaceKv {
    fn eq(&self, other: &(Vec<u8>, Vec<u8>)) -> bool {
        self.kv() == (&other.0, &other.1)
    }
}

impl PartialEq for KeyspaceKv {
    fn eq(&self, other: &Self) -> bool {
        self.k == other.k && self.v == other.v
    }
}

impl Debug for KeyspaceKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyspaceKv")
            .field("key", &log_wrappers::Value(self.key()))
            .field("value", &log_wrappers::Value(self.value()))
            .field("keyspace", &self.keyspace())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v1_parse_keyspace() {
        let k = b"t123_111";
        let (keyspace, key) = ApiV1::parse_keyspace(k).unwrap();
        assert_eq!(None, keyspace);
        assert_eq!(k, key);

        let (keyspace, key) = ApiV1Ttl::parse_keyspace(k).unwrap();
        assert_eq!(None, keyspace);
        assert_eq!(k, key);
    }

    #[test]
    fn test_v2_parse_keyspace() {
        let ok = vec![
            (b"x\x00\x00\x01t123_114", 1, b"t123_114"),
            (b"r\x00\x00\x01t123_112", 1, b"t123_112"),
            (b"x\x01\x00\x00t213_112", 0x010000, b"t213_112"),
            (b"r\x01\x00\x00t123_113", 0x010000, b"t123_113"),
        ];

        for (key, id, user_key) in ok {
            let (keyspace, key) = ApiV2::parse_keyspace(key).unwrap();
            assert_eq!(Some(KeyspaceId::from(id)), keyspace);
            assert_eq!(user_key, key);
        }

        let err: Vec<&[u8]> = vec![b"t123_111", b"s\x00\x00", b"r\x00\x00"];

        for key in err {
            ApiV2::parse_keyspace(key).unwrap_err();
        }
    }
}

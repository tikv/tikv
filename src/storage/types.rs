use std::hash::{Hash, Hasher};
use kvproto::kvrpcpb::KeyAddress;
use kvproto::metapb::Peer;
use byteorder::{BigEndian, WriteBytesExt};

pub type Value = Vec<u8>;
pub type KvPair = (Vec<u8>, Value);

#[derive(Debug, Clone)]
pub struct Key {
    inner: KeyAddress,
}

impl Key {
    pub fn new(key_address: KeyAddress) -> Key {
        Key { inner: key_address }
    }

    pub fn set_rawkey(&mut self, key: Vec<u8>) {
        self.inner.set_key(key)
    }

    pub fn get_rawkey(&self) -> &[u8] {
        self.inner.get_key()
    }

    pub fn get_peer(&self) -> &Peer {
        self.inner.get_peer()
    }

    pub fn get_region_id(&self) -> u64 {
        self.inner.get_region_id()
    }

    pub fn encode_ts(&self, ts: u64) -> Key {
        let mut key_address = self.inner.clone();
        key_address.mut_key().write_u64::<BigEndian>(ts).unwrap();
        Key::new(key_address)
    }
}

impl From<KeyAddress> for Key {
    fn from(key_address: KeyAddress) -> Key {
        Key::new(key_address)
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get_rawkey().hash(state)
    }
}

#[cfg(test)]
pub fn make_key(k: &[u8]) -> Key {
    let mut key = Key::new(KeyAddress::default());
    key.set_rawkey(k.to_vec());
    key
}

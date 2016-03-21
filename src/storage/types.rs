use std::hash::{Hash, Hasher};
use kvproto::kvrpcpb::KeyAddress;
use kvproto::metapb::Peer;
use byteorder::{BigEndian, WriteBytesExt};
use util::codec::bytes;

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

    pub fn encode(&self) -> Key {
        let mut key_address = KeyAddress::new();
        key_address.set_key(bytes::encode_bytes(self.get_rawkey()));
        key_address.set_peer(self.inner.get_peer().clone());
        key_address.set_region_id(self.inner.get_region_id());
        Key::new(key_address)
    }

    pub fn encode_ts(&self, ts: u64) -> Key {
        let mut key_address = KeyAddress::new();
        let mut encoded_rawkey = bytes::encode_bytes(self.get_rawkey());
        encoded_rawkey.write_u64::<BigEndian>(ts).unwrap();
        key_address.set_key(encoded_rawkey);
        key_address.set_peer(self.inner.get_peer().clone());
        key_address.set_region_id(self.inner.get_region_id());
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

use std::hash::{Hash, Hasher};
use kvproto::metapb::Peer;
use byteorder::{BigEndian, WriteBytesExt};

pub type Value = Vec<u8>;
pub type KvPair = (Vec<u8>, Value);

#[derive(Debug, Clone)]
pub struct Key(Vec<u8>);

impl Key {
    pub fn new(key: Vec<u8>) -> Key {
        Key(key)
    }

    pub fn set(&mut self, key: Vec<u8>) {
        self.0 = key
    }

    pub fn raw(&self) -> &Vec<u8> {
        &self.0
    }

    pub fn encode_ts(&self, ts: u64) -> Key {
        let mut encoded = self.0.clone();
        encoded.write_u64::<BigEndian>(ts).unwrap();
        Key(encoded)
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.raw().hash(state)
    }
}

#[cfg(test)]
pub fn make_key(k: &[u8]) -> Key {
    use util::codec::bytes;
    Key::new(bytes::encode_bytes(k))
}

#[derive(Debug)]
pub struct KvContext {
    pub region_id: u64,
    pub peer: Peer,
}

impl KvContext {
    pub fn new(region_id: u64, peer: Peer) -> KvContext {
        KvContext {
            region_id: region_id,
            peer: peer,
        }
    }

    #[cfg(test)]
    pub fn none() -> KvContext {
        KvContext::new(0, Peer::new())
    }
}

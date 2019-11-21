use crate::codec::{Error, Result};
use openssl::hash::{self, MessageDigest};

#[inline]
pub fn hex_digest(hashtype: MessageDigest, input: &[u8]) -> Result<Vec<u8>> {
  hash::hash(hashtype, input)
    .map(|digest| hex::encode(digest).into_bytes())
    .map_err(|e| Error::Other(box_err!("OpenSSL error: {:?}", e)))
}

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use kvproto::encryptionpb::EncryptionMethod;
#[cfg(not(feature = "prost-codec"))]
use protobuf::ProtobufEnum;

use crate::{Error, Result};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum MetadataKey {
    Iv,
    EncryptionMethod,
    PlaintextSha256,
}

const METADATA_KEY_IV: &str = "IV";
const METADATA_KEY_ENCRYPTION_METHOD: &str = "encryption_method";
const METADATA_KEY_PLAINTEXT_SHA256: &str = "plaintext_sha256";

impl MetadataKey {
    pub fn as_str(self) -> &'static str {
        match self {
            MetadataKey::Iv => METADATA_KEY_IV,
            MetadataKey::EncryptionMethod => METADATA_KEY_ENCRYPTION_METHOD,
            MetadataKey::PlaintextSha256 => METADATA_KEY_PLAINTEXT_SHA256,
        }
    }
}

pub fn encode_ecryption_method(method: EncryptionMethod) -> Result<Vec<u8>> {
    let mut value = Vec::with_capacity(4); // Length of i32.
    value
        .write_i32::<BigEndian>(method as i32)
        .map_err(|e| Error::Other(e.into()))?;
    Ok(value)
}

pub fn decode_ecryption_method(mut value: &[u8]) -> Result<EncryptionMethod> {
    if value.len() != 4 {
        return Err(Error::Other(
            format!(
                "encryption method value length mismatch expect 4 get {}",
                value.len(),
            )
            .into(),
        ));
    }
    value
        .read_i32::<BigEndian>()
        .map(|v| EncryptionMethod::from_i32(v).unwrap())
        .map_err(|e| Error::Other(e.into()))
}

pub fn sha256(content: &[u8]) -> Result<Vec<u8>> {
    use openssl::hash::{self, MessageDigest};
    let h = hash::hash(MessageDigest::sha256(), content).map(|digest| digest.to_vec())?;
    Ok(h)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_method() {
        for m in EncryptionMethod::values() {
            let m = *m;
            let v = encode_ecryption_method(m).unwrap();
            let d = decode_ecryption_method(&v).unwrap();
            assert_eq!(m, d);
        }
    }
}

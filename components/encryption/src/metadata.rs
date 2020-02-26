use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use kvproto::encryptionpb::{
    DataKey, EncryptedContent, EncryptionMethod, FileDictionary, FileInfo,
};
use protobuf::ProtobufEnum;

use crate::{Error, Result};

pub const METADATA_KEY_IV: &str = "IV";
pub const METADATA_KEY_ENCRYPTION_METHOD: &str = "encryption_method";

pub fn encode_ecryption_method(method: EncryptionMethod) -> Result<Vec<u8>> {
    let mut value = Vec::with_capacity(4); // Length of i32.
    value
        .write_i32::<BigEndian>(method as i32)
        .map_err(|e| Error::Other(e.into()));
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

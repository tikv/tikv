// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Key prefix definistions and utils for API V2.

pub const TIDB_RANGES: &[(&[u8], &[u8])] = &[(&[b'm'], &[b'm' + 1]), (&[b't'], &[b't' + 1])];
pub const TIDB_RANGES_COMPLEMENT: &[(&[u8], &[u8])] =
    &[(&[], &[b'm']), (&[b'm' + 1], &[b't']), (&[b't' + 1], &[])];

pub const RAW_KEY_PREFIX: u8 = b'r';
pub const TXN_KEY_PREFIX: u8 = b'x';

/// Checks if the key is in TiDB encode.
///
/// Returning true doesn't mean that the key is certainly written by
/// TiDB, but instead, it matches the definition of TiDB key in API V2,
/// therefore, the key is treated as TiDB data in order to fulfill the
/// compatibility.
pub fn is_tidb_key(key: &[u8]) -> bool {
    matches!(KeyPrefix::parse(key).0, KeyPrefix::TiDB)
}

/// Checks if the key is in RawKV encode.
pub fn is_raw_key(key: &[u8]) -> bool {
    matches!(KeyPrefix::parse(key).0, KeyPrefix::Raw { .. })
}

/// Checks if the key is in TxnKV encode.
pub fn is_txn_key(key: &[u8]) -> bool {
    matches!(KeyPrefix::parse(key).0, KeyPrefix::Txn { .. })
}

/// The key prefix in API V2.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum KeyPrefix {
    /// Raw key prefix.
    Raw { keyspace_id: usize },
    /// Transaction key prefix.
    Txn { keyspace_id: usize },
    /// TiDB key prefix.
    TiDB,
    /// Unrecognised key prefix.
    Unknown,
}

impl KeyPrefix {
    /// Parse the keys prefix according to the API V2 definition and return the user key.
    pub fn parse(key: &[u8]) -> (KeyPrefix, &[u8]) {
        if key.is_empty() {
            return (KeyPrefix::Unknown, key);
        }

        match key[0] {
            RAW_KEY_PREFIX => unsigned_varint::decode::usize(&key[1..])
                .map(|(keyspace_id, rest)| (KeyPrefix::Raw { keyspace_id }, rest))
                .unwrap_or((KeyPrefix::Unknown, key)),
            TXN_KEY_PREFIX => unsigned_varint::decode::usize(&key[1..])
                .map(|(keyspace_id, rest)| (KeyPrefix::Txn { keyspace_id }, rest))
                .unwrap_or((KeyPrefix::Unknown, key)),
            b'm' | b't' => {
                // TiDB prefix is also a part of the user key, so don't strip the prefix.
                (KeyPrefix::TiDB, key)
            }
            _ => (KeyPrefix::Unknown, key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEYSPACE_ID_500: &[u8] = &[244, 3];

    #[test]
    fn test_keyspace_id() {
        let mut buf = [0; 10];
        let slice = unsigned_varint::encode::usize(500, &mut buf);
        assert_eq!(slice, KEYSPACE_ID_500);
    }

    #[test]
    fn test_parse() {
        assert_eq!(
            KeyPrefix::parse(&[RAW_KEY_PREFIX, 244, 3, b'a', b'b']),
            (KeyPrefix::Raw { keyspace_id: 500 }, &b"ab"[..])
        );
        assert_eq!(
            KeyPrefix::parse(&[TXN_KEY_PREFIX, 244, 3]),
            (KeyPrefix::Txn { keyspace_id: 500 }, &b""[..])
        );
        assert_eq!(KeyPrefix::parse(b"t_a"), (KeyPrefix::TiDB, &b"t_a"[..]));
        assert_eq!(KeyPrefix::parse(b"m"), (KeyPrefix::TiDB, &b"m"[..]));
        assert_eq!(KeyPrefix::parse(b"ot"), (KeyPrefix::Unknown, &b"ot"[..]));
        assert_eq!(
            KeyPrefix::parse(&[RAW_KEY_PREFIX, 244]),
            (KeyPrefix::Unknown, &[RAW_KEY_PREFIX, 244][..])
        );
    }
}

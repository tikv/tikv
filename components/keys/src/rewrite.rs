// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Key rewriting

/// An error indicating the key cannot be rewritten because it does not start
/// with the given prefix.
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct WrongPrefix;

/// Rewrites the prefix of a byte array.
pub fn rewrite_prefix(
    old_prefix: &[u8],
    new_prefix: &[u8],
    src: &[u8],
) -> Result<Vec<u8>, WrongPrefix> {
    if !src.starts_with(old_prefix) {
        return Err(WrongPrefix);
    }
    let mut result = Vec::with_capacity(src.len() - old_prefix.len() + new_prefix.len());
    result.extend_from_slice(new_prefix);
    result.extend_from_slice(&src[old_prefix.len()..]);
    Ok(result)
}

/// Rewrites the prefix of a byte array used as the end key.
///
/// Besides values supported by `rewrite_prefix`, if the src is exactly the
/// successor of `old_prefix`, this method will also return a Ok with the
/// successor of `new_prefix`.
pub fn rewrite_prefix_of_end_key(
    old_prefix: &[u8],
    new_prefix: &[u8],
    src: &[u8],
) -> Result<Vec<u8>, WrongPrefix> {
    if let dest @ Ok(_) = rewrite_prefix(old_prefix, new_prefix, src) {
        return dest;
    }

    if super::next_key_no_alloc(old_prefix) != src.split_last().map(|(&e, s)| (s, e)) {
        // src is not the sucessor of old_prefix
        return Err(WrongPrefix);
    }

    Ok(super::next_key(new_prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_prefix() {
        assert_eq!(
            rewrite_prefix(b"t123", b"t456", b"t123789"),
            Ok(b"t456789".to_vec()),
        );
        assert_eq!(
            rewrite_prefix(b"", b"t654", b"321"),
            Ok(b"t654321".to_vec()),
        );
        assert_eq!(
            rewrite_prefix(b"t234", b"t567", b"t567890"),
            Err(WrongPrefix),
        );
        assert_eq!(rewrite_prefix(b"t123", b"t567", b"t124"), Err(WrongPrefix),);
    }

    #[test]
    fn test_rewrite_prefix_of_end_key() {
        assert_eq!(
            rewrite_prefix_of_end_key(b"t123", b"t456", b"t123789"),
            Ok(b"t456789".to_vec()),
        );
        assert_eq!(
            rewrite_prefix_of_end_key(b"", b"t654", b"321"),
            Ok(b"t654321".to_vec()),
        );
        assert_eq!(
            rewrite_prefix_of_end_key(b"t234", b"t567", b"t567890"),
            Err(WrongPrefix),
        );
        assert_eq!(
            rewrite_prefix_of_end_key(b"t123", b"t567", b"t124"),
            Ok(b"t568".to_vec()),
        );
        assert_eq!(
            rewrite_prefix_of_end_key(b"t135\xff\xff", b"t248\xff\xff\xff", b"t136"),
            Ok(b"t249".to_vec()),
        );
        assert_eq!(
            rewrite_prefix_of_end_key(b"t147", b"t258", b"t148\xff\xff"),
            Err(WrongPrefix),
        );
        assert_eq!(
            rewrite_prefix_of_end_key(b"\xff\xff", b"\xff\xfe", b""),
            Ok(b"\xff\xff".to_vec()),
        );
        assert_eq!(
            rewrite_prefix_of_end_key(b"\xff\xfe", b"\xff\xff", b"\xff\xff"),
            Ok(b"".to_vec()),
        );
    }
}

// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Key rewriting

use std::ops::Bound::{self, *};

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
        // src is not the successor of old_prefix
        return Err(WrongPrefix);
    }

    Ok(super::next_key(new_prefix))
}

pub fn rewrite_prefix_of_start_bound(
    old_prefix: &[u8],
    new_prefix: &[u8],
    start: Bound<&[u8]>,
) -> Result<Bound<Vec<u8>>, WrongPrefix> {
    Ok(match start {
        Unbounded => {
            if old_prefix.is_empty() {
                Included(new_prefix.to_vec())
            } else {
                Unbounded
            }
        }
        Included(s) => Included(rewrite_prefix(old_prefix, new_prefix, s)?),
        Excluded(s) => Excluded(rewrite_prefix(old_prefix, new_prefix, s)?),
    })
}

pub fn rewrite_prefix_of_end_bound(
    old_prefix: &[u8],
    new_prefix: &[u8],
    end: Bound<&[u8]>,
) -> Result<Bound<Vec<u8>>, WrongPrefix> {
    fn end_key_to_bound(end_key: Vec<u8>) -> Bound<Vec<u8>> {
        if end_key.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(end_key)
        }
    }

    Ok(match end {
        Unbounded => {
            if old_prefix.is_empty() {
                end_key_to_bound(super::next_key(new_prefix))
            } else {
                Unbounded
            }
        }
        Included(e) => Included(rewrite_prefix(old_prefix, new_prefix, e)?),
        Excluded(e) => end_key_to_bound(rewrite_prefix_of_end_key(old_prefix, new_prefix, e)?),
    })
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

    #[test]
    fn test_rewrite_prefix_of_range() {
        use std::ops::Bound::*;

        let rewrite_prefix_of_range =
            |old_prefix: &[u8], new_prefix: &[u8], start: Bound<&[u8]>, end: Bound<&[u8]>| {
                Ok((
                    rewrite_prefix_of_start_bound(old_prefix, new_prefix, start)?,
                    rewrite_prefix_of_end_bound(old_prefix, new_prefix, end)?,
                ))
            };

        assert_eq!(
            rewrite_prefix_of_range(b"t123", b"t456", Included(b"t123456"), Included(b"t123789")),
            Ok((Included(b"t456456".to_vec()), Included(b"t456789".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_range(b"t123", b"t456", Included(b"t123456"), Excluded(b"t123789")),
            Ok((Included(b"t456456".to_vec()), Excluded(b"t456789".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_range(b"t123", b"t456", Excluded(b"t123456"), Excluded(b"t123789")),
            Ok((Excluded(b"t456456".to_vec()), Excluded(b"t456789".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_range(b"t123", b"t456", Excluded(b"t123456"), Included(b"t123789")),
            Ok((Excluded(b"t456456".to_vec()), Included(b"t456789".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_range(b"t123", b"t456", Included(b"t123789"), Unbounded),
            Ok((Included(b"t456789".to_vec()), Unbounded)),
        );
        assert_eq!(
            rewrite_prefix_of_range(b"t123", b"t456", Unbounded, Unbounded),
            Ok((Unbounded, Unbounded)),
        );

        assert_eq!(
            rewrite_prefix_of_range(b"", b"t654", Included(b"321"), Included(b"987")),
            Ok((Included(b"t654321".to_vec()), Included(b"t654987".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_range(b"", b"t654", Included(b"321"), Excluded(b"987")),
            Ok((Included(b"t654321".to_vec()), Excluded(b"t654987".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_range(b"", b"t654", Included(b"321"), Unbounded),
            Ok((Included(b"t654321".to_vec()), Excluded(b"t655".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_range(b"", b"t654", Unbounded, Included(b"987")),
            Ok((Included(b"t654".to_vec()), Included(b"t654987".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_range(b"", b"t654", Unbounded, Unbounded),
            Ok((Included(b"t654".to_vec()), Excluded(b"t655".to_vec()))),
        );

        assert_eq!(
            rewrite_prefix_of_range(
                b"\xff\xfe",
                b"\xff\xff",
                Included(b"\xff\xfe"),
                Excluded(b"\xff\xff")
            ),
            Ok((Included(b"\xff\xff".to_vec()), Unbounded)),
        );
        assert_eq!(
            rewrite_prefix_of_range(
                b"\xff\xfe",
                b"\xff\xff",
                Excluded(b"\xff\xfe"),
                Excluded(b"\xff\xff")
            ),
            Ok((Excluded(b"\xff\xff".to_vec()), Unbounded)),
        );
        assert_eq!(
            rewrite_prefix_of_range(
                b"\xff\xfe",
                b"\xff\xff",
                Included(b"\xff\xfe"),
                Included(b"\xff\xff")
            ),
            Err(WrongPrefix),
        );
    }
}

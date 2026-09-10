// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

//! Thread name prefixes needed by `resource_control`'s pressure-scoring
//! thread scans.
//!
//! Upstream master has a much larger, centralized version of this module
//! covering every TiKV-spawned thread; this branch predates that refactor,
//! so only the constants/helper actually consumed here are ported.

pub const GRPC_SERVER_THREAD: &str = "grpc-server";

pub const UNIFIED_READ_POOL_THREAD: &str = "unified-read-pool";

const LINUX_THREAD_NAME_MAX_LEN: usize = 15;

/// Returns whether `thread_name` belongs to a thread name family whose prefix
/// is `prefix`.
///
/// On Linux, thread names observed via `/proc` come from the `comm` field,
/// which is truncated to at most 15 visible characters. For long TiKV prefixes
/// (for example, `"unified-read-pool"`), the observed thread name may only
/// contain the first 15 bytes of the prefix. We therefore match both:
/// 1. the full prefix (normal case), and
/// 2. the 15-byte truncated prefix (Linux truncation case).
#[inline]
pub fn matches_thread_name_prefix(thread_name: &str, prefix: &str) -> bool {
    if thread_name.starts_with(prefix) {
        return true;
    }
    if prefix.len() <= LINUX_THREAD_NAME_MAX_LEN {
        return false;
    }
    // Thread name prefixes are ASCII constants in TiKV; byte slicing is safe.
    thread_name.starts_with(&prefix[..LINUX_THREAD_NAME_MAX_LEN])
}

#[cfg(test)]
mod tests {
    use super::{matches_thread_name_prefix, LINUX_THREAD_NAME_MAX_LEN};

    #[test]
    fn test_matches_thread_name_prefix_basic() {
        assert!(matches_thread_name_prefix("unified-read-1", "unified-read"));
        assert!(!matches_thread_name_prefix(
            "foo-unified-read",
            "unified-read"
        ));
    }

    #[test]
    fn test_matches_thread_name_prefix_truncated() {
        let long_prefix = "unified-read-pool";
        let truncated = &long_prefix[..LINUX_THREAD_NAME_MAX_LEN];
        let thread_name = format!("{truncated}-1");
        assert!(matches_thread_name_prefix(&thread_name, long_prefix));
        assert!(!matches_thread_name_prefix(
            &format!("x{thread_name}"),
            long_prefix
        ));
    }
}

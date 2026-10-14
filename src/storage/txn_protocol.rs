// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.
// Modifications Copyright 2026 PingCAP, Inc.

//! Transaction RPC compatibility declarations and protocol errors.

use kvproto::{
    errorpb,
    kvrpcpb::{Context, TxnProtocolVersion},
    metapb,
};

/// Legacy is a protocol value, independent of this binary's admission range.
pub const TXN_PROTOCOL_VERSION_LEGACY: u32 = TxnProtocolVersion::TxnVerLegacy as u32;
/// Lowest transaction protocol declaration accepted by this binary.
pub const TXN_PROTOCOL_VERSION_MIN: u32 = TXN_PROTOCOL_VERSION_LEGACY;
/// Highest transaction protocol declaration accepted by this binary.
pub const TXN_PROTOCOL_VERSION_MAX: u32 = TxnProtocolVersion::TxnVerSupportSharedLock as u32;
/// The declaration required before a caller can consume shared-lock semantics.
pub const TXN_PROTOCOL_VERSION_SHARED_LOCK: u32 =
    TxnProtocolVersion::TxnVerSupportSharedLock as u32;

const LEGACY_INCOMPATIBLE_BUSY_REASON: &str = "txn_protocol_incompatible";

/// A transaction-protocol compatibility rejection with its inclusive accepted
/// range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IncompatibleRequest {
    pub provided: u32,
    pub min_compatible: u32,
    pub max_compatible: u32,
    pub message: &'static str,
}

impl IncompatibleRequest {
    /// Creates a rejection for a declaration outside this binary's global
    /// range.
    pub fn global_admission(provided: u32) -> Self {
        Self {
            provided,
            min_compatible: TXN_PROTOCOL_VERSION_MIN,
            max_compatible: TXN_PROTOCOL_VERSION_MAX,
            message: "transaction protocol version is outside the store admission range",
        }
    }

    /// Creates a rejection for a caller that must support shared-lock
    /// semantics.
    pub fn require_shared_lock(provided: u32) -> Self {
        Self {
            provided,
            min_compatible: TXN_PROTOCOL_VERSION_SHARED_LOCK,
            max_compatible: TXN_PROTOCOL_VERSION_MAX,
            message: "transaction protocol version does not support shared locks",
        }
    }

    /// Encodes this rejection in the standard region-error envelope.
    pub fn into_region_error(self) -> errorpb::Error {
        let mut incompatible = errorpb::IncompatibleRequest::default();
        incompatible.set_reason(
            errorpb::IncompatibleRequestReason::IncompatibleRequestReasonTxnProtocolVersionOutOfRange,
        );
        incompatible.set_message(self.message.to_owned());
        incompatible.set_provided_txn_protocol_version(self.provided);
        incompatible.set_min_compatible_txn_protocol_version(self.min_compatible);
        incompatible.set_max_compatible_txn_protocol_version(self.max_compatible);

        let mut error = errorpb::Error::default();
        error.set_incompatible_request(incompatible);
        if self.provided == TXN_PROTOCOL_VERSION_LEGACY {
            let mut busy = errorpb::ServerIsBusy::default();
            busy.set_reason(LEGACY_INCOMPATIBLE_BUSY_REASON.to_owned());
            error.set_server_is_busy(busy);
        }
        error
    }
}

/// Returns the global admission rejection for `ctx`, if its declaration is
/// unsupported.
pub fn check_admission(ctx: &Context) -> Option<IncompatibleRequest> {
    let provided = ctx.get_txn_protocol_version();
    (!(TXN_PROTOCOL_VERSION_MIN..=TXN_PROTOCOL_VERSION_MAX).contains(&provided))
        .then(|| IncompatibleRequest::global_admission(provided))
}

/// Returns whether the caller actually supports shared-lock protocol semantics.
pub fn caller_supports_shared_lock(ctx: &Context) -> bool {
    ctx.get_txn_protocol_version() >= TXN_PROTOCOL_VERSION_SHARED_LOCK
}

/// Validates that `ctx` can consume shared-lock semantics under the rollout
/// policy.
pub fn validate_shared_lock_declaration(
    ctx: &Context,
    allow_legacy_declaration: bool,
) -> Result<(), IncompatibleRequest> {
    if caller_supports_shared_lock(ctx)
        || (allow_legacy_declaration
            && ctx.get_txn_protocol_version() == TXN_PROTOCOL_VERSION_LEGACY)
    {
        Ok(())
    } else {
        Err(IncompatibleRequest::require_shared_lock(
            ctx.get_txn_protocol_version(),
        ))
    }
}

/// Creates the Store metadata range used for PD registration.
pub fn store_range() -> metapb::TxnProtocolVersionRange {
    let mut range = metapb::TxnProtocolVersionRange::default();
    range.set_min(TXN_PROTOCOL_VERSION_MIN);
    range.set_max(TXN_PROTOCOL_VERSION_MAX);
    range
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admission_range_and_legacy_fallback() {
        for version in [TXN_PROTOCOL_VERSION_MIN, 1, TXN_PROTOCOL_VERSION_MAX] {
            let mut ctx = Context::default();
            ctx.set_txn_protocol_version(version);
            assert!(check_admission(&ctx).is_none());
        }

        let mut ctx = Context::default();
        ctx.set_txn_protocol_version(TXN_PROTOCOL_VERSION_MAX + 1);
        let err = check_admission(&ctx).unwrap().into_region_error();
        assert!(err.has_incompatible_request());
        assert!(!err.has_server_is_busy());

        let err = IncompatibleRequest::require_shared_lock(0).into_region_error();
        assert!(err.has_incompatible_request());
        assert!(err.has_server_is_busy());
        assert_eq!(
            err.get_server_is_busy().get_reason(),
            LEGACY_INCOMPATIBLE_BUSY_REASON
        );
    }

    #[test]
    fn test_shared_lock_declaration_validation_only_exempts_legacy() {
        let mut legacy = Context::default();
        assert!(validate_shared_lock_declaration(&legacy, false).is_err());
        validate_shared_lock_declaration(&legacy, true).unwrap();
        legacy.set_txn_protocol_version(1);
        assert!(validate_shared_lock_declaration(&legacy, true).is_err());
        legacy.set_txn_protocol_version(TXN_PROTOCOL_VERSION_SHARED_LOCK);
        validate_shared_lock_declaration(&legacy, false).unwrap();
    }

    #[test]
    fn test_store_range_matches_admission_range() {
        let range = store_range();
        assert_eq!(range.get_min(), TXN_PROTOCOL_VERSION_MIN);
        assert_eq!(range.get_max(), TXN_PROTOCOL_VERSION_MAX);
    }
}

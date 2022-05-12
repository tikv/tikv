// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use semver::{SemVerError, Version};

/// The function assumes only major, minor and patch are considered, and they are
/// all less than u16::MAX, which is 65535.
const fn ver_to_val(major: u64, minor: u64, patch: u64) -> u64 {
    major << 32 | minor << 16 | patch
}

#[derive(Clone, Default)]
pub struct FeatureGate {
    version: Arc<AtomicU64>,
}

impl FeatureGate {
    pub fn set_version(&self, version: &str) -> Result<bool, SemVerError> {
        let new = Version::parse(version)?;
        let val = ver_to_val(new.major, new.minor, new.patch);
        let mut cur = self.version.load(Ordering::Relaxed);
        loop {
            if val <= cur {
                return Ok(false);
            }
            match self
                .version
                .compare_exchange_weak(cur, val, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Ok(true),
                Err(v) => cur = v,
            }
        }
    }

    pub fn can_enable(&self, feature: Feature) -> bool {
        self.version.load(Ordering::Relaxed) >= feature.ver
    }

    /// Reset the version in feature gate to the specified value.
    ///
    /// # Safety
    ///
    /// Correctness in FeatureGate depends on monotonic increasing of version number,
    /// should use `set_version` instead.
    pub unsafe fn reset_version(&self, version: &str) -> Result<(), SemVerError> {
        let new = Version::parse(version)?;
        let val = ver_to_val(new.major, new.minor, new.patch);
        self.version.store(val, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Copy, Clone)]
pub struct Feature {
    ver: u64,
}

impl Feature {
    pub const fn require(major: u64, minor: u64, patch: u64) -> Feature {
        Feature {
            ver: ver_to_val(major, minor, patch),
        }
    }
}

// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

static FIPS_VERSION: AtomicUsize = AtomicUsize::new(0);

/// Enable OpenSSL FIPS mode if `can_enable` returns true.
/// It should be called at the very start of a program.
pub fn maybe_enable() {
    if !can_enable() {
        return;
    }
    #[cfg(ossl1)]
    {
        openssl::fips::enable(true).unwrap();
        FIPS_VERSION.store(1, Ordering::SeqCst);
        return;
    }
    #[cfg(ossl3)]
    {
        std::mem::forget(openssl::provider::Provider::load(None, "fips").unwrap());
        FIPS_VERSION.store(3, Ordering::SeqCst);
        return;
    }
    #[allow(unreachable_code)]
    {
        slog_global::warn!("OpenSSL FIPS mode is disabled unexpectedly");
    }
}

/// Return true if it is built for FIPS mode.
pub fn can_enable() -> bool {
    !cfg!(disable_fips)
}

/// Prints OpenSSL FIPS mode status.
pub fn log_status() {
    let ver = FIPS_VERSION.load(Ordering::SeqCst);
    if ver == 0 {
        slog_global::info!("OpenSSL FIPS mode is disabled");
    } else {
        slog_global::info!("OpenSSL FIPS mode is enabled"; "openssl_major_version" => ver);
    }
}

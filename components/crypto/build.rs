// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::env;

fn main() {
    if !option_env!("ENABLE_FIPS").map_or(false, |v| v == "1") {
        println!("cargo:rustc-cfg=disable_fips");
        return;
    }
    if let Ok(version) = env::var("DEP_OPENSSL_VERSION_NUMBER") {
        let version = u64::from_str_radix(&version, 16).unwrap();

        #[allow(clippy::unusual_byte_groupings)]
        // Follow OpenSSL numeric release version identifier style:
        // MNNFFPPS: major minor fix patch status
        // See https://github.com/openssl/openssl/blob/OpenSSL_1_0_0-stable/crypto/opensslv.h
        if version >= 0x3_00_00_00_0 {
            println!("cargo:rustc-cfg=ossl3");
        } else {
            println!("cargo:rustc-cfg=ossl1");
        }
    } else {
        panic!(
            "

The DEP_OPENSSL_VERSION_NUMBER environment variable is not found.
Please make sure \"openssl-sys\" is in crypto's dependencies.

"
        )
    }
}

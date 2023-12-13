// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! Utilities for cryptographically strong random number generation.

use openssl::{error::ErrorStack, rand};

/// Fill buffer with cryptographically strong pseudo-random bytes.
pub fn rand_bytes(buf: &mut [u8]) -> Result<(), ErrorStack> {
    rand::rand_bytes(buf)
}

/// Return a random u64.
pub fn rand_u64() -> Result<u64, ErrorStack> {
    let mut rand_id = [0u8; 8];
    rand_bytes(&mut rand_id)?;
    Ok(u64::from_ne_bytes(rand_id))
}

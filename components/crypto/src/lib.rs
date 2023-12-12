// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! A shim crate for cryptographic operations, with special considerations for
//! meeting FIPS 140 requirements.
//!
//! This crate provides a set of cryptographic functionalities, including
//! RNG (random number generator). It has been meticulously crafted
//! to adhere to the FIPS 140 standards, ensuring a secure and compliant
//! environment for cryptographic operations in regulated environments.
// TODO: add message digest.

pub mod fips;
pub mod rand;

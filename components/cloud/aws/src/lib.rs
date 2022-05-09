// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(assert_matches)]

mod kms;
pub use kms::{AwsKms, ENCRYPTION_VENDOR_NAME_AWS_KMS};

mod s3;
pub use s3::{Config, S3Storage, STORAGE_VENDOR_NAME_AWS};

mod util;

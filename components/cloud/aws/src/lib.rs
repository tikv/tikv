// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod kms;
pub use kms::{AwsKms, ENCRYPTION_VENDOR_NAME_AWS_KMS};

mod s3;
pub use s3::{Config, S3Storage, STORAGE_NAME, STORAGE_VENDOR_NAME_AWS};

mod util;

// SDK tests share AWS environment variables, failpoints and request metrics.
#[cfg(test)]
static AWS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

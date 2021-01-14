#[macro_use]
extern crate slog_global;

mod s3;
pub use s3::{Config, S3Storage};

#[macro_use]
extern crate slog_global;

mod gcs;
pub use gcs::{Config, GCSStorage};

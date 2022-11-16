// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(test)]

#[macro_use]
extern crate slog_global;

mod gcs;
pub use gcs::{Config, GcsStorage};

pub mod utils {
    use std::future::Future;

    use cloud::metrics;
    use tikv_util::stream::{retry_ext, RetryError, RetryExt};
    pub async fn retry<G, T, F, E>(action: G, name: &'static str) -> Result<T, E>
    where
        G: FnMut() -> F,
        F: Future<Output = Result<T, E>>,
        E: RetryError + std::fmt::Debug,
    {
        retry_ext(action, RetryExt::default().with_fail_hook(move |err: &E| {
            warn!("gcp request meet error."; "err" => ?err, "retry?" => %err.is_retryable(), "context" => %name);
            metrics::CLOUD_ERROR_VEC.with_label_values(&["gcp", name]).inc();
        })).await
    }
}

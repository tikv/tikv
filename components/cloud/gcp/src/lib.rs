// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(test)]

#[macro_use]
extern crate slog_global;

mod gcs;
pub use gcs::{Config, GcsStorage};

mod client;
mod kms;
pub use kms::GcpKms;

pub const STORAGE_VENDOR_NAME_GCP: &str = "gcp";

pub mod utils {
    use std::{future::Future, io};

    use cloud::metrics;
    use hyper::{body::Bytes, Body};
    use tame_gcs::ApiResponse;
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

    pub async fn read_from_http_body<M: ApiResponse<Bytes>>(
        b: http::Response<Body>,
    ) -> io::Result<M> {
        use crate::gcs::ResultExt;
        let (headers, body) = b.into_parts();
        let bytes = hyper::body::to_bytes(body).await.or_io_error(format_args!(
            "cannot read bytes from http response {:?}",
            headers
        ))?;
        let cached_resp = http::Response::from_parts(headers, bytes);
        M::try_from_parts(cached_resp).or_invalid_input(format_args!("invalid response format"))
    }
}

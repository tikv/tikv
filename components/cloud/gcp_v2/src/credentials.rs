// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;

#[derive(Clone)]
pub(crate) enum CredentialsMode {
    Default,
    Json(String),
}

impl std::fmt::Debug for CredentialsMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialsMode::Default => f.write_str("Default"),
            CredentialsMode::Json(_) => f.write_str("Json(REDACTED)"),
        }
    }
}

pub(crate) fn validate_credentials_json(creds_json: &str) -> io::Result<()> {
    let _: serde_json::Value = serde_json::from_str(creds_json)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    Ok(())
}

pub(crate) fn ensure_rustls_fips_provider() -> io::Result<()> {
    if let Some(provider) = rustls::crypto::CryptoProvider::get_default() {
        if provider.fips() {
            return Ok(());
        }
        return Err(io::Error::other(
            "rustls crypto provider is already initialized without FIPS; gcp_v2 requires the aws-lc-rs FIPS provider",
        ));
    }

    if rustls::crypto::default_fips_provider()
        .install_default()
        .is_ok()
    {
        return Ok(());
    }

    match rustls::crypto::CryptoProvider::get_default() {
        Some(provider) if provider.fips() => Ok(()),
        Some(_) => Err(io::Error::other(
            "rustls crypto provider is already initialized without FIPS; gcp_v2 requires the aws-lc-rs FIPS provider",
        )),
        None => Err(io::Error::other(
            "failed to install the rustls aws-lc-rs FIPS provider",
        )),
    }
}

pub(crate) fn build_credentials(
    mode: &CredentialsMode,
) -> io::Result<Option<google_cloud_auth::credentials::Credentials>> {
    ensure_rustls_fips_provider()?;
    match mode {
        CredentialsMode::Default => Ok(None),
        CredentialsMode::Json(creds_json) => {
            let creds_value: serde_json::Value = serde_json::from_str(creds_json)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let creds_type = creds_value
                .get("type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing `type` in credentials_blob",
                    )
                })?;

            let creds = match creds_type {
                "service_account" => {
                    google_cloud_auth::credentials::service_account::Builder::new(creds_value)
                        .build()
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
                }
                "external_account" => {
                    google_cloud_auth::credentials::external_account::Builder::new(creds_value)
                        .build()
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "unsupported credentials_blob type `{creds_type}`, supported: service_account, external_account"
                        ),
                    ));
                }
            };
            Ok(Some(creds))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ensure_rustls_fips_provider;

    #[test]
    fn test_ensure_rustls_fips_provider() {
        ensure_rustls_fips_provider().unwrap();
        assert!(
            rustls::crypto::CryptoProvider::get_default()
                .unwrap()
                .fips()
        );
        ensure_rustls_fips_provider().unwrap();
    }
}

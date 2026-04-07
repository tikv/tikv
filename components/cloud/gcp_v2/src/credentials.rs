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

#[derive(Clone, Copy, Debug)]
pub(crate) enum RustlsProvider {
    #[cfg(feature = "fips")]
    AwsLcRsFips,
    #[cfg(not(feature = "fips"))]
    ProcessDefault,
}

pub(crate) fn resolve_rustls_provider() -> RustlsProvider {
    #[cfg(feature = "fips")]
    {
        RustlsProvider::AwsLcRsFips
    }

    #[cfg(not(feature = "fips"))]
    {
        RustlsProvider::ProcessDefault
    }
}

impl RustlsProvider {
    fn install_default(self) -> Result<(), std::sync::Arc<rustls::crypto::CryptoProvider>> {
        match self {
            #[cfg(feature = "fips")]
            Self::AwsLcRsFips => rustls::crypto::default_fips_provider().install_default(),
            #[cfg(not(feature = "fips"))]
            Self::ProcessDefault => rustls::crypto::ring::default_provider().install_default(),
        }
    }

    fn matches_default(self, provider: &std::sync::Arc<rustls::crypto::CryptoProvider>) -> bool {
        match self {
            #[cfg(feature = "fips")]
            Self::AwsLcRsFips => provider.fips(),
            #[cfg(not(feature = "fips"))]
            Self::ProcessDefault => {
                let _ = provider;
                true
            }
        }
    }

    fn mismatch_error(self) -> io::Error {
        match self {
            #[cfg(feature = "fips")]
            Self::AwsLcRsFips => io::Error::other(
                "rustls crypto provider is already initialized without FIPS; gcp_v2 requires the aws-lc-rs FIPS provider",
            ),
            #[cfg(not(feature = "fips"))]
            Self::ProcessDefault => {
                unreachable!("process-default rustls provider accepts any initialized provider")
            }
        }
    }

    fn install_error(self) -> io::Error {
        match self {
            #[cfg(not(feature = "fips"))]
            Self::ProcessDefault => io::Error::other("failed to install the rustls ring provider"),

            #[cfg(all(feature = "fips", not(target_os = "macos")))]
            Self::AwsLcRsFips => {
                io::Error::other("failed to install the rustls aws-lc-rs FIPS provider")
            }

            #[cfg(all(feature = "fips", target_os = "macos"))]
            Self::AwsLcRsFips => io::Error::other(
                "failed to install the rustls aws-lc-rs FIPS provider on macOS; make sure the required aws_lc_* dylib is available to the TiKV process (for example via DYLD_LIBRARY_PATH)",
            ),
        }
    }

    pub(crate) fn ensure_default(self) -> io::Result<()> {
        if let Some(provider) = rustls::crypto::CryptoProvider::get_default() {
            return if self.matches_default(provider) {
                Ok(())
            } else {
                Err(self.mismatch_error())
            };
        }

        if self.install_default().is_ok() {
            return Ok(());
        }

        match rustls::crypto::CryptoProvider::get_default() {
            Some(provider) if self.matches_default(provider) => Ok(()),
            Some(_) => Err(self.mismatch_error()),
            None => Err(self.install_error()),
        }
    }
}

pub(crate) fn build_credentials(
    mode: &CredentialsMode,
) -> io::Result<Option<google_cloud_auth::credentials::Credentials>> {
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
    use super::resolve_rustls_provider;

    #[test]
    fn test_resolve_rustls_provider() {
        resolve_rustls_provider().ensure_default().unwrap();
        let provider = rustls::crypto::CryptoProvider::get_default().unwrap();
        #[cfg(feature = "fips")]
        assert!(provider.fips());
        #[cfg(not(feature = "fips"))]
        assert!(!provider.fips());
        resolve_rustls_provider().ensure_default().unwrap();
    }
}

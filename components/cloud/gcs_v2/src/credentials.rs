// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;

#[derive(Clone)]
pub(crate) enum CredentialsMode {
    Default,
    Anonymous,
    Json(String),
}

impl std::fmt::Debug for CredentialsMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialsMode::Default => f.write_str("Default"),
            CredentialsMode::Anonymous => f.write_str("Anonymous"),
            CredentialsMode::Json(_) => f.write_str("Json(REDACTED)"),
        }
    }
}

pub(crate) fn validate_credentials_json(creds_json: &str) -> io::Result<()> {
    let _: serde_json::Value =
        serde_json::from_str(creds_json).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    Ok(())
}

pub(crate) fn build_credentials(
    mode: &CredentialsMode,
) -> io::Result<Option<google_cloud_auth::credentials::Credentials>> {
    match mode {
        CredentialsMode::Default => Ok(None),
        CredentialsMode::Anonymous => {
            Ok(Some(google_cloud_auth::credentials::anonymous::Builder::new().build()))
        }
        CredentialsMode::Json(creds_json) => {
            let creds_value: serde_json::Value = serde_json::from_str(creds_json)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let creds_type = creds_value
                .get("type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing `type` in credentials_blob")
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

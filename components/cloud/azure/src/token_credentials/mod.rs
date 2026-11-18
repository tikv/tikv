// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

pub mod certificate_credentials;
pub mod default_credentials;
pub mod shared_key;

use std::fmt::Write;

use azure_core::{error::ErrorKind, http::StatusCode};
use serde::Deserialize;

/// Keep diagnostic codes and correlation IDs without retaining token responses
/// or free-form service messages, which can contain credentials.
fn authentication_error_summary(status: StatusCode, body: &[u8]) -> String {
    #[derive(Default, Deserialize)]
    #[serde(default)]
    struct TokenError {
        error: String,
        error_codes: Vec<u64>,
        trace_id: String,
        correlation_id: String,
    }

    let mut summary = format!("HTTP {status}");
    if body.len() > 16 * 1024 {
        return summary;
    }
    let Ok(error) = serde_json::from_slice::<TokenError>(body) else {
        return summary;
    };
    if matches!(
        error.error.as_str(),
        "invalid_request"
            | "invalid_client"
            | "invalid_grant"
            | "unauthorized_client"
            | "unsupported_grant_type"
            | "invalid_scope"
            | "access_denied"
            | "temporarily_unavailable"
            | "server_error"
            | "interaction_required"
            | "consent_required"
            | "login_required"
    ) {
        write!(&mut summary, ", error={}", error.error).unwrap();
    }
    for code in error.error_codes.iter().take(8) {
        write!(&mut summary, ", AADSTS{code}").unwrap();
    }
    for (name, value) in [
        ("trace_id", error.trace_id),
        ("correlation_id", error.correlation_id),
    ] {
        if let Ok(id) = uuid::Uuid::parse_str(&value) {
            write!(&mut summary, ", {name}={id}").unwrap();
        }
    }
    summary
}

/// Summarize structured causes without formatting arbitrary SDK error messages.
fn credential_error_summary(error: &azure_core::Error) -> String {
    let mut cause: &(dyn std::error::Error + 'static) = error;
    loop {
        if let Some(error) = cause.downcast_ref::<azure_core::Error>() {
            if let ErrorKind::HttpResponse {
                status,
                raw_response,
                ..
            } = error.kind()
            {
                let body = raw_response
                    .as_ref()
                    .map(|response| response.body().as_ref())
                    .unwrap_or_default();
                return authentication_error_summary(*status, body);
            }
        }
        if let Some(error) = cause.downcast_ref::<std::io::Error>() {
            return format!("I/O error ({:?})", error.kind());
        }
        if let Some(error) = cause.downcast_ref::<std::env::VarError>() {
            return match error {
                std::env::VarError::NotPresent => "required environment variable is missing",
                std::env::VarError::NotUnicode(_) => "environment variable is not valid UTF-8",
            }
            .to_owned();
        }
        match cause.source() {
            Some(source) => cause = source,
            None => break,
        }
    }
    // HttpResponse is handled above; other kinds contain no service-provided data.
    error.kind().to_string()
}

#[cfg(test)]
mod tests {
    use azure_core::{
        Error,
        http::{RawResponse, headers::Headers},
    };

    use super::*;

    #[test]
    fn test_authentication_error_keeps_codes_and_redacts_response_text() {
        let body = br#"{
            "error": "invalid_client", "error_codes": [700027],
            "trace_id": "61e8a597-68d2-41e7-8ec9-210cd1cb8000",
            "correlation_id": "0e7b9c3c-0320-43b8-99f8-1788a30f121e",
            "error_description": "client_assertion=secret-assertion",
            "access_token": "secret-token"
        }"#;
        let message = authentication_error_summary(StatusCode::Unauthorized, body);
        assert!(message.contains("401"));
        assert!(message.contains("invalid_client"));
        assert!(message.contains("AADSTS700027"));
        assert!(message.contains("trace_id=61e8a597-68d2-41e7-8ec9-210cd1cb8000"));
        assert!(message.contains("correlation_id=0e7b9c3c-0320-43b8-99f8-1788a30f121e"));
        assert!(!message.contains("secret"));

        let response =
            RawResponse::from_bytes(StatusCode::Unauthorized, Headers::default(), body.to_vec());
        let error = Error::with_message(
            ErrorKind::HttpResponse {
                status: StatusCode::Unauthorized,
                error_code: Some("secret-error-code".into()),
                raw_response: Some(Box::new(response)),
            },
            "secret-response",
        );
        assert_eq!(credential_error_summary(&error), message);
    }

    #[test]
    fn test_authentication_error_omits_untrusted_and_oversized_fields() {
        let expected = authentication_error_summary(StatusCode::BadRequest, b"");
        for body in [
            br#"{"error":"secret-token","trace_id":"secret-assertion"}"#.to_vec(),
            b"malformed secret-token".to_vec(),
            vec![b'x'; 16 * 1024 + 1],
        ] {
            assert_eq!(
                authentication_error_summary(StatusCode::BadRequest, &body),
                expected
            );
        }
    }

    #[test]
    fn test_credential_error_keeps_io_cause_without_paths() {
        let error = Error::with_error(
            ErrorKind::Credential,
            std::io::Error::new(std::io::ErrorKind::NotFound, "secret-token-file"),
            "secret-context",
        );
        assert_eq!(credential_error_summary(&error), "I/O error (NotFound)");
    }
}

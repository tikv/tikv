// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.
// Copyright 2026 PingCAP, Inc.

use std::{collections::BTreeMap, fmt::Write, sync::Arc};

use async_trait::async_trait;
use azure_core::{
    credentials::Secret,
    http::{
        Context, Request,
        policies::{Policy, PolicyResult},
    },
};
use time::{OffsetDateTime, macros::format_description};

/// Supplies SharedKey authentication, which the supported Blob SDK leaves to
/// custom policies. Sign on every retry so the date and request body match.
#[derive(Debug)]
pub struct SharedKeyPolicy {
    account: String,
    key: Secret,
}

impl SharedKeyPolicy {
    /// Validate an account key and retain it with redacted debug output.
    pub fn new(account: String, key: String) -> azure_core::Result<Self> {
        azure_core::base64::decode(&key)?;
        Ok(Self {
            account,
            key: Secret::new(key),
        })
    }

    // https://learn.microsoft.com/rest/api/storageservices/authorize-with-shared-key
    fn string_to_sign(&self, request: &Request) -> String {
        let headers = request.headers();
        let mut result = format!("{}\n", request.method());
        for name in [
            "content-encoding",
            "content-language",
            "content-length",
            "content-md5",
            "content-type",
            "date",
            "if-modified-since",
            "if-match",
            "if-none-match",
            "if-unmodified-since",
            "range",
        ] {
            let value = headers.get_optional_str(&name.into()).unwrap_or_default();
            // x-ms-date replaces Date; zero-length bodies use an empty value.
            if name != "date" && !(name == "content-length" && value == "0") {
                result.push_str(value);
            }
            result.push('\n');
        }
        let mut ms_headers: Vec<_> = headers
            .iter()
            .filter(|(name, _)| name.as_str().starts_with("x-ms-"))
            .collect();
        ms_headers.sort_unstable_by_key(|(name, _)| name.as_str());
        for (name, value) in ms_headers {
            write!(&mut result, "{}:", name.as_str()).unwrap();
            write_canonicalized_header_value(&mut result, value.as_str());
            result.push('\n');
        }
        write!(&mut result, "/{}{}", self.account, request.url().path()).unwrap();
        let mut query: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (name, value) in request.url().query_pairs() {
            query
                .entry(name.to_ascii_lowercase())
                .or_default()
                .push(value.into_owned());
        }
        for (name, mut values) in query {
            values.sort_unstable();
            write!(&mut result, "\n{}:{}", name, values.join(",")).unwrap();
        }
        result
    }
}

/// Collapse linear whitespace outside quoted strings, preserving quoted pairs
/// and the whitespace within quotes as required by Azure SharedKey signing.
fn write_canonicalized_header_value(output: &mut String, value: &str) {
    let mut quoted = false;
    let mut escaped = false;
    let mut whitespace = false;
    for ch in value.trim_matches([' ', '\t', '\r', '\n']).chars() {
        if !quoted && matches!(ch, ' ' | '\t' | '\r' | '\n') {
            whitespace = true;
            continue;
        }
        if whitespace {
            output.push(' ');
            whitespace = false;
        }
        output.push(ch);
        if escaped {
            escaped = false;
        } else if quoted && ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            quoted = !quoted;
        }
    }
}

#[async_trait]
impl Policy for SharedKeyPolicy {
    // Match the SDK trait's async_trait-generated lifetime bounds. Writing
    // Context<'_> here generates different bounds and fails to implement Policy.
    #[allow(elided_lifetimes_in_paths)]
    async fn send(
        &self,
        ctx: &Context,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        let date = OffsetDateTime::now_utc()
            .format(format_description!(
                "[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT"
            ))
            .expect("valid HTTP date format");
        request.insert_header("x-ms-date", date);
        if let Some(len) = request.body().len() {
            request.insert_header("content-length", len.to_string());
        }
        let signature = azure_core::hmac::hmac_sha256(&self.string_to_sign(request), &self.key)?;
        request.insert_header(
            "authorization",
            format!("SharedKey {}:{}", self.account, signature),
        );
        next[0].send(ctx, request, &next[1..]).await
    }
}

#[cfg(test)]
mod tests {
    use azure_core::http::{Method, Url};

    use super::*;

    #[test]
    fn test_shared_key_matches_azure_signature_example() {
        let policy = SharedKeyPolicy::new("myaccount".into(), "c2VjcmV0".into()).unwrap();
        let mut request = Request::new(Url::parse("https://myaccount.blob.core.windows.net/mycontainer?timeout=20&restype=container&comp=metadata").unwrap(), Method::Get);
        request.insert_header("x-ms-date", "Fri, 26 Jun 2015 23:39:12 GMT");
        request.insert_header("x-ms-version", "2015-02-21");
        request.insert_header("content-length", "0");
        request.insert_header("date", "ignored because x-ms-date is set");
        // Published canonical string from the Azure SharedKey protocol example.
        assert_eq!(
            policy.string_to_sign(&request),
            concat!(
                "GET\n\n\n\n\n\n\n\n\n\n\n\n",
                "x-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\nx-ms-version:2015-02-21\n",
                "/myaccount/mycontainer\ncomp:metadata\nrestype:container\ntimeout:20"
            )
        );
        assert!(!format!("{:?}", policy).contains("c2VjcmV0"));
    }

    #[test]
    fn test_shared_key_preserves_escaped_path_and_sorts_decoded_query_values() {
        let policy = SharedKeyPolicy::new("account".into(), "c2VjcmV0".into()).unwrap();
        let request = Request::new(Url::parse("http://127.0.0.1/account/container/a%20b?include=metadata&include=snapshots&marker=a%2Bb&comp=list").unwrap(), Method::Get);
        assert!(policy.string_to_sign(&request).ends_with(
            "/account/account/container/a%20b\ncomp:list\ninclude:metadata,snapshots\nmarker:a+b"
        ));
    }

    #[test]
    fn test_shared_key_normalizes_header_whitespace_and_preserves_quoted_values() {
        let policy = SharedKeyPolicy::new("account".into(), "c2VjcmV0".into()).unwrap();
        for (value, expected) in [
            (" \tone   \t two \r\n three  ", "one two three"),
            ("  \"one  two\tthree\" \t four ", "\"one  two\tthree\" four"),
            (r#"  "one \"  two"   three  "#, r#""one \"  two" three"#),
            (" \t ", ""),
        ] {
            let mut request = Request::new(
                Url::parse("https://account.blob.core.windows.net/container/blob").unwrap(),
                Method::Get,
            );
            request.insert_header("x-ms-meta-value", value);
            assert!(
                policy
                    .string_to_sign(&request)
                    .contains(&format!("x-ms-meta-value:{expected}\n"))
            );
        }
    }
}

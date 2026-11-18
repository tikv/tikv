# Third-party logging maintenance notes

`components/server/src/setup.rs` installs the logger implemented in
`components/tikv_util/src/logger/mod.rs`.

The Azure SDK upgrade removes the temporary Debug/Trace suppression for
`azure_core 0.18`. Azure logs now follow the same configured root-module filters
as other third-party logs.

- Keep the new SDK's request-log sanitization intact.
- Never include Authorization values, SAS signatures, certificates, or token
  response bodies in custom authentication and error diagnostics.
- Keep safe authentication status/error codes and correlation IDs for diagnosis.
- Preserve release-8.5's logger thread setup and other redaction behavior.

Review `components/cloud/azure/src/token_credentials` and its redaction tests
when changing Azure credentials. The upload transport regression in `azblob.rs`
checks that nested HTTP errors do not disclose a SAS signature.

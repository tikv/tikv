# TiKV Security Policy

As a widely adopted distributed storage service, TiKV attaches great importance to code security. We are very grateful to users, security vulnerability researchers, etc. for reporting security vulnerabilities to us. All reported security vulnerabilities will be carefully assessed, addressed, and answered by us.

This document describes the security process and policy followed by the TiKV project.

## TiKV Security Team

Security vulnerabilities should be handled quickly and sometimes privately. The primary goal of this process is to reduce the total time users are vulnerable to publicly known exploits.

The TiKV Security Team is responsible for organizing the entire response including internal communication and external disclosure but will need help from relevant developers to successfully run this process.

The TiKV Security Team will consist of TiKV contributors assigned by the TiKV Maintainer team.

## Supported versions

The following are the versions that we support for security updates

| Version | Supported          |
| ------- | ------------------ |
| 6.x   | :white_check_mark: |
| 5.x   | :white_check_mark: |
| 4.x   | :white_check_mark: |
| 3.x   | :white_check_mark: |
| 2.x   | :white_check_mark: |
| < 2.0   | :x:                |

## Reporting a vulnerability

For all TiKV security-related defects, please send an email to tikv-security@lists.cncf.io. This mailing list is specially maintained by the TiKV security team. You will receive an acknowledgement mail within 24 hours. After that, we will give a detailed response about the subsequent process within 48 hours. Please do not submit security vulnerabilities directly as Github Issues.

If you want to, you can choose to use the PGP public key provided by us to encrypt the content of the mail. The public key is provided at the end of this document.

## Disclosure policy

For known public security vulnerabilities, we will disclose the disclosure as soon as possible after receiving the report. Vulnerabilities discovered for the first time will be disclosed in accordance with the following process:

1. The received security vulnerability report shall be handed over to the security team for follow-up coordination and repair work.
2. After the vulnerability is confirmed, we will create a draft Security Advisory on Github that lists the details of the vulnerability.
3. Invite related personnel to discuss about the fix.
4. Fork the temporary private repository on Github, and collaborate to fix the vulnerability.
5. After the fix code is merged into all supported versions, the vulnerability will be publicly posted in the GitHub Advisory Database.

## PGP Public Key

```
-----BEGIN PGP PUBLIC KEY BLOCK-----

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
ulx+VF4Lf9n3ydf593Nha9bMJ/rnSp01
=XbYK
-----END PGP PUBLIC KEY BLOCK-----
```
